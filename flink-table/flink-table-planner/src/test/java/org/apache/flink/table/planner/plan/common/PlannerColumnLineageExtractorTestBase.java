/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.common;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageExtractor;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageInput;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageOrigin;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageRelation;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageTransformation;
import org.apache.flink.table.planner.lineage.PlannerLineageDataset;
import org.apache.flink.table.planner.lineage.PlannerSinkColumnLineage;
import org.apache.flink.table.planner.lineage.TableLineageExtractionException;
import org.apache.flink.table.planner.plan.abilities.source.LimitPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for extracting column lineage from logical relational plans. */
public abstract class PlannerColumnLineageExtractorTestBase extends TableTestBase {

    private TableTestUtil util;

    protected abstract boolean isBatchMode();

    @BeforeEach
    void setup() {
        util =
                isBatchMode()
                        ? batchTestUtil(TableConfig.getDefault())
                        : streamTestUtil(TableConfig.getDefault());

        final Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.STRING())
                        .column("d", DataTypes.BIGINT())
                        .build();
        util.addTableSource("FirstTable", schema);
        util.addTableSource("SecondTable", schema);
        util.getTableEnv().createTemporarySystemFunction("test_plus", TestPlus.class);
        util.getTableEnv().createTemporarySystemFunction("test_zero", TestZero.class);
    }

    @Test
    void testExtractsDirectProjectionAndAlias() {
        final PlannerSinkColumnLineage lineage =
                extract("sink-one", "SELECT a AS customer_id FROM FirstTable", "customer_id");

        assertThat(lineage.getSinkKey()).isEqualTo("sink-one");
        assertThat(lineage.getExpectedOutputFields()).containsExactly("customer_id");
        assertThatThrownBy(() -> lineage.getExpectedOutputFields().add("unexpected"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(lineage.getRelations()).hasSize(1);
        final PlannerColumnLineageRelation relation = lineage.getRelations().get(0);
        assertThat(relation.getOutputField()).isEqualTo("customer_id");
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertThat(relation.getTransformations())
                .containsExactly(PlannerColumnLineageTransformation.ALIAS);
        assertInputs(relation, "FirstTable.a:DIRECT");
    }

    @Test
    void testWatermarkPreservesColumnValuesWithoutAddingWatermarkDependencies() {
        util.addTableSource(
                "WatermarkedTable",
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT())
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .watermark("ts", "ts - INTERVAL '5' SECOND")
                        .build());

        final PlannerSinkColumnLineage lineage =
                extract("sink-watermark", "SELECT a, ts FROM WatermarkedTable", "a", "ts");

        assertInputs(relation(lineage, "a"), "WatermarkedTable.a:DIRECT");
        assertInputs(relation(lineage, "ts"), "WatermarkedTable.ts:DIRECT");
        assertThat(relation(lineage, "a").getTransformations()).isEmpty();
        assertThat(relation(lineage, "ts").getTransformations()).isEmpty();
    }

    @Test
    void testSelectsExactFieldFromProjectedRowConstructor() {
        final RelNode input = toRelNode("SELECT a, b FROM FirstTable");
        final RexNode row =
                input.getCluster()
                        .getRexBuilder()
                        .makeCall(
                                SqlStdOperatorTable.ROW,
                                RexInputRef.of(0, input.getRowType()),
                                RexInputRef.of(1, input.getRowType()));
        final RelNode rowProject = project(input, row, "constructed_row");
        final RexNode selectedField =
                input.getCluster()
                        .getRexBuilder()
                        .makeFieldAccess(RexInputRef.of(0, rowProject.getRowType()), 0);

        final PlannerSinkColumnLineage lineage =
                PlannerColumnLineageExtractor.extract(
                        "sink-row-field",
                        Collections.singletonList("selected"),
                        project(rowProject, selectedField, "selected"));

        assertInputs(relation(lineage, "selected"), "FirstTable.a:DIRECT");
        assertThat(relation(lineage, "selected").getOrigin())
                .isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertThat(relation(lineage, "selected").getTransformations())
                .containsExactly(PlannerColumnLineageTransformation.EXPRESSION);
    }

    @Test
    void testRejectsNestedSourceFieldWithoutNestedPathContract() {
        util.addTableSource(
                "NestedTable",
                Schema.newBuilder()
                        .column(
                                "payload",
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("label", DataTypes.STRING())))
                        .build());

        assertThatThrownBy(
                        () ->
                                extract(
                                        "sink-nested-source",
                                        "SELECT n.payload.id FROM NestedTable n",
                                        "id"))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("RexFieldAccess");
    }

    @Test
    void testExtractsCastExpressionConstantAndFilter() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-expression",
                        "SELECT CAST(a AS STRING), b + 1, 'CN' FROM FirstTable WHERE c = 'ok'",
                        "a_text",
                        "b_plus_one",
                        "country");

        final PlannerColumnLineageRelation castRelation = relation(lineage, "a_text");
        assertThat(castRelation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertThat(castRelation.getTransformations())
                .contains(PlannerColumnLineageTransformation.CAST)
                .contains(PlannerColumnLineageTransformation.FILTER);
        assertInputs(castRelation, "FirstTable.a:DIRECT", "FirstTable.c:INDIRECT");

        final PlannerColumnLineageRelation expressionRelation = relation(lineage, "b_plus_one");
        assertThat(expressionRelation.getOrigin())
                .isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertThat(expressionRelation.getTransformations())
                .contains(PlannerColumnLineageTransformation.EXPRESSION)
                .contains(PlannerColumnLineageTransformation.FILTER);
        assertInputs(expressionRelation, "FirstTable.b:DIRECT", "FirstTable.c:INDIRECT");

        final PlannerColumnLineageRelation constantRelation = relation(lineage, "country");
        assertThat(constantRelation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.CONSTANT);
        assertInputs(constantRelation, "FirstTable.c:INDIRECT");
        assertThat(constantRelation.getTransformations())
                .containsExactly(PlannerColumnLineageTransformation.FILTER);
    }

    @Test
    void testPreservesDirectAndIndirectRolesForSameInputField() {
        final PlannerSinkColumnLineage lineage =
                extract("sink-same-field-filter", "SELECT a FROM FirstTable WHERE a > 0", "a");

        final PlannerColumnLineageRelation relation = relation(lineage, "a");
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertInputs(relation, "FirstTable.a:DIRECT", "FirstTable.a:INDIRECT");
        assertThat(relation.getTransformations())
                .containsExactly(PlannerColumnLineageTransformation.FILTER);
    }

    @Test
    void testRecordsExpectedSourceWhenOutputHasNoFieldDependency() {
        final PlannerSinkColumnLineage lineage =
                extract("sink-constant-source", "SELECT 1 FROM FirstTable", "constant_value");

        assertThat(lineage.getExpectedSources())
                .extracting(PlannerLineageDataset::asSerializableString)
                .containsExactly("`default_catalog`.`default_database`.`FirstTable`");
        assertThat(relation(lineage, "constant_value").getInputs()).isEmpty();
    }

    @Test
    void testExtractsCaseValueAndConditionDependencies() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-case",
                        "SELECT CASE WHEN b > 0 THEN a ELSE d END FROM FirstTable",
                        "selected_value");

        final PlannerColumnLineageRelation relation = relation(lineage, "selected_value");
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertInputs(
                relation, "FirstTable.a:DIRECT", "FirstTable.b:INDIRECT", "FirstTable.d:DIRECT");
        assertThat(relation.getTransformations())
                .containsExactly(PlannerColumnLineageTransformation.CONDITIONAL);
    }

    @Test
    void testExtractsJoinConditionAsIndirectDependency() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-join",
                        "SELECT f.a, s.c FROM FirstTable f JOIN SecondTable s ON f.b = s.b",
                        "left_a",
                        "right_c");

        final PlannerColumnLineageRelation left = relation(lineage, "left_a");
        assertInputs(
                left, "FirstTable.a:DIRECT", "FirstTable.b:INDIRECT", "SecondTable.b:INDIRECT");
        assertThat(left.getTransformations()).contains(PlannerColumnLineageTransformation.JOIN);

        final PlannerColumnLineageRelation right = relation(lineage, "right_c");
        assertInputs(
                right, "SecondTable.c:DIRECT", "FirstTable.b:INDIRECT", "SecondTable.b:INDIRECT");
        assertThat(right.getTransformations()).contains(PlannerColumnLineageTransformation.JOIN);
    }

    @Test
    void testExtractsAggregateGroupByAndCountStar() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-aggregate",
                        "SELECT b, SUM(a), COUNT(*) FROM FirstTable GROUP BY b",
                        "group_b",
                        "total_a",
                        "row_count");

        final PlannerColumnLineageRelation group = relation(lineage, "group_b");
        assertThat(group.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertInputs(group, "FirstTable.b:DIRECT", "FirstTable.b:INDIRECT");
        assertThat(group.getTransformations())
                .contains(PlannerColumnLineageTransformation.GROUP_BY);

        final PlannerColumnLineageRelation total = relation(lineage, "total_a");
        assertThat(total.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertInputs(total, "FirstTable.a:DIRECT", "FirstTable.b:INDIRECT");
        assertThat(total.getTransformations())
                .contains(PlannerColumnLineageTransformation.AGGREGATION)
                .contains(PlannerColumnLineageTransformation.GROUP_BY);

        final PlannerColumnLineageRelation count = relation(lineage, "row_count");
        assertThat(count.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.SYSTEM);
        assertInputs(count, "FirstTable.b:INDIRECT");
        assertThat(count.getTransformations())
                .contains(PlannerColumnLineageTransformation.AGGREGATION)
                .contains(PlannerColumnLineageTransformation.GROUP_BY);
    }

    @Test
    void testAggregateConstantArgumentsRetainExactRowDependencies() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-system-aggregate",
                        "SELECT SUM(1), COUNT(*) FILTER (WHERE c = 'ok') "
                                + "FROM FirstTable GROUP BY b",
                        "sum_one",
                        "filtered_count");

        final PlannerColumnLineageRelation sumOne = relation(lineage, "sum_one");
        assertThat(sumOne.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.SYSTEM);
        assertInputs(sumOne, "FirstTable.b:INDIRECT");

        final PlannerColumnLineageRelation filteredCount = relation(lineage, "filtered_count");
        assertThat(filteredCount.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.SYSTEM);
        assertInputs(filteredCount, "FirstTable.b:INDIRECT", "FirstTable.c:INDIRECT");
        assertThat(filteredCount.getTransformations())
                .contains(PlannerColumnLineageTransformation.FILTER)
                .contains(PlannerColumnLineageTransformation.AGGREGATION);
    }

    @Test
    void testExtractsRexOverDependenciesExactly() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-window",
                        "SELECT COUNT(a) OVER (PARTITION BY b ORDER BY d ROWS BETWEEN UNBOUNDED"
                                + " PRECEDING AND CURRENT ROW) FROM FirstTable",
                        "running_total");

        final PlannerColumnLineageRelation relation = relation(lineage, "running_total");
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertInputs(
                relation, "FirstTable.a:DIRECT", "FirstTable.b:INDIRECT", "FirstTable.d:INDIRECT");
        assertThat(relation.getTransformations())
                .contains(PlannerColumnLineageTransformation.WINDOW);
    }

    @Test
    void testWindowConstantArgumentHasSystemOrigin() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-window-system",
                        "SELECT SUM(1) OVER (PARTITION BY b ORDER BY d) FROM FirstTable",
                        "running_count");

        final PlannerColumnLineageRelation relation = relation(lineage, "running_count");
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.SYSTEM);
        assertInputs(relation, "FirstTable.b:INDIRECT", "FirstTable.d:INDIRECT");
        assertThat(relation.getTransformations())
                .contains(PlannerColumnLineageTransformation.WINDOW);
    }

    @Test
    void testLogicalWindowResolvesConstantsAfterChildFields() {
        final RelNode rawPlan =
                toRelNode(
                        "SELECT SUM(1) OVER (PARTITION BY b ORDER BY d) AS running_count "
                                + "FROM FirstTable");
        final HepPlanner planner =
                new HepPlanner(
                        new HepProgramBuilder()
                                .addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
                                .build());
        planner.setRoot(rawPlan);
        final RelNode logicalWindowPlan = planner.findBestExp();
        final Window window = findRel(logicalWindowPlan, Window.class);

        assertThat(window).isNotNull();
        assertThat(window.getConstants()).hasSize(1);

        final PlannerColumnLineageRelation relation =
                PlannerColumnLineageExtractor.extract(
                                "sink-logical-window",
                                Collections.singletonList("running_count"),
                                logicalWindowPlan)
                        .getRelations()
                        .get(0);
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.SYSTEM);
        assertInputs(relation, "FirstTable.b:INDIRECT", "FirstTable.d:INDIRECT");
        assertThat(relation.getTransformations())
                .containsExactly(
                        PlannerColumnLineageTransformation.WINDOW,
                        PlannerColumnLineageTransformation.ALIAS);
    }

    @Test
    void testExtractsUnionBranchesByOutputOrdinal() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-union",
                        "SELECT a AS id FROM FirstTable UNION ALL SELECT d AS id FROM SecondTable",
                        "id");

        final PlannerColumnLineageRelation relation = relation(lineage, "id");
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertInputs(relation, "FirstTable.a:DIRECT", "SecondTable.d:DIRECT");
        assertThat(relation.getTransformations())
                .contains(PlannerColumnLineageTransformation.UNION);
    }

    @Test
    void testExtractsSortFieldWithoutLimitOrOffsetDependencies() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-sort",
                        "SELECT a FROM FirstTable ORDER BY b "
                                + "OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
                        "a");

        final PlannerColumnLineageRelation relation = relation(lineage, "a");
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertInputs(relation, "FirstTable.a:DIRECT", "FirstTable.b:INDIRECT");
        assertThat(relation.getTransformations())
                .containsExactly(PlannerColumnLineageTransformation.SORT);
    }

    @Test
    void testMixedUnionKeepsDirectAndExactIndirectDependencies() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-mixed-union",
                        "SELECT a AS id FROM FirstTable UNION ALL "
                                + "SELECT 1 AS id FROM SecondTable WHERE c = 'active'",
                        "id");

        final PlannerColumnLineageRelation relation = relation(lineage, "id");
        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.INPUT_FIELDS);
        assertInputs(relation, "FirstTable.a:DIRECT", "SecondTable.c:INDIRECT");
        assertThat(relation.getTransformations())
                .contains(PlannerColumnLineageTransformation.FILTER)
                .contains(PlannerColumnLineageTransformation.UNION);
    }

    @Test
    void testExtractsValuesAsConstants() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-values",
                        "SELECT id, label FROM (VALUES (1, 'x')) AS T(id, label)",
                        "id",
                        "label");

        assertThat(lineage.getRelations())
                .allSatisfy(
                        relation -> {
                            assertThat(relation.getOrigin())
                                    .isEqualTo(PlannerColumnLineageOrigin.CONSTANT);
                            assertInputs(relation);
                        });
        assertThat(lineage.getExpectedSources()).isEmpty();
    }

    @Test
    void testExpandsViewAndCteToUnderlyingTable() {
        util.getTableEnv()
                .executeSql(
                        "CREATE TEMPORARY VIEW ActiveView AS "
                                + "SELECT a, b + 1 AS b1 FROM FirstTable WHERE c = 'active'");

        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-view",
                        "WITH ActiveCte AS (SELECT a AS id, b1 FROM ActiveView) "
                                + "SELECT id, b1 FROM ActiveCte",
                        "id",
                        "score");

        assertInputs(relation(lineage, "id"), "FirstTable.a:DIRECT", "FirstTable.c:INDIRECT");
        assertInputs(relation(lineage, "score"), "FirstTable.b:DIRECT", "FirstTable.c:INDIRECT");
    }

    @Test
    void testExtractsAllUdfArgumentFields() {
        final PlannerSinkColumnLineage lineage =
                extract("sink-udf", "SELECT test_plus(a, b) FROM FirstTable", "enriched");

        final PlannerColumnLineageRelation relation = relation(lineage, "enriched");
        assertInputs(relation, "FirstTable.a:DIRECT", "FirstTable.b:DIRECT");
        assertThat(relation.getTransformations()).contains(PlannerColumnLineageTransformation.UDF);
    }

    @Test
    void testKeepsResultsIsolatedBySink() {
        final RelNode relNode = toRelNode("SELECT a FROM FirstTable");

        final PlannerSinkColumnLineage first =
                PlannerColumnLineageExtractor.extract(
                        "sink-one", Collections.singletonList("first_id"), relNode);
        final PlannerSinkColumnLineage second =
                PlannerColumnLineageExtractor.extract(
                        "sink-two", Collections.singletonList("second_id"), relNode);

        assertThat(first.getSinkKey()).isEqualTo("sink-one");
        assertThat(first.getRelations())
                .extracting(PlannerColumnLineageRelation::getOutputField)
                .containsExactly("first_id");
        assertThat(second.getSinkKey()).isEqualTo("sink-two");
        assertThat(second.getRelations())
                .extracting(PlannerColumnLineageRelation::getOutputField)
                .containsExactly("second_id");
    }

    @Test
    void testDatasetIdentityUsesSerializableObjectIdentifierEncoding() {
        final PlannerLineageDataset dataset =
                new PlannerLineageDataset(Arrays.asList("cat.with.dot", "db", "table`name"));

        assertThat(dataset.asSerializableString()).isEqualTo("`cat.with.dot`.`db`.`table``name`");
        assertThatThrownBy(() -> new PlannerLineageDataset(Arrays.asList("database", "table")))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("catalog, database, and object");
    }

    @Test
    void testSourceIdentityIgnoresPlannerAbilityDigest() {
        final RelNode input = toRelNode("SELECT a FROM FirstTable");
        final TableScan scan = findRel(input, TableScan.class);
        assertThat(scan).isNotNull();
        final TableSourceTable sourceTable = scan.getTable().unwrap(TableSourceTable.class);
        assertThat(sourceTable).isNotNull();
        final TableSourceTable sourceWithDigest =
                sourceTable.copy(
                        sourceTable.tableSource(),
                        sourceTable.getRowType(),
                        new SourceAbilitySpec[] {new LimitPushDownSpec(7)});
        assertThat(sourceWithDigest.getQualifiedName()).hasSizeGreaterThan(3);

        final PlannerColumnLineageRelation relation =
                PlannerColumnLineageExtractor.extract(
                                "sink-source-identity",
                                Arrays.asList("a", "b", "c", "d"),
                                LogicalTableScan.create(
                                        scan.getCluster(),
                                        sourceWithDigest,
                                        Collections.emptyList()))
                        .getRelations()
                        .get(0);

        assertThat(relation.getInputs().get(0).getDataset().getQualifiedName())
                .containsExactly("default_catalog", "default_database", "FirstTable");
    }

    @Test
    void testRejectsAnonymousSourceWithoutStableIdentity() {
        final RelNode input = toRelNode("SELECT a FROM FirstTable");
        final TableScan scan = findRel(input, TableScan.class);
        assertThat(scan).isNotNull();
        final TableSourceTable sourceTable = scan.getTable().unwrap(TableSourceTable.class);
        assertThat(sourceTable).isNotNull();
        final TableSourceTable anonymousSource =
                sourceTable.copy(
                        sourceTable.tableSource(),
                        ContextResolvedTable.anonymous(
                                "lineage-test",
                                sourceTable.contextResolvedTable().getResolvedTable()),
                        sourceTable.getRowType(),
                        sourceTable.abilitySpecs());

        assertThatThrownBy(
                        () ->
                                PlannerColumnLineageExtractor.extract(
                                        "sink-anonymous-source",
                                        Collections.singletonList("a"),
                                        LogicalTableScan.create(
                                                scan.getCluster(),
                                                anonymousSource,
                                                Collections.emptyList())))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("Anonymous source")
                .hasMessageContaining("stable dataset identity");
    }

    @Test
    void testRejectsMissingExpectedOutputRelation() {
        final PlannerSinkColumnLineage extracted =
                extract("sink-partial", "SELECT a FROM FirstTable", "first_id");

        assertThatThrownBy(
                        () ->
                                new PlannerSinkColumnLineage(
                                        "sink-partial",
                                        Arrays.asList("first_id", "missing_id"),
                                        extracted.getExpectedSources(),
                                        extracted.getRelations()))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("missing_id")
                .hasMessageContaining("no lineage relation");
    }

    @Test
    void testRejectsRelationOutsideExpectedOutputs() {
        final PlannerSinkColumnLineage extracted =
                extract("sink-invalid", "SELECT a FROM FirstTable", "actual_id");

        assertThatThrownBy(
                        () ->
                                new PlannerSinkColumnLineage(
                                        "sink-invalid",
                                        Collections.singletonList("expected_id"),
                                        extracted.getExpectedSources(),
                                        extracted.getRelations()))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("actual_id")
                .hasMessageContaining("not an expected output");
    }

    @Test
    void testRejectsNullExpectedSources() {
        final PlannerSinkColumnLineage extracted =
                extract("sink-null-source", "SELECT a FROM FirstTable", "actual_id");

        assertThatThrownBy(
                        () ->
                                new PlannerSinkColumnLineage(
                                        extracted.getSinkKey(),
                                        extracted.getExpectedOutputFields(),
                                        null,
                                        extracted.getRelations()))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("Expected sources must not be null");
    }

    @Test
    void testRejectsDuplicateExpectedSources() {
        final PlannerSinkColumnLineage extracted =
                extract("sink-duplicate-source", "SELECT a FROM FirstTable", "actual_id");
        final PlannerLineageDataset source = extracted.getExpectedSources().get(0);

        assertThatThrownBy(
                        () ->
                                new PlannerSinkColumnLineage(
                                        extracted.getSinkKey(),
                                        extracted.getExpectedOutputFields(),
                                        Arrays.asList(source, source),
                                        extracted.getRelations()))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("duplicate expected source identity")
                .hasMessageContaining(source.asSerializableString());
    }

    @Test
    void testRejectsRelationInputOutsideExpectedSources() {
        final PlannerSinkColumnLineage extracted =
                extract("sink-unexpected-source", "SELECT a FROM FirstTable", "actual_id");

        assertThatThrownBy(
                        () ->
                                new PlannerSinkColumnLineage(
                                        extracted.getSinkKey(),
                                        extracted.getExpectedOutputFields(),
                                        Collections.emptyList(),
                                        extracted.getRelations()))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("not an expected source identity");
    }

    @Test
    void testTreatsDynamicParameterAsConstant() {
        final RelNode input = toRelNode("SELECT * FROM FirstTable");
        final RexNode dynamicParameter =
                new RexDynamicParam(input.getRowType().getFieldList().get(0).getType(), 0);
        final RelNode project = project(input, dynamicParameter, "parameter_value");

        final PlannerColumnLineageRelation relation =
                PlannerColumnLineageExtractor.extract(
                                "sink-parameter",
                                Collections.singletonList("parameter_value"),
                                project)
                        .getRelations()
                        .get(0);

        assertThat(relation.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.CONSTANT);
        assertInputs(relation);
    }

    @Test
    void testTreatsZeroOperandCallsAsSystemValues() {
        final PlannerSinkColumnLineage lineage =
                extract(
                        "sink-zero-operand",
                        "SELECT RAND(), test_zero() FROM FirstTable",
                        "random_value",
                        "udf_value");

        final PlannerColumnLineageRelation randomValue = relation(lineage, "random_value");
        assertThat(randomValue.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.SYSTEM);
        assertInputs(randomValue);

        final PlannerColumnLineageRelation udfValue = relation(lineage, "udf_value");
        assertThat(udfValue.getOrigin()).isEqualTo(PlannerColumnLineageOrigin.SYSTEM);
        assertInputs(udfValue);
        assertThat(udfValue.getTransformations())
                .containsExactly(PlannerColumnLineageTransformation.UDF);
    }

    @Test
    void testRejectsOutOfRangeInputReference() {
        final RelNode input = toRelNode("SELECT * FROM FirstTable");
        final RexNode outOfRange =
                new RexInputRef(99, input.getRowType().getFieldList().get(0).getType());
        final RelNode project = new InvalidProject(input, outOfRange);

        assertThatThrownBy(
                        () ->
                                PlannerColumnLineageExtractor.extract(
                                        "sink-out-of-range",
                                        Collections.singletonList("invalid"),
                                        project))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("99")
                .hasMessageContaining("fields are available");
    }

    @Test
    void testRejectsUnsupportedRexNode() {
        final RelNode input = toRelNode("SELECT * FROM FirstTable");
        final RexNode correlVariable =
                input.getCluster()
                        .getRexBuilder()
                        .makeCorrel(input.getRowType(), new CorrelationId(0));
        final RelNode project = project(input, correlVariable, "correlated");

        assertThatThrownBy(
                        () ->
                                PlannerColumnLineageExtractor.extract(
                                        "sink-correlated",
                                        Collections.singletonList("correlated"),
                                        project))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("RexCorrelVariable");
    }

    @Test
    void testRejectsUnsupportedRelNode() {
        final RelNode relNode =
                toRelNode("SELECT a FROM FirstTable INTERSECT SELECT a FROM SecondTable");

        assertThatThrownBy(
                        () ->
                                PlannerColumnLineageExtractor.extract(
                                        "sink-unsupported",
                                        Collections.singletonList("a"),
                                        relNode))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("Unsupported relational node")
                .hasMessageContaining("Intersect");
    }

    @Test
    void testRejectsRecursiveRepeatUnionExplicitly() {
        final RelNode input = toRelNode("SELECT a FROM FirstTable");
        final RelNode repeatUnion = LogicalRepeatUnion.create(input, input, true, null);

        assertThatThrownBy(
                        () ->
                                PlannerColumnLineageExtractor.extract(
                                        "sink-recursive-union",
                                        Collections.singletonList("a"),
                                        repeatUnion))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("Recursive CTE")
                .hasMessageContaining("LogicalRepeatUnion");
    }

    @Test
    void testRejectsRecursiveTableSpoolExplicitly() {
        final RelNode input = toRelNode("SELECT a FROM FirstTable");
        final TableScan scan = findRel(input, TableScan.class);
        assertThat(scan).isNotNull();
        final RelNode tableSpool =
                LogicalTableSpool.create(input, Spool.Type.LAZY, Spool.Type.LAZY, scan.getTable());

        assertThatThrownBy(
                        () ->
                                PlannerColumnLineageExtractor.extract(
                                        "sink-recursive-spool",
                                        Collections.singletonList("a"),
                                        tableSpool))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("Recursive CTE")
                .hasMessageContaining("LogicalTableSpool");
    }

    private PlannerSinkColumnLineage extract(String sinkKey, String query, String... outputFields) {
        return PlannerColumnLineageExtractor.extract(
                sinkKey, Arrays.asList(outputFields), toRelNode(query));
    }

    private RelNode toRelNode(String query) {
        return TableTestUtil.toRelNode(util.getTableEnv().sqlQuery(query));
    }

    private static RelNode project(RelNode input, RexNode expression, String outputName) {
        return LogicalProject.create(
                input,
                Collections.emptyList(),
                Collections.singletonList(expression),
                Collections.singletonList(outputName));
    }

    private static PlannerColumnLineageRelation relation(
            PlannerSinkColumnLineage lineage, String outputField) {
        return lineage.getRelations().stream()
                .filter(relation -> relation.getOutputField().equals(outputField))
                .findFirst()
                .orElseThrow(
                        () -> new AssertionError("Missing output field lineage: " + outputField));
    }

    private static <T extends RelNode> T findRel(RelNode node, Class<T> type) {
        if (type.isInstance(node)) {
            return type.cast(node);
        }
        for (RelNode input : node.getInputs()) {
            final T match = findRel(input, type);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private static void assertInputs(
            PlannerColumnLineageRelation relation, String... expectedInputs) {
        final List<String> actualInputs =
                relation.getInputs().stream()
                        .map(PlannerColumnLineageExtractorTestBase::inputSummary)
                        .collect(Collectors.toList());
        assertThat(actualInputs).containsExactlyInAnyOrder(expectedInputs);
    }

    private static String inputSummary(PlannerColumnLineageInput input) {
        final List<String> qualifiedName = input.getDataset().getQualifiedName();
        return qualifiedName.get(qualifiedName.size() - 1)
                + "."
                + input.getFieldName()
                + ":"
                + input.getDependencyType();
    }

    private static final class InvalidProject extends Project {

        private final List<RexNode> exposedProjects;

        private InvalidProject(RelNode input, RexNode invalidExpression) {
            this(
                    input,
                    invalidExpression,
                    input.getCluster()
                            .getTypeFactory()
                            .builder()
                            .add("invalid", invalidExpression.getType())
                            .build());
        }

        private InvalidProject(RelNode input, RexNode invalidExpression, RelDataType rowType) {
            super(
                    input.getCluster(),
                    input.getTraitSet(),
                    Collections.emptyList(),
                    input,
                    Collections.singletonList(
                            new RexInputRef(0, input.getRowType().getFieldList().get(0).getType())),
                    rowType);
            this.exposedProjects = Collections.singletonList(invalidExpression);
        }

        @Override
        public List<RexNode> getProjects() {
            return exposedProjects;
        }

        @Override
        public Project copy(
                org.apache.calcite.plan.RelTraitSet traitSet,
                RelNode input,
                List<RexNode> projects,
                RelDataType rowType) {
            throw new UnsupportedOperationException("Invalid test project must not be copied.");
        }
    }

    /** Test scalar function whose output depends on both declared arguments. */
    public static class TestPlus extends ScalarFunction {
        public Long eval(Long value, Integer delta) {
            return value == null || delta == null ? null : value + delta;
        }
    }

    /** Test scalar function whose value is produced without input fields. */
    public static class TestZero extends ScalarFunction {
        public Long eval() {
            return 0L;
        }
    }
}

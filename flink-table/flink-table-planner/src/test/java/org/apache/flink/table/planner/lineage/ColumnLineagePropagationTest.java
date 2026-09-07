/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.lineage;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.lineage.ColumnLineageInput;
import org.apache.flink.streaming.api.lineage.ColumnLineageRelation;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.lineage.LineageGraphObservation;
import org.apache.flink.streaming.api.lineage.LineageGraphUtils;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TransformationWithLineage;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.CompiledPlanUtils;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.TransformationSinkProvider;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.OutputConversionModifyOperation;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.calcite.Sink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests complete column lineage propagation through direct and compiled-plan translation. */
class ColumnLineagePropagationTest {

    private static final String INSERT_SQL =
            "INSERT INTO LineageSink SELECT `value` + 1 FROM LineageSource";

    @Test
    void testAnonymousSinkDoesNotEraseNamedWriterLineage() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final StatementSet statements = environment.createStatementSet();
        statements.addInsert(
                TableDescriptor.forConnector("blackhole").build(),
                environment.from("LineageSource"));
        statements.addInsertSql(INSERT_SQL);
        final JsonNode json = new ObjectMapper().readTree(statements.compilePlan().asJsonString());
        assertThat(json.findValues("columnLineage")).hasSize(1);
        assertThat(json.findValues("columnLineage").get(0).get("sinkKey").asText())
                .isEqualTo(identifier("LineageSink").asSerializableString());
    }

    @Test
    void testAnonymousSourceDoesNotEraseUnrelatedWriterLineage() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        createValuesTable(environment, "OtherSink", "value");
        final StatementSet statements = environment.createStatementSet();
        statements.addInsert(
                "OtherSink",
                environment.from(
                        TableDescriptor.forConnector("values")
                                .schema(
                                        Schema.newBuilder()
                                                .column("value", DataTypes.BIGINT())
                                                .build())
                                .option("bounded", "true")
                                .build()));
        statements.addInsertSql(INSERT_SQL);
        final JsonNode json = new ObjectMapper().readTree(statements.compilePlan().asJsonString());
        assertThat(json.findValues("columnLineage")).hasSize(1);
        assertThat(json.findValues("columnLineage").get(0).get("sinkKey").asText())
                .isEqualTo(identifier("LineageSink").asSerializableString());
    }

    @Test
    void testSameDatasetWriterSlotsKeepSuccessfulContributionInEitherOrder() throws Exception {
        for (boolean unavailableFirst : new boolean[] {true, false}) {
            final TableEnvironmentImpl environment = createEnvironment();
            environment
                    .getConfig()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SINK_ENABLED, false);
            final StatementSet statements = environment.createStatementSet();
            if (!unavailableFirst) {
                statements.addInsertSql(INSERT_SQL);
            }
            statements.addInsert(
                    "LineageSink",
                    environment.from(
                            TableDescriptor.forConnector("values")
                                    .schema(
                                            Schema.newBuilder()
                                                    .column("value", DataTypes.BIGINT())
                                                    .build())
                                    .option("bounded", "true")
                                    .build()));
            if (unavailableFirst) {
                statements.addInsertSql(INSERT_SQL);
            }
            final List<Transformation<?>> transformations =
                    CompiledPlanUtils.toTransformations(environment, statements.compilePlan());
            assertThat(transformations).hasSize(2);
            assertThat(
                            ((TransformationWithLineage<?>)
                                            transformations.get(unavailableFirst ? 0 : 1))
                                    .getColumnLineage())
                    .isNull();
            assertThat(
                            ((TransformationWithLineage<?>)
                                            transformations.get(unavailableFirst ? 1 : 0))
                                    .getColumnLineage())
                    .isNotNull();
            assertThat(transformations)
                    .filteredOn(TransformationWithLineage.class::isInstance)
                    .extracting(
                            transformation ->
                                    ((TransformationWithLineage<?>) transformation)
                                            .getColumnLineage())
                    .filteredOn(lineage -> lineage != null)
                    .hasSize(1);
            assertThat(LineageGraphUtils.observe(transformations).columnRelations()).isEmpty();
        }
    }

    @Test
    void testUnsupportedColumnsPreserveIndependentTablesAndOtherSinkAcrossRestore()
            throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        createValuesTable(environment, "OtherSource", "value");
        environment.createTemporaryTable(
                "OtherSink",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("value", DataTypes.BIGINT()).build())
                        .option("sink-insert-only", "false")
                        .build());
        final StatementSet statements = environment.createStatementSet();
        statements.addInsertSql(INSERT_SQL);
        statements.addInsertSql(
                "INSERT INTO OtherSink SELECT `value` FROM LineageSource "
                        + "INTERSECT SELECT `value` FROM OtherSource");
        final String json = statements.compilePlan().asJsonString();
        final LineageGraphObservation observation =
                LineageGraphUtils.observe(
                        CompiledPlanUtils.toTransformations(
                                environment,
                                environment.loadPlan(PlanReference.fromJsonString(json))));

        assertThat(observation.getTableStatus()).isEqualTo("COMPLETE");
        assertThat(observation.getColumnStatus()).isEqualTo("PARTIAL");
        assertThat(observation.columnRelations())
                .extracting(relation -> relation.outputDataset().name())
                .containsExactly(identifier("LineageSink").asSerializableString());
        assertThat(observation.relations())
                .flatExtracting(edge -> edge.source().datasets())
                .extracting(dataset -> dataset.name())
                .contains(identifier("OtherSource").asSerializableString());
    }

    @Test
    void testOldPlanWithoutIndependentTablesDoesNotClaimCompleteTableLineage() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final JsonNode json =
                new ObjectMapper().readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        assertThat(json.findParents("tableLineage")).hasSize(1);
        json.findParents("tableLineage")
                .forEach(parent -> ((ObjectNode) parent).remove("tableLineage"));
        final LineageGraphObservation observation =
                LineageGraphUtils.observe(
                        CompiledPlanUtils.toTransformations(
                                environment,
                                environment.loadPlan(
                                        PlanReference.fromJsonString(json.toString()))));
        assertThat(observation.getTableStatus()).isEqualTo("PARTIAL");
        assertThat(observation.getColumnStatus()).isEqualTo("COMPLETE");
        assertThat(observation.columnRelations()).hasSize(1);
    }

    @Test
    void testMixedWritersDoNotPublishPartialColumnsForSharedDataset() throws Exception {
        for (boolean reuse : new boolean[] {false, true}) {
            final TableEnvironmentImpl environment = createEnvironment();
            createValuesTable(environment, "OtherSource", "value");
            environment.createTemporaryTable(
                    "SharedSink",
                    TableDescriptor.forConnector("values")
                            .schema(Schema.newBuilder().column("value", DataTypes.BIGINT()).build())
                            .option("sink-insert-only", "false")
                            .build());
            environment
                    .getConfig()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SINK_ENABLED, reuse);
            final StatementSet statements = environment.createStatementSet();
            statements.addInsertSql(INSERT_SQL);
            statements.addInsertSql("INSERT INTO SharedSink SELECT `value` FROM LineageSource");
            statements.addInsertSql(
                    "INSERT INTO SharedSink SELECT `value` FROM LineageSource INTERSECT SELECT `value` FROM OtherSource");
            final LineageGraphObservation observation =
                    LineageGraphUtils.observe(
                            CompiledPlanUtils.toTransformations(
                                    environment,
                                    environment.loadPlan(
                                            PlanReference.fromJsonString(
                                                    statements.compilePlan().asJsonString()))));
            assertThat(observation.getTableStatus()).isEqualTo("COMPLETE");
            assertThat(observation.getColumnStatus()).isEqualTo("PARTIAL");
            assertThat(observation.columnRelations())
                    .extracting(relation -> relation.outputDataset().name())
                    .containsExactly(identifier("LineageSink").asSerializableString());
        }
    }

    @Test
    void testUnsupportedColumnsKeepOptimizerPrunedLogicalTables() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        createValuesTable(environment, "OtherSource", "value");
        final String json =
                environment
                        .compilePlanSql(
                                "INSERT INTO LineageSink SELECT `value` FROM LineageSource WHERE 1=0 "
                                        + "INTERSECT SELECT `value` FROM OtherSource WHERE 1=0")
                        .asJsonString();
        environment.dropTemporaryTable("LineageSource");
        environment.dropTemporaryTable("OtherSource");
        final LineageGraphObservation observation =
                LineageGraphUtils.observe(
                        CompiledPlanUtils.toTransformations(
                                environment,
                                environment.loadPlan(PlanReference.fromJsonString(json))));
        assertThat(observation.getTableStatus()).isEqualTo("COMPLETE");
        assertThat(observation.getColumnStatus()).isEqualTo("UNAVAILABLE");
        assertThat(observation.sources())
                .flatExtracting(SourceLineageVertex::datasets)
                .extracting(dataset -> dataset.name())
                .containsExactlyInAnyOrder(
                        identifier("LineageSource").asSerializableString(),
                        identifier("OtherSource").asSerializableString());
    }

    @Test
    void testPrunedSourceRetainsLogicalLineageWithoutRuntimeRead() throws Exception {
        for (String projection : new String[] {"`value`", "1"}) {
            final TableEnvironmentImpl environment = createEnvironment();
            final String sql =
                    "INSERT INTO LineageSink SELECT "
                            + projection
                            + " FROM LineageSource WHERE 1=0";
            final CompiledPlan compiled = environment.compilePlanSql(sql);
            final String json = compiled.asJsonString();
            // Restoring lineage must use the frozen source metadata, not today's catalog.
            environment.dropTemporaryTable("LineageSource");
            final CompiledPlan restored = environment.loadPlan(PlanReference.fromJsonString(json));
            final List<Transformation<?>> transformations =
                    CompiledPlanUtils.toTransformations(environment, restored);
            assertThat(transformations)
                    .flatExtracting(Transformation::getTransitivePredecessors)
                    .filteredOn(TransformationWithLineage.class::isInstance)
                    .noneSatisfy(
                            transformation ->
                                    assertThat(
                                                    ((TransformationWithLineage<?>) transformation)
                                                            .getLineageVertex())
                                            .isInstanceOf(SourceLineageVertex.class));
            final LineageGraph graph = LineageGraphUtils.convertToLineageGraph(transformations);
            assertThat(graph.sources())
                    .flatExtracting(SourceLineageVertex::datasets)
                    .extracting(dataset -> dataset.name())
                    .containsExactly(identifier("LineageSource").asSerializableString());
            assertThat(graph.columnRelations()).hasSize(1);
            if (projection.equals("1")) {
                assertThat(graph.columnRelations().get(0).inputs()).isEmpty();
            } else {
                assertThat(graph.columnRelations().get(0).inputs())
                        .extracting(ColumnLineageInput::inputField)
                        .containsExactly("value");
                assertThat(
                                graph.columnRelations()
                                        .get(0)
                                        .inputs()
                                        .get(0)
                                        .inputDataset()
                                        .namespace())
                        .startsWith("values://");
            }
        }
    }

    @Test
    void testPrunedAndLiveSourcesSurviveSinkReuse() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        createValuesTable(environment, "LiveSource", "value");
        environment
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SINK_ENABLED, true);
        final StatementSet statements = environment.createStatementSet();
        statements.addInsertSql(
                "INSERT INTO LineageSink SELECT `value` FROM LineageSource WHERE 1=0");
        statements.addInsertSql("INSERT INTO LineageSink SELECT `value` FROM LiveSource");
        final CompiledPlan restored =
                environment.loadPlan(
                        PlanReference.fromJsonString(statements.compilePlan().asJsonString()));
        final LineageGraph graph =
                LineageGraphUtils.convertToLineageGraph(
                        CompiledPlanUtils.toTransformations(environment, restored));
        assertThat(graph.columnRelations())
                .flatExtracting(ColumnLineageRelation::inputs)
                .extracting(input -> input.inputDataset().name())
                .containsExactlyInAnyOrder(
                        identifier("LineageSource").asSerializableString(),
                        identifier("LiveSource").asSerializableString());
    }

    @Test
    void testOldPlanWithoutPruningEvidenceExecutesWithoutLineage() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode json =
                mapper.readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        final List<ObjectNode> lineages = new ArrayList<>();
        collectColumnLineages(json, lineages);
        assertThat(lineages).hasSize(1);
        lineages.get(0).remove("prunedSources");
        final CompiledPlan restored =
                environment.loadPlan(PlanReference.fromJsonString(mapper.writeValueAsString(json)));
        assertLineageUnavailable(environment, restored);
    }

    @Test
    void testPrunedSourceSnapshotCannotHideMissingMetadata() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final ObjectMapper mapper = new ObjectMapper();
        final String plan = environment.compilePlanSql(INSERT_SQL + " WHERE 1=0").asJsonString();
        final JsonNode missingSnapshot = mapper.readTree(plan);
        final List<ObjectNode> lineages = new ArrayList<>();
        collectColumnLineages(missingSnapshot, lineages);
        assertThat(lineages).hasSize(1);
        lineages.get(0).putArray("prunedSources");
        final CompiledPlan missing =
                environment.loadPlan(
                        PlanReference.fromJsonString(mapper.writeValueAsString(missingSnapshot)));
        assertLineageUnavailable(environment, missing);

        final JsonNode unknownField = mapper.readTree(plan);
        assertThat(replaceFirstInputField(unknownField, "does_not_exist")).isEqualTo(1);
        final CompiledPlan invalidField =
                environment.loadPlan(
                        PlanReference.fromJsonString(mapper.writeValueAsString(unknownField)));
        assertLineageUnavailable(environment, invalidField);
    }

    @Test
    void testLiveSourceCannotBeDeclaredPruned() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode pruned =
                mapper.readTree(
                        environment.compilePlanSql(INSERT_SQL + " WHERE 1=0").asJsonString());
        final JsonNode live =
                mapper.readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        final List<ObjectNode> prunedLineages = new ArrayList<>();
        final List<ObjectNode> liveLineages = new ArrayList<>();
        collectColumnLineages(pruned, prunedLineages);
        collectColumnLineages(live, liveLineages);
        assertThat(prunedLineages).hasSize(1);
        assertThat(liveLineages).hasSize(1);
        liveLineages.get(0).set("prunedSources", prunedLineages.get(0).get("prunedSources"));
        final CompiledPlan restored =
                environment.loadPlan(PlanReference.fromJsonString(mapper.writeValueAsString(live)));
        assertLineageUnavailable(environment, restored);
    }

    @Test
    void testDirectStreamingAndBatchRetainPrunedSource() {
        for (EnvironmentSettings settings :
                new EnvironmentSettings[] {
                    EnvironmentSettings.inStreamingMode(), EnvironmentSettings.inBatchMode()
                }) {
            final TableEnvironmentImpl environment =
                    (TableEnvironmentImpl) TableEnvironmentImpl.create(settings);
            createValuesTable(environment, "LineageSource", "value");
            createValuesTable(environment, "LineageSink", "result");
            final ModifyOperation operation =
                    (ModifyOperation)
                            environment.getParser().parse(INSERT_SQL + " WHERE 1=0").get(0);
            final LineageGraph graph =
                    LineageGraphUtils.convertToLineageGraph(
                            environment
                                    .getPlanner()
                                    .translate(Collections.singletonList(operation)));
            assertThat(graph.columnRelations())
                    .flatExtracting(ColumnLineageRelation::inputs)
                    .extracting(ColumnLineageInput::inputField)
                    .containsExactly("value");
        }
    }

    @Test
    void testPruningDoesNotOverrideCatalogSerializationPolicy() {
        final TableEnvironmentImpl environment = createEnvironment();
        environment
                .getConfig()
                .set(
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanCompilation.IDENTIFIER);
        assertThat(environment.compilePlanSql(INSERT_SQL + " WHERE 1=0").asJsonString())
                .doesNotContain("\"columnLineage\"");
    }

    @Test
    void testDirectAndCompiledPlanProduceEquivalentColumnLineage() throws Exception {
        final TableEnvironmentImpl directEnvironment = createEnvironment();
        final LineageGraph direct = directLineage(directEnvironment);

        final TableEnvironmentImpl compiledEnvironment = createEnvironment();
        final CompiledPlan compiledPlan = compiledEnvironment.compilePlanSql(INSERT_SQL);
        final CompiledPlan restored =
                compiledEnvironment.loadPlan(
                        PlanReference.fromJsonString(compiledPlan.asJsonString()));
        final LineageGraph compiled =
                LineageGraphUtils.convertToLineageGraph(
                        CompiledPlanUtils.toTransformations(compiledEnvironment, restored));

        assertThat(direct.columnRelations()).hasSize(1);
        assertThat(compiled.columnRelations()).hasSize(1);
        assertThat(direct.columnRelations().get(0).outputDataset().namespace())
                .startsWith("values://");
        assertRelationEquivalent(
                direct.columnRelations().get(0), compiled.columnRelations().get(0));
    }

    @Test
    void testConstantProjectionStillRequiresAndRetainsNamedSource() {
        final TableEnvironmentImpl environment = createEnvironment();
        final CompiledPlan compiledPlan =
                environment.compilePlanSql("INSERT INTO LineageSink SELECT 1 FROM LineageSource");

        final LineageGraph graph =
                LineageGraphUtils.convertToLineageGraph(
                        CompiledPlanUtils.toTransformations(environment, compiledPlan));

        assertThat(graph.sources())
                .flatExtracting(SourceLineageVertex::datasets)
                .extracting(dataset -> dataset.name())
                .contains(identifier("LineageSource").asSerializableString());
        assertThat(graph.columnRelations())
                .singleElement()
                .satisfies(
                        relation -> {
                            assertThat(relation.inputs()).isEmpty();
                            assertThat(relation.outputField()).isEqualTo("result");
                        });
    }

    @Test
    void testStatementSetBindsEachNamedSinkIndependently() {
        final TableEnvironmentImpl environment = createEnvironment();
        environment.createTemporaryTable(
                "SecondLineageSink",
                TableDescriptor.forConnector("values")
                        .schema(
                                Schema.newBuilder()
                                        .column("second_result", DataTypes.BIGINT())
                                        .build())
                        .build());
        final StatementSet statementSet = environment.createStatementSet();
        statementSet.addInsertSql(INSERT_SQL);
        statementSet.addInsertSql(
                "INSERT INTO SecondLineageSink SELECT `value` + 2 FROM LineageSource");

        final LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(
                        CompiledPlanUtils.toTransformations(
                                environment, statementSet.compilePlan()));

        assertThat(lineageGraph.columnRelations())
                .extracting(
                        relation -> relation.outputDataset().name(),
                        ColumnLineageRelation::outputField)
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple(
                                identifier("LineageSink").asSerializableString(), "result"),
                        org.assertj.core.groups.Tuple.tuple(
                                identifier("SecondLineageSink").asSerializableString(),
                                "second_result"));
    }

    @Test
    void testMultipleWritesToSameSinkReuseAndMergeColumnLineage() {
        final TableEnvironmentImpl environment = createEnvironment();
        environment
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SINK_ENABLED, true);
        final StatementSet statementSet = environment.createStatementSet();
        statementSet.addInsertSql("INSERT INTO LineageSink SELECT `value` FROM LineageSource");
        statementSet.addInsertSql(
                "INSERT INTO LineageSink "
                        + "SELECT CASE WHEN `value` > 0 THEN `value` ELSE 0 END FROM LineageSource");

        final List<Transformation<?>> transformations =
                CompiledPlanUtils.toTransformations(environment, statementSet.compilePlan());
        final LineageGraph graph = LineageGraphUtils.convertToLineageGraph(transformations);

        assertThat(transformations).hasSize(1);
        final Set<Transformation<?>> sourceTransformations =
                Collections.newSetFromMap(new IdentityHashMap<>());
        transformations.stream()
                .flatMap(transformation -> transformation.getTransitivePredecessors().stream())
                .filter(TransformationWithLineage.class::isInstance)
                .filter(
                        transformation ->
                                ((TransformationWithLineage<?>) transformation).getLineageVertex()
                                        instanceof SourceLineageVertex)
                .forEach(sourceTransformations::add);
        assertThat(sourceTransformations).hasSize(1);
        assertThat(graph.columnRelations()).hasSize(1);
        final ColumnLineageRelation relation = graph.columnRelations().get(0);
        assertThat(relation.inputs())
                .extracting(ColumnLineageInput::dependencyType)
                .contains(
                        org.apache.flink.streaming.api.lineage.ColumnLineageDependencyType.DIRECT,
                        org.apache.flink.streaming.api.lineage.ColumnLineageDependencyType
                                .INDIRECT);
        assertThat(relation.transformation())
                .hasValueSatisfying(value -> assertThat(value).contains("UNION"));
    }

    @Test
    void testSinkReuseConfigurationPreservesAllSourceDependencies() {
        for (boolean[] settings : new boolean[][] {{true, true}, {false, true}, {true, false}}) {
            final boolean reuse = settings[0];
            final boolean reuseSubplans = settings[1];
            final TableEnvironmentImpl environment = createEnvironment();
            environment
                    .getConfig()
                    .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SINK_ENABLED, reuse);
            environment
                    .getConfig()
                    .set(
                            OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED,
                            reuseSubplans);
            createValuesTable(environment, "OtherSource", "value");
            final StatementSet statements = environment.createStatementSet();
            statements.addInsertSql("INSERT INTO LineageSink SELECT `value` FROM LineageSource");
            statements.addInsertSql("INSERT INTO LineageSink SELECT `value` FROM OtherSource");
            final CompiledPlan plan = statements.compilePlan();
            final CompiledPlan restored =
                    environment.loadPlan(PlanReference.fromJsonString(plan.asJsonString()));
            final List<Transformation<?>> transformations =
                    CompiledPlanUtils.toTransformations(environment, restored);
            assertThat(transformations).hasSize(reuse && reuseSubplans ? 1 : 2);
            final LineageGraph graph = LineageGraphUtils.convertToLineageGraph(transformations);
            assertThat(graph.columnRelations())
                    .singleElement()
                    .satisfies(
                            relation ->
                                    assertThat(relation.inputs())
                                            .extracting(input -> input.inputDataset().name())
                                            .containsExactlyInAnyOrder(
                                                    identifier("LineageSource")
                                                            .asSerializableString(),
                                                    identifier("OtherSource")
                                                            .asSerializableString()));
        }
    }

    @Test
    void testReusedConstantWritesRetainBothSourceTables() {
        final TableEnvironmentImpl environment = createEnvironment();
        createValuesTable(environment, "OtherSource", "value");
        final StatementSet statements = environment.createStatementSet();
        statements.addInsertSql("INSERT INTO LineageSink SELECT 1 FROM LineageSource");
        statements.addInsertSql("INSERT INTO LineageSink SELECT 2 FROM OtherSource");
        final List<Transformation<?>> transformations =
                CompiledPlanUtils.toTransformations(environment, statements.compilePlan());
        assertThat(transformations).hasSize(1);
        final LineageGraph graph = LineageGraphUtils.convertToLineageGraph(transformations);
        assertThat(graph.sources())
                .flatExtracting(SourceLineageVertex::datasets)
                .extracting(dataset -> dataset.name())
                .containsExactlyInAnyOrder(
                        identifier("LineageSource").asSerializableString(),
                        identifier("OtherSource").asSerializableString());
        assertThat(graph.columnRelations())
                .singleElement()
                .satisfies(
                        relation -> {
                            assertThat(relation.inputs()).isEmpty();
                            assertThat(relation.origin())
                                    .isEqualTo(
                                            org.apache.flink.streaming.api.lineage
                                                    .ColumnLineageOrigin.CONSTANT);
                        });
    }

    @Test
    void testPartialTargetColumnWritesKeepEveryLogicalSinkRoot() {
        final TableEnvironmentImpl environment = createEnvironment();
        environment.createTemporaryTable(
                "PartialSink",
                TableDescriptor.forConnector("values")
                        .schema(
                                Schema.newBuilder()
                                        .column("left_result", DataTypes.BIGINT())
                                        .column("right_result", DataTypes.BIGINT())
                                        .build())
                        .build());
        final StatementSet statementSet = environment.createStatementSet();
        statementSet.addInsertSql(
                "INSERT INTO PartialSink (left_result) SELECT `value` FROM LineageSource");
        statementSet.addInsertSql(
                "INSERT INTO PartialSink (right_result) SELECT `value` + 1 FROM LineageSource");
        statementSet.addInsertSql(
                "INSERT OVERWRITE PartialSink (left_result) SELECT `value` + 2 FROM LineageSource");

        final List<Transformation<?>> transformations =
                CompiledPlanUtils.toTransformations(environment, statementSet.compilePlan());
        final LineageGraph graph = LineageGraphUtils.convertToLineageGraph(transformations);

        assertThat(transformations).hasSize(3);
        assertThat(graph.columnRelations())
                .extracting(ColumnLineageRelation::outputField)
                .containsExactlyInAnyOrder("left_result", "right_result");
    }

    @Test
    void testDynamicOptionsHintsOnlyReuseCompatibleSinks() {
        final TableEnvironmentImpl environment = createEnvironment();
        final StatementSet statementSet = environment.createStatementSet();
        statementSet.addInsertSql(
                "INSERT INTO LineageSink /*+ OPTIONS('sink-insert-only' = 'true') */ "
                        + "SELECT `value` FROM LineageSource");
        statementSet.addInsertSql(
                "INSERT INTO LineageSink /*+ OPTIONS('sink-insert-only' = 'false') */ "
                        + "SELECT `value` + 1 FROM LineageSource");
        statementSet.addInsertSql(
                "INSERT INTO LineageSink /*+ OPTIONS('sink-insert-only' = 'true') */ "
                        + "SELECT `value` + 2 FROM LineageSource");

        final List<Transformation<?>> transformations =
                CompiledPlanUtils.toTransformations(environment, statementSet.compilePlan());
        final LineageGraph graph = LineageGraphUtils.convertToLineageGraph(transformations);

        assertThat(transformations).hasSize(2);
        assertThat(graph.columnRelations()).hasSize(2);
        assertThat(graph.columnRelations())
                .extracting(ColumnLineageRelation::outputField)
                .containsOnly("result");
        assertThat(graph.columnRelations())
                .anySatisfy(
                        relation ->
                                assertThat(relation.transformation())
                                        .hasValueSatisfying(
                                                value -> assertThat(value).contains("UNION")));
    }

    @Test
    void testCompiledPlanWithoutColumnLineageStillTranslates() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final CompiledPlan compiledPlan = environment.compilePlanSql(INSERT_SQL);
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode planJson = objectMapper.readTree(compiledPlan.asJsonString());
        removeColumnLineage(planJson);

        final CompiledPlan legacyPlan =
                environment.loadPlan(
                        PlanReference.fromJsonString(objectMapper.writeValueAsString(planJson)));

        assertLineageUnavailable(environment, legacyPlan);
    }

    @Test
    void testEmptyColumnLineageDoesNotPreventTranslation() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode planJson =
                objectMapper.readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        assertThat(emptyColumnLineage(planJson)).isEqualTo(1);
        final CompiledPlan tampered =
                environment.loadPlan(
                        PlanReference.fromJsonString(objectMapper.writeValueAsString(planJson)));

        assertLineageUnavailable(environment, tampered);
    }

    @Test
    void testSwappedSinkKeyWithholdsLineageOnly() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode planJson =
                objectMapper.readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        assertThat(replaceSinkKey(planJson, "`default_catalog`.`default_database`.`OtherSink`"))
                .isEqualTo(1);
        final CompiledPlan tampered =
                environment.loadPlan(
                        PlanReference.fromJsonString(objectMapper.writeValueAsString(planJson)));

        assertLineageUnavailable(environment, tampered);
    }

    @Test
    void testTwoSwappedSinkKeysWithholdLineageOnly() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        environment.createTemporaryTable(
                "SecondLineageSink",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("result", DataTypes.BIGINT()).build())
                        .build());
        final StatementSet statementSet = environment.createStatementSet();
        statementSet.addInsertSql(INSERT_SQL);
        statementSet.addInsertSql(
                "INSERT INTO SecondLineageSink SELECT `value` + 2 FROM LineageSource");
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode planJson = objectMapper.readTree(statementSet.compilePlan().asJsonString());
        final List<ObjectNode> columnLineages = new ArrayList<>();
        collectColumnLineages(planJson, columnLineages);
        assertThat(columnLineages).hasSize(2);
        final String firstSinkKey = columnLineages.get(0).get("sinkKey").asText();
        final String secondSinkKey = columnLineages.get(1).get("sinkKey").asText();
        columnLineages.get(0).put("sinkKey", secondSinkKey);
        columnLineages.get(1).put("sinkKey", firstSinkKey);
        final CompiledPlan tampered =
                environment.loadPlan(
                        PlanReference.fromJsonString(objectMapper.writeValueAsString(planJson)));

        assertLineageUnavailable(environment, tampered);
    }

    @Test
    void testUnknownInputFieldWithholdsLineageOnly() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode planJson =
                objectMapper.readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        assertThat(replaceFirstInputField(planJson, "does_not_exist")).isEqualTo(1);
        final CompiledPlan tampered =
                environment.loadPlan(
                        PlanReference.fromJsonString(objectMapper.writeValueAsString(planJson)));

        assertLineageUnavailable(environment, tampered);
    }

    @Test
    void testMissingRuntimeSourceWithholdsLineageOnly() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode planJson =
                objectMapper.readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        assertThat(addMissingExpectedSource(planJson)).isEqualTo(1);
        final CompiledPlan tampered =
                environment.loadPlan(
                        PlanReference.fromJsonString(objectMapper.writeValueAsString(planJson)));

        assertLineageUnavailable(environment, tampered);
    }

    @Test
    void testQuotedDottedIdentifiersRemainUnambiguous() {
        final TableEnvironmentImpl environment =
                (TableEnvironmentImpl)
                        TableEnvironmentImpl.create(EnvironmentSettings.inStreamingMode());
        createValuesTable(environment, "source.one", "value");
        createValuesTable(environment, "source.two", "value");
        createValuesTable(environment, "sink.one", "result");
        createValuesTable(environment, "sink.two", "result");
        final StatementSet statementSet = environment.createStatementSet();
        statementSet.addInsertSql("INSERT INTO `sink.one` SELECT `value` FROM `source.one`");
        statementSet.addInsertSql("INSERT INTO `sink.two` SELECT `value` FROM `source.two`");

        final LineageGraph graph =
                LineageGraphUtils.convertToLineageGraph(
                        CompiledPlanUtils.toTransformations(
                                environment, statementSet.compilePlan()));

        assertThat(graph.columnRelations())
                .extracting(relation -> relation.outputDataset().name())
                .containsExactlyInAnyOrder(
                        identifier("sink.one").asSerializableString(),
                        identifier("sink.two").asSerializableString());
        assertThat(graph.columnRelations())
                .flatExtracting(ColumnLineageRelation::inputs)
                .extracting(input -> input.inputDataset().name())
                .containsExactlyInAnyOrder(
                        identifier("source.one").asSerializableString(),
                        identifier("source.two").asSerializableString());
    }

    @Test
    void testNamedSinkWithoutConnectorNamespaceUsesLogicalCatalogNamespace() {
        final TableEnvironmentImpl environment = createEnvironment();
        environment.createTemporaryTable(
                "NoLineageSink",
                TableDescriptor.forConnector("blackhole")
                        .schema(Schema.newBuilder().column("result", DataTypes.BIGINT()).build())
                        .build());
        final CompiledPlan compiledPlan =
                environment.compilePlanSql(
                        "INSERT INTO NoLineageSink SELECT `value` FROM LineageSource");

        final LineageGraph graph =
                LineageGraphUtils.convertToLineageGraph(
                        CompiledPlanUtils.toTransformations(environment, compiledPlan));

        assertThat(graph.columnRelations()).hasSize(1);
        assertThat(graph.columnRelations().get(0).outputDataset().namespace())
                .isEqualTo("flink://catalog/default_catalog");
    }

    @Test
    void testCustomTransformationSinkProviderCannotSilentlyDropColumnLineage() {
        final TableEnvironmentImpl environment = createEnvironment();
        environment.createTemporaryTable(
                "CustomTransformationSink",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("result", DataTypes.BIGINT()).build())
                        .option(
                                "table-sink-class",
                                InputPassthroughTransformationTableSink.class.getName())
                        .build());
        final CompiledPlan compiledPlan =
                environment.compilePlanSql(
                        "INSERT INTO CustomTransformationSink "
                                + "SELECT `value` FROM LineageSource");

        assertLineageUnavailable(environment, compiledPlan);
    }

    @Test
    void testExplainUsesTheSameColumnLineagePipeline() {
        final TableEnvironmentImpl environment = createEnvironment();

        assertThat(environment.explainSql(INSERT_SQL)).contains("LineageSink");
    }

    @Test
    void testAnonymousTableSinkStillCompilesWithoutLineage() {
        final TableEnvironmentImpl environment = createEnvironment();

        assertThat(
                        environment
                                .from("LineageSource")
                                .insertInto(TableDescriptor.forConnector("blackhole").build())
                                .compilePlan()
                                .asJsonString())
                .doesNotContain("\"columnLineage\"");
    }

    @Test
    void testTableToDataStreamConversionRemainsCompatible() {
        final TableEnvironmentImpl environment = createEnvironment();
        final TableImpl table = (TableImpl) environment.from("LineageSource");
        final ModifyOperation operation =
                new OutputConversionModifyOperation(
                        table.getQueryOperation(),
                        table.getResolvedSchema().toPhysicalRowDataType(),
                        OutputConversionModifyOperation.UpdateMode.APPEND);

        assertThat(environment.getPlanner().translate(Collections.singletonList(operation)))
                .isNotEmpty();
    }

    @Test
    void testUnsupportedModifyRootCannotBypassLineageGate() {
        final TableEnvironmentImpl environment = createEnvironment();
        final ModifyOperation operation =
                (ModifyOperation) environment.getParser().parse(INSERT_SQL).get(0);
        final RelNode sinkRoot = ((PlannerBase) environment.getPlanner()).translateToRel(operation);
        final RelNode unsupportedRoot = ((Sink) sinkRoot).getInput();

        assertThatThrownBy(
                        () ->
                                PlannerColumnLineagePlanBinder.extract(
                                        Collections.singletonList(unsupportedRoot),
                                        Collections.singletonList(operation)))
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining(operation.getClass().getName())
                .hasMessageContaining(unsupportedRoot.getClass().getName())
                .hasMessageContaining("field '<unknown>'");
    }

    private static void assertLineageUnavailable(
            TableEnvironmentImpl environment, CompiledPlan plan) {
        final List<Transformation<?>> transformations =
                CompiledPlanUtils.toTransformations(environment, plan);
        assertThat(transformations).isNotEmpty();
        assertThatThrownBy(() -> LineageGraphUtils.convertToLineageGraph(transformations))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Lineage");
    }

    private static TableEnvironmentImpl createEnvironment() {
        final TableEnvironmentImpl environment =
                (TableEnvironmentImpl)
                        TableEnvironmentImpl.create(EnvironmentSettings.inStreamingMode());
        environment.createTemporaryTable(
                "LineageSource",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("value", DataTypes.BIGINT()).build())
                        .option("bounded", "true")
                        .build());
        environment.createTemporaryTable(
                "LineageSink",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("result", DataTypes.BIGINT()).build())
                        .build());
        return environment;
    }

    private static void createValuesTable(
            TableEnvironmentImpl environment, String tableName, String fieldName) {
        environment.createTemporaryTable(
                "`" + tableName + "`",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column(fieldName, DataTypes.BIGINT()).build())
                        .option("bounded", "true")
                        .build());
    }

    private static ObjectIdentifier identifier(String objectName) {
        return ObjectIdentifier.of("default_catalog", "default_database", objectName);
    }

    private static LineageGraph directLineage(TableEnvironmentImpl environment) {
        final ModifyOperation operation =
                (ModifyOperation) environment.getParser().parse(INSERT_SQL).get(0);
        final List<Transformation<?>> transformations =
                environment.getPlanner().translate(Collections.singletonList(operation));
        return LineageGraphUtils.convertToLineageGraph(transformations);
    }

    private static void removeColumnLineage(JsonNode jsonNode) {
        if (jsonNode.isObject()) {
            ((ObjectNode) jsonNode).remove("columnLineage");
        }
        jsonNode.elements().forEachRemaining(ColumnLineagePropagationTest::removeColumnLineage);
    }

    private static int emptyColumnLineage(JsonNode node) {
        int changed = 0;
        if (node.isObject() && node.has("columnLineage")) {
            final ObjectNode columnLineage = (ObjectNode) node.get("columnLineage");
            columnLineage.putArray("expectedOutputFields");
            columnLineage.putArray("relations");
            changed++;
        }
        for (JsonNode child : node) {
            changed += emptyColumnLineage(child);
        }
        return changed;
    }

    private static int replaceSinkKey(JsonNode node, String sinkKey) {
        int changed = 0;
        if (node.isObject() && node.has("columnLineage")) {
            ((ObjectNode) node.get("columnLineage")).put("sinkKey", sinkKey);
            changed++;
        }
        for (JsonNode child : node) {
            changed += replaceSinkKey(child, sinkKey);
        }
        return changed;
    }

    private static void collectColumnLineages(JsonNode node, List<ObjectNode> columnLineages) {
        if (node.isObject() && node.has("columnLineage")) {
            columnLineages.add((ObjectNode) node.get("columnLineage"));
        }
        node.elements().forEachRemaining(child -> collectColumnLineages(child, columnLineages));
    }

    private static int replaceFirstInputField(JsonNode node, String fieldName) {
        if (node.isObject()
                && node.has("dataset")
                && node.has("field")
                && node.has("dependencyType")) {
            ((ObjectNode) node).put("field", fieldName);
            return 1;
        }
        for (JsonNode child : node) {
            final int changed = replaceFirstInputField(child, fieldName);
            if (changed > 0) {
                return changed;
            }
        }
        return 0;
    }

    private static int addMissingExpectedSource(JsonNode node) {
        int changed = 0;
        if (node.isObject() && node.has("columnLineage")) {
            ((ObjectNode) node.get("columnLineage"))
                    .withArray("expectedSources")
                    .addObject()
                    .putArray("qualifiedName")
                    .add("default_catalog")
                    .add("default_database")
                    .add("MissingSource");
            changed++;
        }
        for (JsonNode child : node) {
            changed += addMissingExpectedSource(child);
        }
        return changed;
    }

    private static void assertRelationEquivalent(
            ColumnLineageRelation direct, ColumnLineageRelation compiled) {
        assertThat(compiled.outputDataset().namespace())
                .isEqualTo(direct.outputDataset().namespace());
        assertThat(compiled.outputDataset().name()).isEqualTo(direct.outputDataset().name());
        assertThat(compiled.outputField()).isEqualTo(direct.outputField()).isEqualTo("result");
        assertThat(compiled.origin()).isEqualTo(direct.origin());
        assertThat(compiled.transformation()).isEqualTo(direct.transformation());
        assertThat(compiled.inputs()).hasSize(1);
        assertThat(direct.inputs()).hasSize(1);
        final ColumnLineageInput directInput = direct.inputs().get(0);
        final ColumnLineageInput compiledInput = compiled.inputs().get(0);
        assertThat(compiledInput.inputDataset().namespace())
                .isEqualTo(directInput.inputDataset().namespace());
        assertThat(compiledInput.inputDataset().name())
                .isEqualTo(directInput.inputDataset().name());
        assertThat(compiledInput.inputField())
                .isEqualTo(directInput.inputField())
                .isEqualTo("value");
        assertThat(compiledInput.dependencyType()).isEqualTo(directInput.dependencyType());
    }

    /** Custom provider whose non-carrier result makes lineage unavailable, not execution. */
    public static final class InputPassthroughTransformationTableSink implements DynamicTableSink {

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return (TransformationSinkProvider)
                    providerContext ->
                            new PartitionTransformation<>(
                                    providerContext.getInputTransformation(),
                                    new RebalancePartitioner<>());
        }

        @Override
        public DynamicTableSink copy() {
            return new InputPassthroughTransformationTableSink();
        }

        @Override
        public String asSummaryString() {
            return "InputPassthroughTransformationTableSink";
        }
    }
}

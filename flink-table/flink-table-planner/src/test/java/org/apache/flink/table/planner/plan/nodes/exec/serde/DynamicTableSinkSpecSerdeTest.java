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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.FileSystemTableFactory;
import org.apache.flink.formats.testcsv.TestCsvFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.DefaultIndex;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageDependencyType;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageInput;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageOrigin;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageRelation;
import org.apache.flink.table.planner.lineage.PlannerLineageDataset;
import org.apache.flink.table.planner.lineage.PlannerSinkColumnLineage;
import org.apache.flink.table.planner.lineage.PlannerSinkTableLineage;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.PartitioningSpec;
import org.apache.flink.table.planner.plan.abilities.sink.TargetColumnWritingSpec;
import org.apache.flink.table.planner.plan.abilities.sink.WritingMetadataSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.utils.PlannerMocks;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.CatalogManagerMocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS;
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.TestDynamicTableFactory.BUFFER_SIZE;
import static org.apache.flink.table.factories.TestDynamicTableFactory.TARGET;
import static org.apache.flink.table.factories.TestFormatFactory.DELIMITER;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.DynamicTableSourceSpecSerdeTest.tableWithOnlyPhysicalColumns;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.configuredSerdeContext;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toJson;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.toObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/** Tests for {@link DynamicTableSinkSpec} serialization and deserialization. */
@Execution(CONCURRENT)
class DynamicTableSinkSpecSerdeTest {

    @ParameterizedTest
    @ValueSource(strings = {"0", "2", "-1", "1.5", "\"1\"", "null"})
    void testUnknownOptionalLineageVersionDoesNotPreventRestore(String version) throws Exception {
        final PlannerMocks mocks = PlannerMocks.create();
        final SerdeContext context =
                configuredSerdeContext(mocks.getCatalogManager(), mocks.getTableConfig());
        final DynamicTableSinkSpec spec = testDynamicTableSinkSpecSerde().findFirst().get();
        final String sinkKey =
                spec.getContextResolvedTable().getIdentifier().asSerializableString();
        mocks.getCatalogManager()
                .createTemporaryTable(
                        spec.getContextResolvedTable().getResolvedTable(),
                        spec.getContextResolvedTable().getIdentifier(),
                        false);
        spec.setColumnLineage(
                new PlannerSinkColumnLineage(
                        sinkKey,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()));
        spec.setTableLineage(
                new PlannerSinkTableLineage(
                        sinkKey, Collections.emptyList(), Collections.emptyList()));
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode json = (ObjectNode) mapper.readTree(toJson(context, spec));
        for (String field : Arrays.asList("columnLineage", "tableLineage")) {
            ((ObjectNode) json.get(field)).set("formatVersion", mapper.readTree(version));
        }
        final DynamicTableSinkSpec restored =
                toObject(context, json.toString(), DynamicTableSinkSpec.class);
        assertThat(restored.getContextResolvedTable()).isEqualTo(spec.getContextResolvedTable());
        assertThat(restored.getColumnLineage()).isNull();
        assertThat(restored.getTableLineage()).isNull();
        assertThat(restored.getTableSink(mocks.getPlannerContext().getFlinkContext())).isNotNull();
    }

    @Test
    void testVersionedAndLegacyOptionalLineageRoundTrip() throws Exception {
        final PlannerMocks mocks = PlannerMocks.create();
        final SerdeContext context =
                configuredSerdeContext(mocks.getCatalogManager(), mocks.getTableConfig());
        final DynamicTableSinkSpec spec = testDynamicTableSinkSpecSerde().findFirst().get();
        final String sinkKey =
                spec.getContextResolvedTable().getIdentifier().asSerializableString();
        mocks.getCatalogManager()
                .createTemporaryTable(
                        spec.getContextResolvedTable().getResolvedTable(),
                        spec.getContextResolvedTable().getIdentifier(),
                        false);
        spec.setColumnLineage(
                new PlannerSinkColumnLineage(
                        sinkKey,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()));
        spec.setTableLineage(
                new PlannerSinkTableLineage(
                        sinkKey, Collections.emptyList(), Collections.emptyList()));
        final ObjectNode json = (ObjectNode) new ObjectMapper().readTree(toJson(context, spec));
        assertThat(json.path("columnLineage").path("formatVersion").asInt()).isEqualTo(1);
        assertThat(json.path("tableLineage").path("formatVersion").asInt()).isEqualTo(1);
        for (boolean legacy : Arrays.asList(false, true)) {
            if (legacy) {
                ((ObjectNode) json.get("columnLineage")).remove("formatVersion");
                ((ObjectNode) json.get("tableLineage")).remove("formatVersion");
            }
            final DynamicTableSinkSpec restored =
                    toObject(context, json.toString(), DynamicTableSinkSpec.class);
            assertThat(restored.getColumnLineage()).isEqualTo(spec.getColumnLineage());
            assertThat(restored.getTableLineage().getSinkKey()).isEqualTo(sinkKey);
            assertThat(restored.getTableLineage().getExpectedSources()).isEmpty();
        }
    }

    @Test
    void testOptionalLineageDoesNotChangeExecutionIdentity() {
        final DynamicTableSinkSpec spec = testDynamicTableSinkSpecSerde().findFirst().get();
        final DynamicTableSinkSpec sameExecution =
                new DynamicTableSinkSpec(spec.getContextResolvedTable(), null, null);
        final java.util.Set<DynamicTableSinkSpec> cached = new java.util.HashSet<>();
        cached.add(spec);
        spec.setColumnLineage(
                new PlannerSinkColumnLineage(
                        spec.getContextResolvedTable().getIdentifier().asSerializableString(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList()));

        spec.setTableLineage(
                new PlannerSinkTableLineage(
                        spec.getContextResolvedTable().getIdentifier().asSerializableString(),
                        Collections.emptyList(),
                        Collections.emptyList()));

        assertThat(spec).isEqualTo(sameExecution);
        assertThat(spec.hashCode()).isEqualTo(sameExecution.hashCode());
        assertThat(cached).contains(spec, sameExecution);
        assertThat(spec.toString()).doesNotContain("PlannerSinkColumnLineage@");
    }

    static Stream<DynamicTableSinkSpec> testDynamicTableSinkSpecSerde() {
        Map<String, String> options1 = new HashMap<>();
        options1.put("connector", FileSystemTableFactory.IDENTIFIER);
        options1.put("format", TestCsvFormatFactory.IDENTIFIER);
        options1.put("path", "/tmp");

        final ResolvedSchema resolvedSchema1 =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("a", DataTypes.BIGINT())),
                        Collections.emptyList(),
                        null,
                        Collections.singletonList(
                                DefaultIndex.newIndex("idx", Collections.singletonList("a"))),
                        null);
        final CatalogTable catalogTable1 =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema1).build())
                        .options(options1)
                        .build();

        DynamicTableSinkSpec spec1 =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        CatalogManagerMocks.DEFAULT_CATALOG,
                                        CatalogManagerMocks.DEFAULT_DATABASE,
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable1, resolvedSchema1)),
                        null,
                        null);

        Map<String, String> options2 = new HashMap<>();
        options2.put("connector", FileSystemTableFactory.IDENTIFIER);
        options2.put("format", TestCsvFormatFactory.IDENTIFIER);
        options2.put("path", "/tmp");

        final ResolvedSchema resolvedSchema2 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.physical("p", DataTypes.STRING())),
                        Collections.emptyList(),
                        null,
                        Collections.singletonList(
                                DefaultIndex.newIndex("idx", Collections.singletonList("a"))),
                        null);
        final CatalogTable catalogTable2 =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema2).build())
                        .options(options2)
                        .build();

        DynamicTableSinkSpec spec2 =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        CatalogManagerMocks.DEFAULT_CATALOG,
                                        CatalogManagerMocks.DEFAULT_DATABASE,
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable2, resolvedSchema2)),
                        Arrays.asList(
                                new OverwriteSpec(true),
                                new PartitioningSpec(
                                        new HashMap<String, String>() {
                                            {
                                                put("p", "A");
                                            }
                                        })),
                        new int[][] {{0}, {1}});

        Map<String, String> options3 = new HashMap<>();
        options3.put("connector", TestValuesTableFactory.IDENTIFIER);
        options3.put("writable-metadata", "m:STRING");

        final ResolvedSchema resolvedSchema3 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.metadata("m", DataTypes.STRING(), null, false)),
                        Collections.emptyList(),
                        null,
                        Collections.singletonList(
                                DefaultIndex.newIndex("idx", Collections.singletonList("a"))),
                        null);
        final CatalogTable catalogTable3 =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema3).build())
                        .options(options3)
                        .build();

        DynamicTableSinkSpec spec3 =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        CatalogManagerMocks.DEFAULT_CATALOG,
                                        CatalogManagerMocks.DEFAULT_DATABASE,
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable3, resolvedSchema3)),
                        Collections.singletonList(
                                new WritingMetadataSpec(
                                        Collections.singletonList("m"),
                                        RowType.of(new BigIntType(), new IntType()))),
                        null);

        Map<String, String> options4 = new HashMap<>();
        options4.put("connector", TestValuesTableFactory.IDENTIFIER);
        int[][] targetColumnIndices = new int[][] {{0}, {1}};

        // Todo: add test cases for nested columns in schema after FLINK-31301 is fixed.
        final ResolvedSchema resolvedSchema4 =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("a", DataTypes.BIGINT()),
                                Column.physical("b", DataTypes.INT()),
                                Column.metadata("p", DataTypes.STRING(), null, false)),
                        Collections.emptyList(),
                        null,
                        Collections.singletonList(
                                DefaultIndex.newIndex("idx", Collections.singletonList("a"))),
                        null);
        final CatalogTable catalogTable4 =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema4).build())
                        .options(options4)
                        .build();

        DynamicTableSinkSpec spec4 =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.temporary(
                                ObjectIdentifier.of(
                                        CatalogManagerMocks.DEFAULT_CATALOG,
                                        CatalogManagerMocks.DEFAULT_DATABASE,
                                        "MyTable"),
                                new ResolvedCatalogTable(catalogTable4, resolvedSchema4)),
                        Collections.singletonList(new TargetColumnWritingSpec(targetColumnIndices)),
                        targetColumnIndices);

        return Stream.of(spec1, spec2, spec3, spec4);
    }

    @ParameterizedTest
    @MethodSource("testDynamicTableSinkSpecSerde")
    void testDynamicTableSinkSpecSerde(DynamicTableSinkSpec spec) throws IOException {
        PlannerMocks plannerMocks = PlannerMocks.create();

        CatalogManager catalogManager = plannerMocks.getCatalogManager();
        catalogManager.createTable(
                spec.getContextResolvedTable().getResolvedTable(),
                spec.getContextResolvedTable().getIdentifier(),
                false);

        SerdeContext serdeCtx =
                configuredSerdeContext(catalogManager, plannerMocks.getTableConfig());

        // Re-init the spec to be permanent with correct catalog
        spec =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.permanent(
                                spec.getContextResolvedTable().getIdentifier(),
                                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get(),
                                spec.getContextResolvedTable().getResolvedTable()),
                        spec.getSinkAbilities(),
                        null);

        String actualJson = toJson(serdeCtx, spec);
        DynamicTableSinkSpec actual = toObject(serdeCtx, actualJson, DynamicTableSinkSpec.class);

        assertThat(actual.getContextResolvedTable()).isEqualTo(spec.getContextResolvedTable());
        assertThat(actual.getSinkAbilities()).isEqualTo(spec.getSinkAbilities());

        assertThat(actual.getTableSink(plannerMocks.getPlannerContext().getFlinkContext()))
                .isNotNull();
    }

    @Test
    void testDynamicTableSinkSpecSerdeWithEnrichmentOptions() throws Exception {
        // Test model
        ObjectIdentifier identifier =
                ObjectIdentifier.of(
                        CatalogManagerMocks.DEFAULT_CATALOG,
                        CatalogManagerMocks.DEFAULT_DATABASE,
                        "my_table");

        String formatPrefix = FactoryUtil.getFormatPrefix(FORMAT, TestFormatFactory.IDENTIFIER);

        Map<String, String> planOptions = new HashMap<>();
        planOptions.put(CONNECTOR.key(), TestDynamicTableFactory.IDENTIFIER);
        planOptions.put(TARGET.key(), "abc");
        planOptions.put(BUFFER_SIZE.key(), "1000");
        planOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        planOptions.put(formatPrefix + DELIMITER.key(), "|");

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put(CONNECTOR.key(), TestDynamicTableFactory.IDENTIFIER);
        catalogOptions.put(TARGET.key(), "xyz");
        catalogOptions.put(BUFFER_SIZE.key(), "2000");
        catalogOptions.put(FORMAT.key(), TestFormatFactory.IDENTIFIER);
        catalogOptions.put(formatPrefix + DELIMITER.key(), ",");

        ResolvedCatalogTable planResolvedCatalogTable = tableWithOnlyPhysicalColumns(planOptions);
        ResolvedCatalogTable catalogResolvedCatalogTable =
                tableWithOnlyPhysicalColumns(catalogOptions);

        // Create planner mocks
        PlannerMocks plannerMocks =
                PlannerMocks.create(
                        new Configuration()
                                .set(PLAN_RESTORE_CATALOG_OBJECTS, CatalogPlanRestore.ALL)
                                .set(PLAN_COMPILE_CATALOG_OBJECTS, CatalogPlanCompilation.ALL));

        CatalogManager catalogManager = plannerMocks.getCatalogManager();
        catalogManager.createTable(catalogResolvedCatalogTable, identifier, false);

        // Mock the context
        SerdeContext serdeCtx =
                configuredSerdeContext(catalogManager, plannerMocks.getTableConfig());

        DynamicTableSinkSpec planSpec =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.permanent(
                                identifier,
                                catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get(),
                                planResolvedCatalogTable),
                        Collections.emptyList(),
                        null);

        String actualJson = toJson(serdeCtx, planSpec);
        DynamicTableSinkSpec actual = toObject(serdeCtx, actualJson, DynamicTableSinkSpec.class);

        assertThat(actual.getContextResolvedTable()).isEqualTo(planSpec.getContextResolvedTable());
        assertThat(actual.getSinkAbilities()).isNull();

        TestDynamicTableFactory.DynamicTableSinkMock dynamicTableSink =
                (TestDynamicTableFactory.DynamicTableSinkMock)
                        actual.getTableSink(plannerMocks.getPlannerContext().getFlinkContext());

        assertThat(dynamicTableSink.target).isEqualTo("abc");
        assertThat(dynamicTableSink.bufferSize).isEqualTo(2000);
        assertThat(((TestFormatFactory.EncodingFormatMock) dynamicTableSink.valueFormat).delimiter)
                .isEqualTo(",");
    }

    @Test
    void testColumnLineageJsonAndSmileRoundTrip() throws Exception {
        final ObjectIdentifier sinkIdentifier =
                ObjectIdentifier.of(
                        CatalogManagerMocks.DEFAULT_CATALOG,
                        CatalogManagerMocks.DEFAULT_DATABASE,
                        "LineageSink");
        final ResolvedSchema sinkSchema =
                ResolvedSchema.of(Column.physical("result", DataTypes.BIGINT()));
        final CatalogTable sinkTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(sinkSchema).build())
                        .options(
                                Collections.singletonMap(
                                        "connector", TestValuesTableFactory.IDENTIFIER))
                        .build();

        final PlannerMocks plannerMocks = PlannerMocks.create();
        final CatalogManager catalogManager = plannerMocks.getCatalogManager();
        catalogManager.createTable(
                new ResolvedCatalogTable(sinkTable, sinkSchema), sinkIdentifier, false);
        final SerdeContext serdeContext =
                configuredSerdeContext(catalogManager, plannerMocks.getTableConfig());

        final PlannerLineageDataset sourceDataset =
                new PlannerLineageDataset(
                        Arrays.asList(
                                CatalogManagerMocks.DEFAULT_CATALOG,
                                CatalogManagerMocks.DEFAULT_DATABASE,
                                "LineageSource"));
        final PlannerSinkColumnLineage columnLineage =
                new PlannerSinkColumnLineage(
                        sinkIdentifier.asSerializableString(),
                        Collections.singletonList("result"),
                        Collections.singletonList(sourceDataset),
                        Collections.singletonList(
                                new PlannerColumnLineageRelation(
                                        "result",
                                        Collections.singletonList(
                                                new PlannerColumnLineageInput(
                                                        sourceDataset,
                                                        "value",
                                                        PlannerColumnLineageDependencyType.DIRECT)),
                                        PlannerColumnLineageOrigin.INPUT_FIELDS,
                                        Collections.emptyList())));
        final DynamicTableSinkSpec spec =
                new DynamicTableSinkSpec(
                        ContextResolvedTable.permanent(
                                sinkIdentifier,
                                catalogManager
                                        .getCatalog(catalogManager.getCurrentCatalog())
                                        .orElseThrow(AssertionError::new),
                                new ResolvedCatalogTable(sinkTable, sinkSchema)),
                        Collections.emptyList(),
                        null,
                        columnLineage);

        final String json =
                CompiledPlanSerdeUtil.createJsonObjectWriter(serdeContext).writeValueAsString(spec);
        final DynamicTableSinkSpec jsonRoundTrip =
                CompiledPlanSerdeUtil.createJsonObjectReader(serdeContext)
                        .readValue(json, DynamicTableSinkSpec.class);
        final byte[] smile =
                CompiledPlanSerdeUtil.createSmileObjectWriter(serdeContext).writeValueAsBytes(spec);
        final DynamicTableSinkSpec smileRoundTrip =
                CompiledPlanSerdeUtil.createSmileObjectReader(serdeContext)
                        .readValue(smile, DynamicTableSinkSpec.class);

        assertThat(jsonRoundTrip.getColumnLineage()).isEqualTo(columnLineage);
        assertThat(jsonRoundTrip.getColumnLineage().getExpectedSources())
                .containsExactly(sourceDataset);
        assertThat(smileRoundTrip.getColumnLineage()).isEqualTo(columnLineage);
        assertThat(smileRoundTrip.getColumnLineage().getExpectedSources())
                .containsExactly(sourceDataset);

        final JsonNode missingExpectedSources = new ObjectMapper().readTree(json);
        ((ObjectNode) missingExpectedSources.get("columnLineage")).remove("expectedSources");
        final DynamicTableSinkSpec withoutOptionalLineage =
                CompiledPlanSerdeUtil.createJsonObjectReader(serdeContext)
                        .readValue(missingExpectedSources.toString(), DynamicTableSinkSpec.class);
        assertThat(withoutOptionalLineage.getColumnLineage()).isNull();
        assertThat(withoutOptionalLineage.getContextResolvedTable())
                .isEqualTo(spec.getContextResolvedTable());

        final PlannerSinkColumnLineage constantLineage =
                new PlannerSinkColumnLineage(
                        sinkIdentifier.asSerializableString(),
                        Collections.singletonList("result"),
                        Collections.emptyList(),
                        Collections.singletonList(
                                new PlannerColumnLineageRelation(
                                        "result",
                                        Collections.emptyList(),
                                        PlannerColumnLineageOrigin.CONSTANT,
                                        Collections.emptyList())));
        spec.setColumnLineage(constantLineage);
        final String constantJson =
                CompiledPlanSerdeUtil.createJsonObjectWriter(serdeContext).writeValueAsString(spec);
        final DynamicTableSinkSpec constantJsonRoundTrip =
                CompiledPlanSerdeUtil.createJsonObjectReader(serdeContext)
                        .readValue(constantJson, DynamicTableSinkSpec.class);
        final byte[] constantSmile =
                CompiledPlanSerdeUtil.createSmileObjectWriter(serdeContext).writeValueAsBytes(spec);
        final DynamicTableSinkSpec constantSmileRoundTrip =
                CompiledPlanSerdeUtil.createSmileObjectReader(serdeContext)
                        .readValue(constantSmile, DynamicTableSinkSpec.class);

        assertThat(constantJsonRoundTrip.getColumnLineage()).isEqualTo(constantLineage);
        assertThat(constantJsonRoundTrip.getColumnLineage().getExpectedSources()).isEmpty();
        assertThat(constantSmileRoundTrip.getColumnLineage()).isEqualTo(constantLineage);
        assertThat(constantSmileRoundTrip.getColumnLineage().getExpectedSources()).isEmpty();
    }
}

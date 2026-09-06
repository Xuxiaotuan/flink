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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.lineage.ColumnLineageDependencyType;
import org.apache.flink.streaming.api.lineage.ColumnLineageInput;
import org.apache.flink.streaming.api.lineage.ColumnLineageOrigin;
import org.apache.flink.streaming.api.lineage.ColumnLineageRelation;
import org.apache.flink.streaming.api.lineage.DefaultColumnLineageInput;
import org.apache.flink.streaming.api.lineage.DefaultColumnLineageRelation;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.streaming.api.lineage.TransformationColumnLineage;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TransformationWithLineage;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.InsertConflictStrategy;
import org.apache.flink.table.api.InsertConflictStrategy.ConflictBehavior;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.TransformationSinkProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.sink.legacy.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.DynamicSinkUtils;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageInput;
import org.apache.flink.table.planner.lineage.PlannerColumnLineageRelation;
import org.apache.flink.table.planner.lineage.PlannerLineageDataset;
import org.apache.flink.table.planner.lineage.PlannerPrunedSource;
import org.apache.flink.table.planner.lineage.PlannerSinkColumnLineage;
import org.apache.flink.table.planner.lineage.TableLineageDataset;
import org.apache.flink.table.planner.lineage.TableLineageExtractionException;
import org.apache.flink.table.planner.lineage.TableLineageUtils;
import org.apache.flink.table.planner.lineage.TableSinkLineageVertex;
import org.apache.flink.table.planner.lineage.TableSinkLineageVertexImpl;
import org.apache.flink.table.planner.lineage.TableSourceLineageVertexImpl;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelDeleteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelUpdateSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.RowKindSetter;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcerExecutor;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base {@link ExecNode} to write data to an external sink defined by a {@link DynamicTableSink}.
 */
public abstract class CommonExecSink extends ExecNodeBase<Object>
        implements MultipleTransformationTranslator<Object> {

    public static final String CONSTRAINT_VALIDATOR_TRANSFORMATION = "constraint-validator";
    public static final String PARTITIONER_TRANSFORMATION = "partitioner";
    public static final String UPSERT_MATERIALIZE_TRANSFORMATION = "upsert-materialize";
    public static final String TIMESTAMP_INSERTER_TRANSFORMATION = "timestamp-inserter";
    public static final String ROW_KIND_SETTER = "row-kind-setter";
    public static final String SINK_TRANSFORMATION = "sink";

    public static final String FIELD_NAME_DYNAMIC_TABLE_SINK = "dynamicTableSink";

    @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK)
    protected final DynamicTableSinkSpec tableSinkSpec;

    private final ChangelogMode inputChangelogMode;
    private final boolean isBounded;
    protected boolean sinkParallelismConfigured;

    protected CommonExecSink(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            boolean isBounded,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.tableSinkSpec = tableSinkSpec;
        this.inputChangelogMode = inputChangelogMode;
        this.isBounded = isBounded;
    }

    @Override
    public String getSimplifiedName() {
        return tableSinkSpec.getContextResolvedTable().getIdentifier().getObjectName();
    }

    public DynamicTableSinkSpec getTableSinkSpec() {
        return tableSinkSpec;
    }

    @SuppressWarnings("unchecked")
    protected Transformation<Object> createSinkTransformation(
            StreamExecutionEnvironment streamExecEnv,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Transformation<RowData> inputTransform,
            DynamicTableSink tableSink,
            int rowtimeFieldIndex,
            boolean upsertMaterialize,
            int[] inputUpsertKey,
            @Nullable InsertConflictStrategy conflictStrategy) {
        final ResolvedSchema schema = tableSinkSpec.getContextResolvedTable().getResolvedSchema();
        final SinkRuntimeProvider runtimeProvider =
                tableSink.getSinkRuntimeProvider(
                        new SinkRuntimeProviderContext(
                                isBounded, tableSinkSpec.getTargetColumns()));
        final RowType persistedRowType = getPersistedRowType(schema, tableSink);
        final boolean requiresColumnLineage = requiresColumnLineage(tableSink);

        final int[] primaryKeys = getPrimaryKeyIndices(persistedRowType, schema);
        final int sinkParallelism = deriveSinkParallelism(inputTransform, runtimeProvider);
        sinkParallelismConfigured = isParallelismConfigured(runtimeProvider);
        final int inputParallelism = inputTransform.getParallelism();
        final boolean inputInsertOnly = inputChangelogMode.containsOnly(RowKind.INSERT);
        final boolean hasPk = primaryKeys.length > 0;

        if (!inputInsertOnly && sinkParallelism != inputParallelism && !hasPk) {
            throw new TableException(
                    String.format(
                            "The sink for table '%s' has a configured parallelism of %s, while the input parallelism is %s. "
                                    + "Since the configured parallelism is different from the input's parallelism and "
                                    + "the changelog mode is not insert-only, a primary key is required but could not "
                                    + "be found.",
                            tableSinkSpec
                                    .getContextResolvedTable()
                                    .getIdentifier()
                                    .asSummaryString(),
                            sinkParallelism,
                            inputParallelism));
        }

        Object outputObject = null;
        if (runtimeProvider instanceof OutputFormatProvider) {
            outputObject = ((OutputFormatProvider) runtimeProvider).createOutputFormat();
        } else if (runtimeProvider instanceof SinkFunctionProvider) {
            outputObject = ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
        } else if (runtimeProvider instanceof SinkV2Provider) {
            outputObject = ((SinkV2Provider) runtimeProvider).createSink();
        }

        // only add materialization if input has changes, unless the conflict strategy has to
        // compare every insert against the row stored under the same primary key
        final boolean needMaterialization =
                upsertMaterialize && (!inputInsertOnly || detectsDuplicateKeys(conflictStrategy));

        Transformation<RowData> sinkTransform =
                applyConstraintValidations(
                        inputTransform,
                        config,
                        persistedRowType,
                        primaryKeys,
                        inputChangelogMode.keyOnlyDeletes());

        if (hasPk) {
            sinkTransform =
                    applyKeyBy(
                            config,
                            classLoader,
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            inputParallelism,
                            needMaterialization);
        }

        if (needMaterialization) {
            sinkTransform =
                    applyUpsertMaterialize(
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            config,
                            classLoader,
                            persistedRowType,
                            inputUpsertKey);
        }

        Optional<RowKind> targetRowKind = getTargetRowKind();
        if (targetRowKind.isPresent()) {
            sinkTransform = applyRowKindSetter(sinkTransform, targetRowKind.get(), config);
        }

        Transformation transformation =
                (Transformation<Object>)
                        applySinkProvider(
                                sinkTransform,
                                streamExecEnv,
                                runtimeProvider,
                                rowtimeFieldIndex,
                                sinkParallelism,
                                config,
                                classLoader);

        try {
            Optional<LineageVertex> lineageVertexOpt =
                    TableLineageUtils.extractLineageDataset(outputObject);
            LineageDataset tableLineageDataset =
                    TableLineageUtils.createTableLineageDataset(
                            tableSinkSpec.getContextResolvedTable(), lineageVertexOpt);

            TableSinkLineageVertex sinkLineageVertex =
                    new TableSinkLineageVertexImpl(
                            Arrays.asList(tableLineageDataset),
                            TableLineageUtils.convert(inputChangelogMode));

            if (transformation instanceof TransformationWithLineage) {
                final TransformationWithLineage<Object> lineageTransformation =
                        (TransformationWithLineage<Object>) transformation;
                lineageTransformation.setLineageVertex(sinkLineageVertex);
                try {
                    lineageTransformation.setTableLineage(createTableLineage(inputTransform));
                } catch (RuntimeException error) {
                    org.slf4j.LoggerFactory.getLogger(CommonExecSink.class)
                            .warn(
                                    "Logical table lineage unavailable for sink {}; execution continues.",
                                    sinkIdentity(),
                                    error);
                }
                if (requiresColumnLineage) {
                    lineageTransformation.setColumnLineage(
                            createColumnLineage(inputTransform, tableSink, tableLineageDataset));
                }
            } else if (requiresColumnLineage) {
                throw lineageFailure(
                        "<unknown>",
                        "sink runtime provider '"
                                + runtimeProvider.getClass().getName()
                                + "' cannot carry table column lineage");
            }

        } catch (RuntimeException error) {
            transformation.setLineageFailure(
                    error.getClass().getSimpleName() + ": " + error.getMessage());
            org.slf4j.LoggerFactory.getLogger(CommonExecSink.class)
                    .warn(
                            "Column lineage unavailable for sink {}; job execution continues.",
                            tableSinkSpec
                                    .getContextResolvedTable()
                                    .getIdentifier()
                                    .asSummaryString(),
                            error);
        }
        return transformation;
    }

    private boolean requiresColumnLineage(DynamicTableSink tableSink) {
        return !tableSinkSpec.getContextResolvedTable().isAnonymous()
                || !DynamicSinkUtils.isInternalSinkWithoutLineage(tableSink);
    }

    private org.apache.flink.streaming.api.lineage.TransformationTableLineage createTableLineage(
            Transformation<RowData> inputTransform) {
        org.apache.flink.table.planner.lineage.PlannerSinkTableLineage table =
                tableSinkSpec.getTableLineage();
        if (table == null) {
            return null;
        }
        if (!sinkIdentity().equals(table.getSinkKey())) {
            throw lineageFailure("<unknown>", "logical table sink identity mismatch");
        }
        Map<String, LineageDataset> actual = collectSourceDatasets(inputTransform);
        Set<String> expected =
                table.getExpectedSources().stream()
                        .map(CommonExecSink::datasetKey)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, SourceLineageVertex> vertices = new LinkedHashMap<>();
        for (Transformation<?> predecessor : inputTransform.getTransitivePredecessors()) {
            if (predecessor instanceof TransformationWithLineage) {
                LineageVertex vertex =
                        ((TransformationWithLineage<?>) predecessor).getLineageVertex();
                if (vertex instanceof SourceLineageVertex) {
                    for (LineageDataset dataset : vertex.datasets()) {
                        vertices.put(
                                dataset.name(),
                                new TableSourceLineageVertexImpl(
                                        Collections.singletonList(dataset),
                                        ((SourceLineageVertex) vertex).boundedness()));
                    }
                }
            }
        }
        for (PlannerPrunedSource pruned : table.getPrunedSources()) {
            if (!expected.contains(pruned.name())
                    || actual.putIfAbsent(pruned.name(), pruned) != null) {
                throw lineageFailure("<unknown>", "invalid pruned logical table source");
            }
            vertices.put(
                    pruned.name(),
                    new TableSourceLineageVertexImpl(
                            Collections.singletonList(pruned),
                            org.apache.flink.api.connector.source.Boundedness.BOUNDED));
        }
        if (!actual.keySet().equals(expected) || !vertices.keySet().containsAll(expected)) {
            throw lineageFailure(
                    "<unknown>", "runtime sources do not match verified logical table sources");
        }
        List<SourceLineageVertex> sources = new ArrayList<>();
        for (String key : expected) {
            sources.add(vertices.get(key));
        }
        return new org.apache.flink.streaming.api.lineage.TransformationTableLineage(sources);
    }

    private TransformationColumnLineage createColumnLineage(
            Transformation<RowData> inputTransform,
            DynamicTableSink tableSink,
            LineageDataset sinkDataset) {
        final PlannerSinkColumnLineage plannerLineage = tableSinkSpec.getColumnLineage();
        if (plannerLineage == null) {
            throw lineageFailure(
                    "<unknown>", "compiled plan does not contain complete column lineage");
        }
        final String actualSinkKey = sinkIdentity();
        if (!actualSinkKey.equals(plannerLineage.getSinkKey())) {
            throw lineageFailure(
                    "<unknown>",
                    "compiled column lineage sink key '"
                            + plannerLineage.getSinkKey()
                            + "' does not match actual sink key '"
                            + actualSinkKey
                            + "'");
        }
        final List<String> actualOutputFields =
                DynamicSinkUtils.createConsumedType(
                                tableSinkSpec.getContextResolvedTable().getResolvedSchema(),
                                tableSink)
                        .getFieldNames();
        if (!actualOutputFields.equals(plannerLineage.getExpectedOutputFields())) {
            throw lineageFailure(
                    "<unknown>",
                    "compiled column lineage expected output fields "
                            + plannerLineage.getExpectedOutputFields()
                            + " do not match actual consumed fields "
                            + actualOutputFields);
        }

        final Map<String, LineageDataset> sourceDatasets = collectSourceDatasets(inputTransform);
        if (plannerLineage.getPrunedSources() == null) {
            throw lineageFailure(
                    "<unknown>",
                    "compiled plan has no optimizer pruning evidence; recompile the plan");
        }
        final Map<String, PlannerPrunedSource> prunedSources = new LinkedHashMap<>();
        for (PlannerPrunedSource pruned : plannerLineage.getPrunedSources()) {
            if (pruned == null
                    || !plannerLineage.getExpectedSources().contains(pruned.getDataset())
                    || prunedSources.putIfAbsent(pruned.name(), pruned) != null
                    || sourceDatasets.containsKey(pruned.name())) {
                throw lineageFailure(
                        "<unknown>",
                        "invalid or conflicting pruned source identities; recompile the plan");
            }
        }
        final Set<String> expectedSourceKeys =
                plannerLineage.getExpectedSources().stream()
                        .map(CommonExecSink::datasetKey)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        expectedSourceKeys.removeAll(prunedSources.keySet());
        final Set<String> actualSourceKeys = new LinkedHashSet<>(sourceDatasets.keySet());
        if (!actualSourceKeys.equals(expectedSourceKeys)) {
            final Set<String> missingSources = new LinkedHashSet<>(expectedSourceKeys);
            missingSources.removeAll(actualSourceKeys);
            final Set<String> unexpectedSources = new LinkedHashSet<>(actualSourceKeys);
            unexpectedSources.removeAll(expectedSourceKeys);
            throw lineageFailure(
                    "<unknown>",
                    "actual source identities do not match compiled expected source identities; "
                            + "missing="
                            + missingSources
                            + ", unexpected="
                            + unexpectedSources);
        }
        // Only optimizer-certified snapshots may fill logical dependencies absent at runtime.
        sourceDatasets.putAll(prunedSources);
        final List<ColumnLineageRelation> relations = new ArrayList<>();
        for (PlannerColumnLineageRelation plannerRelation : plannerLineage.getRelations()) {
            final List<ColumnLineageInput> inputs = new ArrayList<>();
            for (PlannerColumnLineageInput plannerInput : plannerRelation.getInputs()) {
                final LineageDataset inputDataset =
                        sourceDatasets.get(datasetKey(plannerInput.getDataset()));
                if (inputDataset == null) {
                    throw lineageFailure(
                            plannerRelation.getOutputField(),
                            "input dataset '"
                                    + plannerInput.getDataset().asSerializableString()
                                    + "' cannot be mapped to a source transformation");
                }
                if (!(inputDataset instanceof TableLineageDataset)) {
                    throw lineageFailure(
                            plannerRelation.getOutputField(),
                            "input field '"
                                    + plannerInput.getFieldName()
                                    + "' cannot be verified because actual source schema for '"
                                    + plannerInput.getDataset().asSerializableString()
                                    + "' is unavailable");
                }
                final List<String> actualSourceFields =
                        ((TableLineageDataset) inputDataset).fieldNames();
                if (actualSourceFields == null
                        || !actualSourceFields.contains(plannerInput.getFieldName())) {
                    throw lineageFailure(
                            plannerRelation.getOutputField(),
                            "input field '"
                                    + plannerInput.getFieldName()
                                    + "' does not exist in actual source schema for '"
                                    + plannerInput.getDataset().asSerializableString()
                                    + "'");
                }
                inputs.add(
                        new DefaultColumnLineageInput(
                                inputDataset,
                                plannerInput.getFieldName(),
                                ColumnLineageDependencyType.valueOf(
                                        plannerInput.getDependencyType().name())));
            }
            final String transformation =
                    plannerRelation.getTransformations().isEmpty()
                            ? null
                            : plannerRelation.getTransformations().stream()
                                    .map(Enum::name)
                                    .collect(Collectors.joining(","));
            relations.add(
                    new DefaultColumnLineageRelation(
                            sinkDataset,
                            plannerRelation.getOutputField(),
                            inputs,
                            ColumnLineageOrigin.valueOf(plannerRelation.getOrigin().name()),
                            transformation));
        }
        final List<SourceLineageVertex> prunedVertices = new ArrayList<>();
        for (PlannerPrunedSource pruned : prunedSources.values()) {
            prunedVertices.add(
                    new TableSourceLineageVertexImpl(
                            Collections.singletonList(pruned),
                            // A removed scan contributes no unbounded runtime input.
                            org.apache.flink.api.connector.source.Boundedness.BOUNDED));
        }
        return new TransformationColumnLineage(
                plannerLineage.getExpectedOutputFields(), relations, prunedVertices);
    }

    private Map<String, LineageDataset> collectSourceDatasets(
            Transformation<RowData> inputTransform) {
        final Map<String, LineageDataset> sourceDatasets = new LinkedHashMap<>();
        final Deque<Transformation<?>> pending = new ArrayDeque<>();
        final Set<Transformation<?>> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(inputTransform);
        while (!pending.isEmpty()) {
            final Transformation<?> transformation = pending.removeFirst();
            if (!visited.add(transformation)) {
                continue;
            }
            if (transformation instanceof TransformationWithLineage) {
                final LineageVertex lineageVertex =
                        ((TransformationWithLineage<?>) transformation).getLineageVertex();
                if (lineageVertex instanceof SourceLineageVertex) {
                    if (lineageVertex.datasets() == null || lineageVertex.datasets().isEmpty()) {
                        throw lineageFailure(
                                "<unknown>", "source transformation has no dataset identity");
                    }
                    for (LineageDataset dataset : lineageVertex.datasets()) {
                        if (dataset == null
                                || dataset.name() == null
                                || dataset.name().trim().isEmpty()
                                || dataset.namespace() == null
                                || dataset.namespace().trim().isEmpty()) {
                            throw lineageFailure(
                                    "<unknown>",
                                    "source transformation has an incomplete dataset identity");
                        }
                        final String key = dataset.name();
                        final LineageDataset previous = sourceDatasets.putIfAbsent(key, dataset);
                        if (previous != null
                                && (!previous.name().equals(dataset.name())
                                        || !previous.namespace().equals(dataset.namespace()))) {
                            throw lineageFailure(
                                    "<unknown>",
                                    "source dataset identity '" + key + "' is ambiguous");
                        }
                    }
                    continue;
                }
            }
            pending.addAll(transformation.getInputs());
        }
        return sourceDatasets;
    }

    private static String datasetKey(PlannerLineageDataset dataset) {
        return dataset.asSerializableString();
    }

    private TableLineageExtractionException lineageFailure(String field, String reason) {
        return new TableLineageExtractionException(
                "Cannot extract complete column lineage for sink '"
                        + sinkIdentity()
                        + "', field '"
                        + field
                        + "': "
                        + reason
                        + ".");
    }

    private String sinkIdentity() {
        return tableSinkSpec.getContextResolvedTable().isAnonymous()
                ? tableSinkSpec.getContextResolvedTable().getIdentifier().asSummaryString()
                : tableSinkSpec.getContextResolvedTable().getIdentifier().asSerializableString();
    }

    /** Whether the conflict strategy has to detect rows that share a primary key. */
    protected static boolean detectsDuplicateKeys(
            @Nullable InsertConflictStrategy conflictStrategy) {
        return conflictStrategy != null
                && (conflictStrategy.getBehavior() == ConflictBehavior.ERROR
                        || conflictStrategy.getBehavior() == ConflictBehavior.NOTHING);
    }

    /**
     * Apply an operator to filter or report error to process not-null values for not-null fields.
     */
    private Transformation<RowData> applyConstraintValidations(
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            RowType physicalRowType,
            int[] primaryKeys,
            boolean keyOnlyDeletes) {
        final Optional<ConstraintEnforcerExecutor> enforcerExecutor =
                ConstraintEnforcerExecutor.create(
                        physicalRowType,
                        primaryKeys,
                        keyOnlyDeletes,
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER),
                        config.get(
                                ExecutionConfigOptions.TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER));

        return enforcerExecutor
                .map(
                        executor -> {
                            final String operatorName =
                                    "ConstraintEnforcer["
                                            + Arrays.stream(executor.getConstraints())
                                                    .map(Objects::toString)
                                                    .collect(Collectors.joining(", "))
                                            + "]";

                            return (Transformation<RowData>)
                                    ExecNodeUtil.createOneInputTransformation(
                                            inputTransform,
                                            createTransformationMeta(
                                                    CONSTRAINT_VALIDATOR_TRANSFORMATION,
                                                    operatorName,
                                                    "ConstraintEnforcer",
                                                    config),
                                            new ConstraintEnforcer(executor, operatorName),
                                            getInputTypeInfo(),
                                            inputTransform.getParallelism(),
                                            false);
                        })
                .orElse(inputTransform);
    }

    /**
     * Returns the parallelism of sink operator, it assumes the sink runtime provider implements
     * {@link ParallelismProvider}. It returns parallelism defined in {@link ParallelismProvider} if
     * the parallelism is provided, otherwise it uses parallelism of input transformation.
     */
    private int deriveSinkParallelism(
            Transformation<RowData> inputTransform, SinkRuntimeProvider runtimeProvider) {
        final int inputParallelism = inputTransform.getParallelism();
        if (isParallelismConfigured(runtimeProvider)) {
            int sinkParallelism = ((ParallelismProvider) runtimeProvider).getParallelism().get();
            if (sinkParallelism <= 0) {
                throw new TableException(
                        String.format(
                                "Invalid configured parallelism %s for table '%s'.",
                                sinkParallelism,
                                tableSinkSpec
                                        .getContextResolvedTable()
                                        .getIdentifier()
                                        .asSummaryString()));
            }
            return sinkParallelism;
        } else {
            return inputParallelism;
        }
    }

    private boolean isParallelismConfigured(DynamicTableSink.SinkRuntimeProvider runtimeProvider) {
        return runtimeProvider instanceof ParallelismProvider
                && ((ParallelismProvider) runtimeProvider).getParallelism().isPresent();
    }

    /**
     * Apply a primary key partition transformation to guarantee the strict ordering of changelog
     * messages.
     */
    private Transformation<RowData> applyKeyBy(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            int inputParallelism,
            boolean needMaterialize) {
        final ExecutionConfigOptions.SinkKeyedShuffle sinkShuffleByPk =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE);
        boolean sinkKeyBy = false;
        switch (sinkShuffleByPk) {
            case NONE:
                break;
            case AUTO:
                // should cover both insert-only and changelog input
                sinkKeyBy = sinkParallelism != inputParallelism && sinkParallelism != 1;
                break;
            case FORCE:
                // sink single parallelism has no problem (because none partitioner will cause worse
                // disorder)
                sinkKeyBy = sinkParallelism != 1;
                break;
        }
        if (!sinkKeyBy && !needMaterialize) {
            return inputTransform;
        }

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(classLoader, primaryKeys, getInputTypeInfo());
        final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                new KeyGroupStreamPartitioner<>(
                        selector, KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        Transformation<RowData> partitionedTransform =
                new PartitionTransformation<>(inputTransform, partitioner);
        createTransformationMeta(PARTITIONER_TRANSFORMATION, "Partitioner", "Partitioner", config)
                .fill(partitionedTransform);
        partitionedTransform.setParallelism(sinkParallelism, sinkParallelismConfigured);
        return partitionedTransform;
    }

    protected abstract Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType physicalRowType,
            int[] inputUpsertKey);

    private Transformation<RowData> applyRowKindSetter(
            Transformation<RowData> inputTransform, RowKind rowKind, ExecNodeConfig config) {
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(
                        ROW_KIND_SETTER,
                        String.format("RowKindSetter(TargetRowKind=[%s])", rowKind),
                        "RowKindSetter",
                        config),
                new RowKindSetter(rowKind),
                inputTransform.getOutputType(),
                inputTransform.getParallelism(),
                false);
    }

    private Transformation<?> applySinkProvider(
            Transformation<RowData> inputTransform,
            StreamExecutionEnvironment env,
            SinkRuntimeProvider runtimeProvider,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            final TransformationMetadata metadata =
                    createTransformationMeta(SINK_TRANSFORMATION, config);
            if (runtimeProvider instanceof DataStreamSinkProvider) {
                Transformation<RowData> sinkTransformation =
                        applyRowtimeTransformation(
                                inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final DataStreamSinkProvider provider = (DataStreamSinkProvider) runtimeProvider;
                return provider.consumeDataStream(
                                createProviderContext(metadata, config), dataStream)
                        .getTransformation();
            } else if (runtimeProvider instanceof TransformationSinkProvider) {
                final TransformationSinkProvider provider =
                        (TransformationSinkProvider) runtimeProvider;
                final ProviderContext providerContext = createProviderContext(metadata, config);
                return provider.createTransformation(
                        new TransformationSinkProvider.Context() {
                            @Override
                            public Transformation<RowData> getInputTransformation() {
                                return inputTransform;
                            }

                            @Override
                            public int getRowtimeIndex() {
                                return rowtimeFieldIndex;
                            }

                            @Override
                            public Optional<String> generateUid(String name) {
                                return providerContext.generateUid(name);
                            }

                            @Override
                            public String getContainerNodeType() {
                                return providerContext.getContainerNodeType();
                            }

                            @Override
                            public String getName() {
                                return providerContext.getName();
                            }

                            @Override
                            public String getDescription() {
                                return providerContext.getDescription();
                            }
                        });
            } else if (runtimeProvider instanceof SinkFunctionProvider) {
                final SinkFunction<RowData> sinkFunction =
                        ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
                return createSinkFunctionTransformation(
                        sinkFunction,
                        env,
                        inputTransform,
                        rowtimeFieldIndex,
                        metadata,
                        sinkParallelism);
            } else if (runtimeProvider instanceof OutputFormatProvider) {
                OutputFormat<RowData> outputFormat =
                        ((OutputFormatProvider) runtimeProvider).createOutputFormat();
                final SinkFunction<RowData> sinkFunction =
                        new OutputFormatSinkFunction<>(outputFormat);
                return createSinkFunctionTransformation(
                        sinkFunction,
                        env,
                        inputTransform,
                        rowtimeFieldIndex,
                        metadata,
                        sinkParallelism);
            } else if (runtimeProvider instanceof SinkV2Provider) {
                SinkV2Provider sinkV2Provider = (SinkV2Provider) runtimeProvider;
                Transformation<RowData> sinkTransformation =
                        applyRowtimeTransformation(
                                inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final Transformation<RowData> transformation =
                        DataStreamSink.forSink(
                                        dataStream,
                                        sinkV2Provider.createSink(),
                                        CustomSinkOperatorUidHashes.DEFAULT)
                                .getTransformation();
                sinkV2Provider
                        .getAdditionalMetricVariables()
                        .forEach(transformation::addMetricVariable);
                transformation.setParallelism(sinkParallelism, sinkParallelismConfigured);
                metadata.fill(transformation);
                return transformation;
            } else {
                throw new TableException("Unsupported sink runtime provider.");
            }
        }
    }

    private Transformation<?> createSinkFunctionTransformation(
            SinkFunction<RowData> sinkFunction,
            StreamExecutionEnvironment env,
            Transformation<RowData> inputTransformation,
            int rowtimeFieldIndex,
            TransformationMetadata transformationMetadata,
            int sinkParallelism) {
        final SinkOperator operator = new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex);

        if (sinkFunction instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) sinkFunction)
                    .setInputType(getInputTypeInfo(), env.getConfig());
        }

        final Transformation<?> transformation =
                new LegacySinkTransformation<>(
                        inputTransformation,
                        transformationMetadata.getName(),
                        SimpleOperatorFactory.of(operator),
                        sinkParallelism,
                        sinkParallelismConfigured);
        transformationMetadata.fill(transformation);
        return transformation;
    }

    private Transformation<RowData> applyRowtimeTransformation(
            Transformation<RowData> inputTransform,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ExecNodeConfig config) {
        // Don't apply the transformation/operator if there is no rowtimeFieldIndex
        if (rowtimeFieldIndex == -1) {
            return inputTransform;
        }
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(
                        TIMESTAMP_INSERTER_TRANSFORMATION,
                        String.format(
                                "StreamRecordTimestampInserter(rowtime field: %s)",
                                rowtimeFieldIndex),
                        "StreamRecordTimestampInserter",
                        config),
                new StreamRecordTimestampInserter(rowtimeFieldIndex),
                inputTransform.getOutputType(),
                sinkParallelism,
                sinkParallelismConfigured);
    }

    private InternalTypeInfo<RowData> getInputTypeInfo() {
        return InternalTypeInfo.of(getInputEdges().get(0).getOutputType());
    }

    protected int[] getPrimaryKeyIndices(RowType sinkRowType, ResolvedSchema schema) {
        return schema.getPrimaryKey()
                .map(k -> k.getColumns().stream().mapToInt(sinkRowType::getFieldIndex).toArray())
                .orElse(new int[0]);
    }

    /**
     * The method recreates the type of the incoming record from the sink's schema. It puts the
     * physical columns first, followed by persisted metadata columns.
     */
    protected RowType getPersistedRowType(ResolvedSchema schema, DynamicTableSink sink) {
        if (legacyPhysicalTypeEnabled()) {
            return (RowType) schema.toPhysicalRowDataType().getLogicalType();
        } else {
            return DynamicSinkUtils.createConsumedType(schema, sink);
        }
    }

    /**
     * Get the target row-kind that the row data should change to, assuming the current row kind is
     * RowKind.INSERT. Return Optional.empty() if it doesn't need to change. Currently, it'll only
     * consider row-level delete/update.
     */
    private Optional<RowKind> getTargetRowKind() {
        if (tableSinkSpec.getSinkAbilities() != null) {
            for (SinkAbilitySpec sinkAbilitySpec : tableSinkSpec.getSinkAbilities()) {
                if (sinkAbilitySpec instanceof RowLevelDeleteSpec) {
                    RowLevelDeleteSpec deleteSpec = (RowLevelDeleteSpec) sinkAbilitySpec;
                    if (deleteSpec.getRowLevelDeleteMode()
                            == SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS) {
                        return Optional.of(RowKind.DELETE);
                    }
                } else if (sinkAbilitySpec instanceof RowLevelUpdateSpec) {
                    RowLevelUpdateSpec updateSpec = (RowLevelUpdateSpec) sinkAbilitySpec;
                    if (updateSpec.getRowLevelUpdateMode()
                            == SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS) {
                        return Optional.of(RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
        return Optional.empty();
    }

    protected abstract boolean legacyPhysicalTypeEnabled();
}

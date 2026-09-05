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

import org.apache.flink.annotation.Internal;
import org.apache.flink.legacy.table.connector.source.SourceFunctionProvider;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.ExternalModifyOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.OutputConversionModifyOperation;
import org.apache.flink.table.planner.connectors.DynamicSinkUtils;
import org.apache.flink.table.planner.plan.nodes.calcite.LegacySink;
import org.apache.flink.table.planner.plan.nodes.calcite.Sink;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Extracts logical column lineage and binds it to processed sink execution nodes. */
@Internal
public final class PlannerColumnLineagePlanBinder {

    private final List<PlannerSinkColumnLineage> logicalLineages;
    private final Map<RelNode, PlannerSinkColumnLineage> physicalLineages = new IdentityHashMap<>();
    private List<PlannerSinkColumnLineage> optimizedLineages;
    private final Map<String, List<TableSourceTable>> logicalSources = new LinkedHashMap<>();

    public PlannerColumnLineagePlanBinder(
            List<PlannerSinkColumnLineage> logicalLineages, List<RelNode> logicalRoots) {
        this.logicalLineages = logicalLineages;
        for (TableSourceTable source : collectSourceTables(logicalRoots)) {
            logicalSources
                    .computeIfAbsent(
                            source.contextResolvedTable().getIdentifier().asSerializableString(),
                            ignored -> new ArrayList<>())
                    .add(source);
        }
    }

    /** Binds roots before source and sink reuse can copy or merge them. */
    public void bindPhysicalRoots(List<RelNode> roots) {
        final Map<String, Deque<PlannerSinkColumnLineage>> bySink = new LinkedHashMap<>();
        for (PlannerSinkColumnLineage lineage : logicalLineages) {
            bySink.computeIfAbsent(lineage.getSinkKey(), ignored -> new ArrayDeque<>())
                    .addLast(lineage);
        }
        for (RelNode root : roots) {
            if (!(root instanceof Sink) || ((Sink) root).contextResolvedTable().isAnonymous()) {
                continue;
            }
            final String key =
                    ((Sink) root).contextResolvedTable().getIdentifier().asSerializableString();
            final Deque<PlannerSinkColumnLineage> candidates = bySink.get(key);
            if (candidates == null || candidates.isEmpty()) {
                throw failure(key, "<unknown>", "no logical lineage for optimized sink");
            }
            physicalLineages.put(root, candidates.removeFirst());
        }
        bySink.forEach(
                (key, candidates) -> {
                    if (!candidates.isEmpty()) {
                        throw failure(key, "<unknown>", "logical lineage has no optimized sink");
                    }
                });
    }

    /** Transfers metadata across root-preserving copies without changing their digests. */
    public void transferRoots(List<RelNode> before, List<RelNode> after) {
        if (before.size() != after.size()) {
            throw failure("<unknown>", "<unknown>", "root copies changed the number of sinks");
        }
        final List<PlannerSinkColumnLineage> lineages = new ArrayList<>();
        for (RelNode root : before) {
            lineages.add(physicalLineages.get(root));
        }
        for (int i = 0; i < after.size(); i++) {
            if (lineages.get(i) != null) {
                physicalLineages.put(after.get(i), lineages.get(i));
            }
        }
    }

    /** Called with the exact group selected by the existing SinkReuser. */
    public void reuseSinks(List<Sink> sinks) {
        final List<PlannerSinkColumnLineage> contributions = new ArrayList<>();
        for (Sink sink : sinks) {
            final PlannerSinkColumnLineage lineage = physicalLineages.get(sink);
            if (lineage == null) {
                if (!sink.contextResolvedTable().isAnonymous()) {
                    throw failure(
                            sink.contextResolvedTable().getIdentifier().asSerializableString(),
                            "<unknown>",
                            "reused sink has no logical lineage");
                }
                return;
            }
            contributions.add(lineage);
        }
        physicalLineages.put(sinks.get(0), merge(contributions));
    }

    public void finishRoots(List<RelNode> roots) {
        optimizedLineages = new ArrayList<>();
        for (RelNode root : roots) {
            final PlannerSinkColumnLineage lineage = physicalLineages.get(root);
            if (lineage == null) {
                optimizedLineages.add(null);
                continue;
            }
            final Set<String> physicalSources = new LinkedHashSet<>();
            for (TableSourceTable source : collectSourceTables(Collections.singletonList(root))) {
                physicalSources.add(
                        source.contextResolvedTable().getIdentifier().asSerializableString());
            }
            final List<PlannerPrunedSource> pruned = new ArrayList<>();
            for (PlannerLineageDataset source : lineage.getExpectedSources()) {
                if (!physicalSources.contains(source.asSerializableString())) {
                    pruned.add(snapshotPrunedSource(source));
                }
            }
            optimizedLineages.add(
                    new PlannerSinkColumnLineage(
                            lineage.getSinkKey(),
                            lineage.getExpectedOutputFields(),
                            lineage.getExpectedSources(),
                            lineage.getRelations(),
                            pruned));
        }
    }

    private PlannerPrunedSource snapshotPrunedSource(PlannerLineageDataset dataset) {
        final List<TableSourceTable> candidates =
                logicalSources.get(dataset.asSerializableString());
        if (candidates == null || candidates.isEmpty()) {
            throw failure(
                    "<unknown>", "<unknown>", "pruned source has no logical snapshot: " + dataset);
        }
        final TableSourceTable source = candidates.get(0);
        for (TableSourceTable candidate : candidates) {
            if (!source.contextResolvedTable()
                    .getResolvedTable()
                    .equals(candidate.contextResolvedTable().getResolvedTable())) {
                throw failure(
                        "<unknown>",
                        "<unknown>",
                        "pruned source identity is ambiguous: " + dataset);
            }
        }
        if (source.flinkContext()
                        .getTableConfig()
                        .get(TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS)
                != TableConfigOptions.CatalogPlanCompilation.ALL) {
            throw failure(
                    "<unknown>",
                    "<unknown>",
                    "pruned source snapshots require table.plan.compile.catalog-objects=ALL; recompile the plan");
        }
        if (!(source.tableSource() instanceof ScanTableSource)) {
            throw failure(
                    "<unknown>", "<unknown>", "cannot snapshot pruned non-scan source: " + dataset);
        }
        final ScanTableSource.ScanRuntimeProvider provider =
                ((ScanTableSource) source.tableSource())
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        final Optional<LineageVertex> vertex;
        if (provider instanceof SourceProvider) {
            vertex =
                    TableLineageUtils.extractLineageDataset(
                            ((SourceProvider) provider).createSource());
        } else if (provider instanceof InputFormatProvider) {
            vertex =
                    TableLineageUtils.extractLineageDataset(
                            ((InputFormatProvider) provider).createInputFormat());
        } else if (provider instanceof SourceFunctionProvider) {
            vertex =
                    TableLineageUtils.extractLineageDataset(
                            ((SourceFunctionProvider) provider).createSourceFunction());
        } else {
            throw failure(
                    "<unknown>",
                    "<unknown>",
                    "cannot snapshot pruned source provider without executing it: "
                            + provider.getClass().getName());
        }
        return new PlannerPrunedSource(
                dataset,
                TableLineageUtils.createTableLineageDataset(source.contextResolvedTable(), vertex)
                        .namespace(),
                source.contextResolvedTable().getResolvedTable());
    }

    private static List<TableSourceTable> collectSourceTables(List<RelNode> roots) {
        final List<TableSourceTable> sources = new ArrayList<>();
        final Set<RelNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        final Deque<RelNode> pending = new ArrayDeque<>(roots);
        while (!pending.isEmpty()) {
            final RelNode node = pending.removeFirst();
            if (!visited.add(node)) {
                continue;
            }
            if (node instanceof TableScan) {
                final TableSourceTable source =
                        ((TableScan) node).getTable().unwrap(TableSourceTable.class);
                if (source != null) {
                    sources.add(source);
                }
            }
            pending.addAll(node.getInputs());
        }
        return sources;
    }

    private static PlannerSinkColumnLineage merge(List<PlannerSinkColumnLineage> contributions) {
        final PlannerSinkColumnLineage first = contributions.get(0);
        final Set<PlannerLineageDataset> sources = new LinkedHashSet<>();
        final Map<String, Set<PlannerColumnLineageInput>> inputs = new LinkedHashMap<>();
        final Map<String, Set<PlannerColumnLineageTransformation>> transformations =
                new LinkedHashMap<>();
        final Set<String> systemFields = new LinkedHashSet<>();
        for (PlannerSinkColumnLineage contribution : contributions) {
            if (!first.getSinkKey().equals(contribution.getSinkKey())
                    || !first.getExpectedOutputFields()
                            .equals(contribution.getExpectedOutputFields())) {
                throw failure(
                        first.getSinkKey(),
                        "<unknown>",
                        "reused sinks have incompatible lineage schemas");
            }
            sources.addAll(contribution.getExpectedSources());
            for (PlannerColumnLineageRelation relation : contribution.getRelations()) {
                inputs.computeIfAbsent(relation.getOutputField(), ignored -> new LinkedHashSet<>())
                        .addAll(relation.getInputs());
                transformations
                        .computeIfAbsent(
                                relation.getOutputField(), ignored -> new LinkedHashSet<>())
                        .addAll(relation.getTransformations());
                if (relation.getOrigin() == PlannerColumnLineageOrigin.SYSTEM) {
                    systemFields.add(relation.getOutputField());
                }
            }
        }
        final List<PlannerColumnLineageRelation> relations = new ArrayList<>();
        for (String field : first.getExpectedOutputFields()) {
            final Set<PlannerColumnLineageInput> fieldInputs = inputs.get(field);
            final boolean direct =
                    fieldInputs.stream()
                            .anyMatch(
                                    input ->
                                            input.getDependencyType()
                                                    == PlannerColumnLineageDependencyType.DIRECT);
            transformations.get(field).add(PlannerColumnLineageTransformation.UNION);
            relations.add(
                    new PlannerColumnLineageRelation(
                            field,
                            new ArrayList<>(fieldInputs),
                            direct
                                    ? PlannerColumnLineageOrigin.INPUT_FIELDS
                                    : systemFields.contains(field)
                                            ? PlannerColumnLineageOrigin.SYSTEM
                                            : PlannerColumnLineageOrigin.CONSTANT,
                            new ArrayList<>(transformations.get(field))));
        }
        return new PlannerSinkColumnLineage(
                first.getSinkKey(),
                first.getExpectedOutputFields(),
                new ArrayList<>(sources),
                relations);
    }

    public static List<PlannerSinkColumnLineage> extract(
            List<RelNode> relNodes, List<?> operations) {
        if (relNodes.size() != operations.size()) {
            throw failure(
                    "<unknown>",
                    "<unknown>",
                    "relational roots and operations cannot be aligned by position");
        }
        final List<PlannerSinkColumnLineage> result = new ArrayList<>();
        for (int i = 0; i < relNodes.size(); i++) {
            final RelNode relNode = relNodes.get(i);
            final Object operation = operations.get(i);
            if (isDataStreamOrClientResult(operation)) {
                continue;
            }
            if (relNode instanceof LegacySink) {
                final LegacySink legacySink = (LegacySink) relNode;
                throw failure(
                        legacySink.sinkName() == null ? "<anonymous>" : legacySink.sinkName(),
                        "<unknown>",
                        "legacy Table sink cannot carry the required column-lineage contract");
            }
            if (!(relNode instanceof Sink)) {
                if (operation instanceof ModifyOperation) {
                    throw failure(
                            "<unknown>",
                            "<unknown>",
                            "operation '"
                                    + operation.getClass().getName()
                                    + "' produced unsupported relational root '"
                                    + relNode.getClass().getName()
                                    + "'");
                }
                continue;
            }
            final Sink sink = (Sink) relNode;
            if (sink.contextResolvedTable().isAnonymous()) {
                throw failure(
                        sink.contextResolvedTable().getIdentifier().asSummaryString(),
                        "<unknown>",
                        "anonymous Table sink has no stable dataset identity");
            }
            final String sinkKey =
                    sink.contextResolvedTable().getIdentifier().asSerializableString();
            final List<String> sinkFields =
                    DynamicSinkUtils.createConsumedType(
                                    sink.contextResolvedTable().getResolvedSchema(),
                                    sink.tableSink())
                            .getFieldNames();
            try {
                result.add(
                        PlannerColumnLineageExtractor.extract(
                                sinkKey, sinkFields, sink.getInput()));
            } catch (TableLineageExtractionException e) {
                throw failure(sinkKey, "<unknown>", e.getMessage());
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static boolean isDataStreamOrClientResult(Object operation) {
        return operation instanceof CollectModifyOperation
                || operation instanceof ExternalModifyOperation
                || operation instanceof OutputConversionModifyOperation;
    }

    public ExecNodeGraph bind(ExecNodeGraph execNodeGraph) {
        if (optimizedLineages == null
                || optimizedLineages.size() != execNodeGraph.getRootNodes().size()) {
            throw failure(
                    "<unknown>",
                    "<unknown>",
                    "execution roots cannot be aligned with optimized lineage");
        }
        for (int i = 0; i < execNodeGraph.getRootNodes().size(); i++) {
            final ExecNode<?> rootNode = execNodeGraph.getRootNodes().get(i);
            final PlannerSinkColumnLineage lineage = optimizedLineages.get(i);
            if (!(rootNode instanceof CommonExecSink)) {
                if (lineage != null) {
                    throw failure(
                            lineage.getSinkKey(),
                            "<unknown>",
                            "execution root lost its Table sink");
                }
                continue;
            }
            final CommonExecSink sink = (CommonExecSink) rootNode;
            if (sink.getTableSinkSpec().getContextResolvedTable().isAnonymous()) {
                // Extraction admits anonymous sinks only for the explicit client-result and
                // Table-to-DataStream operations above. Restored plans do not pass extraction, so
                // CommonExecSink performs the same check against the actual runtime sink class.
                continue;
            }
            final String sinkKey =
                    sink.getTableSinkSpec()
                            .getContextResolvedTable()
                            .getIdentifier()
                            .asSerializableString();
            if (lineage == null || !sinkKey.equals(lineage.getSinkKey())) {
                throw failure(sinkKey, "<unknown>", "no complete logical column lineage was bound");
            }
            sink.getTableSinkSpec().setColumnLineage(lineage);
        }
        return execNodeGraph;
    }

    private static TableLineageExtractionException failure(
            String sink, String field, String reason) {
        return new TableLineageExtractionException(
                "Cannot extract complete column lineage for sink '"
                        + sink
                        + "', field '"
                        + field
                        + "': "
                        + reason
                        + ".");
    }
}

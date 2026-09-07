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
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.source.ScanTableSource;
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
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(PlannerColumnLineagePlanBinder.class);
    private String failureReason;

    /** Creates a best-effort observer; extraction failures cannot veto optimization. */
    public static PlannerColumnLineagePlanBinder observe(List<RelNode> roots, List<?> operations) {
        try {
            if (roots.size() != operations.size()) {
                throw failure("<unknown>", "<unknown>", "roots and operations cannot be aligned");
            }
            List<PlannerSinkColumnLineage> columns =
                    new ArrayList<>(Collections.nCopies(roots.size(), null));
            List<PlannerSinkTableLineage> tables =
                    new ArrayList<>(Collections.nCopies(roots.size(), null));
            for (int i = 0; i < roots.size(); i++) {
                RelNode root = roots.get(i);
                if (!(root instanceof Sink) || isDataStreamOrClientResult(operations.get(i))) {
                    continue;
                }
                Sink sink = (Sink) root;
                if (sink.contextResolvedTable().isAnonymous()) {
                    continue;
                }
                String key = sink.contextResolvedTable().getIdentifier().asSerializableString();
                try {
                    columns.set(
                            i,
                            extract(
                                            Collections.singletonList(root),
                                            Collections.singletonList(operations.get(i)))
                                    .get(0));
                } catch (RuntimeException error) {
                    report("Column lineage unavailable for sink " + key, error);
                }
                try {
                    if (!sink.contextResolvedTable().isAnonymous()) {
                        tables.set(
                                i,
                                new PlannerSinkTableLineage(
                                        key,
                                        PlannerTableLineageExtractor.extract(sink.getInput()),
                                        Collections.emptyList()));
                    }
                } catch (RuntimeException error) {
                    report("Logical table lineage unavailable for sink " + key, error);
                }
            }
            PlannerColumnLineagePlanBinder observer =
                    new PlannerColumnLineagePlanBinder(columns, roots);
            observer.logicalTables.clear();
            observer.logicalTables.addAll(tables);
            return observer;
        } catch (RuntimeException e) {
            PlannerColumnLineagePlanBinder observer =
                    new PlannerColumnLineagePlanBinder(
                            Collections.emptyList(), Collections.emptyList());
            observer.fail(e);
            return observer;
        }
    }

    private void fail(RuntimeException error) {
        failureReason =
                error instanceof TableLineageExtractionException
                        ? error.getMessage()
                        : error.getClass().getSimpleName() + ": " + error.getMessage();
        physicalLineages.clear();
        physicalTables.clear();
        optimizedLineages = null;
        optimizedTables = null;
        report("Lineage observation failed", error);
    }

    private static void report(String context, RuntimeException error) {
        if (error instanceof TableLineageExtractionException) {
            LOG.warn("{}; execution continues. Reason: {}", context, error.getMessage());
            LOG.debug(context, error);
        } else {
            LOG.warn("{}; execution continues.", context, error);
        }
    }

    private void observe(Runnable action) {
        if (failureReason != null) {
            return;
        }
        try {
            action.run();
        } catch (RuntimeException error) {
            fail(error);
        }
    }

    /** Prevents publication of a partial job graph after an observer callback failed. */
    public void markFailedTransformations(
            List<? extends org.apache.flink.api.dag.Transformation<?>> transformations) {
        if (failureReason != null) {
            transformations.forEach(t -> t.setLineageFailure(failureReason));
        }
    }

    private final List<PlannerSinkColumnLineage> logicalLineages;
    private final List<RelNode> logicalRoots;
    private final Map<RelNode, PlannerSinkColumnLineage> physicalLineages = new IdentityHashMap<>();
    private List<PlannerSinkColumnLineage> optimizedLineages;
    private final List<PlannerSinkTableLineage> logicalTables = new ArrayList<>();
    private final Map<RelNode, PlannerSinkTableLineage> physicalTables = new IdentityHashMap<>();
    private List<PlannerSinkTableLineage> optimizedTables;
    private final Map<String, List<TableSourceTable>> logicalSources = new LinkedHashMap<>();

    public PlannerColumnLineagePlanBinder(
            List<PlannerSinkColumnLineage> logicalLineages, List<RelNode> logicalRoots) {
        this.logicalLineages = logicalLineages;
        this.logicalRoots = new ArrayList<>(logicalRoots);
        for (PlannerSinkColumnLineage lineage : logicalLineages) {
            logicalTables.add(
                    lineage == null
                            ? null
                            : new PlannerSinkTableLineage(
                                    lineage.getSinkKey(),
                                    lineage.getExpectedSources(),
                                    Collections.emptyList()));
        }
        for (TableSourceTable source : collectSourceTables(logicalRoots)) {
            if (source.contextResolvedTable().isAnonymous()) {
                continue;
            }
            logicalSources
                    .computeIfAbsent(
                            source.contextResolvedTable().getIdentifier().asSerializableString(),
                            ignored -> new ArrayList<>())
                    .add(source);
        }
    }

    /** Binds roots before source and sink reuse can copy or merge them. */
    public void bindPhysicalRoots(List<RelNode> roots) {
        observe(() -> bindPhysicalRootsChecked(roots));
    }

    private void bindPhysicalRootsChecked(List<RelNode> roots) {
        if (roots.size() != logicalRoots.size() || roots.size() != logicalLineages.size()) {
            throw failure(
                    "<unknown>",
                    "<unknown>",
                    "logical and physical writer slots cannot be aligned");
        }
        // The optimizer maps sink blocks in root order before SubplanReuser invokes this
        // callback. Keep unavailable slots: a dataset key cannot distinguish its writers.
        for (int i = 0; i < roots.size(); i++) {
            RelNode root = roots.get(i);
            RelNode logicalRoot = logicalRoots.get(i);
            if (!(logicalRoot instanceof Sink)) {
                if (root instanceof Sink) {
                    throw failure("<unknown>", "<unknown>", "optimized writer changed root kind");
                }
                continue;
            }
            if (!(root instanceof Sink)
                    || !((Sink) logicalRoot)
                            .contextResolvedTable()
                            .equals(((Sink) root).contextResolvedTable())) {
                throw failure(
                        "<unknown>", "<unknown>", "optimized writer changed its sink context");
            }
            if (logicalTables.get(i) != null) {
                physicalTables.put(root, logicalTables.get(i));
            }
            if (logicalLineages.get(i) != null) {
                physicalLineages.put(root, logicalLineages.get(i));
            }
        }
    }

    /** Transfers metadata across root-preserving copies without changing their digests. */
    public void transferRoots(List<RelNode> before, List<RelNode> after) {
        observe(() -> transferRootsChecked(before, after));
    }

    private void transferRootsChecked(List<RelNode> before, List<RelNode> after) {
        if (before.size() != after.size()) {
            throw failure("<unknown>", "<unknown>", "root copies changed the number of sinks");
        }
        final List<PlannerSinkColumnLineage> lineages = new ArrayList<>();
        final List<PlannerSinkTableLineage> tables = new ArrayList<>();
        for (RelNode root : before) {
            lineages.add(physicalLineages.get(root));
            tables.add(physicalTables.get(root));
        }
        for (int i = 0; i < after.size(); i++) {
            if (tables.get(i) != null) {
                physicalTables.put(after.get(i), tables.get(i));
            } else {
                physicalTables.remove(after.get(i));
            }
            if (lineages.get(i) != null) {
                physicalLineages.put(after.get(i), lineages.get(i));
            } else {
                physicalLineages.remove(after.get(i));
            }
        }
    }

    /** Called with the exact group selected by the existing SinkReuser. */
    public void reuseSinks(List<Sink> sinks) {
        if (failureReason != null) {
            return;
        }
        try {
            reuseSinksChecked(sinks);
        } catch (RuntimeException error) {
            sinks.forEach(
                    sink -> {
                        physicalTables.remove(sink);
                        physicalLineages.remove(sink);
                    });
            report("Lineage unavailable for reused sink group", error);
        }
    }

    private void reuseSinksChecked(List<Sink> sinks) {
        Set<PlannerLineageDataset> sources = new LinkedHashSet<>();
        boolean completeTables = true;
        for (Sink sink : sinks) {
            PlannerSinkTableLineage table = physicalTables.get(sink);
            if (table == null) {
                completeTables = false;
            } else {
                sources.addAll(table.getExpectedSources());
            }
        }
        if (completeTables) {
            physicalTables.put(
                    sinks.get(0),
                    new PlannerSinkTableLineage(
                            sinks.get(0)
                                    .contextResolvedTable()
                                    .getIdentifier()
                                    .asSerializableString(),
                            new ArrayList<>(sources),
                            Collections.emptyList()));
        } else {
            physicalTables.remove(sinks.get(0));
        }
        final List<PlannerSinkColumnLineage> contributions = new ArrayList<>();
        for (Sink sink : sinks) {
            final PlannerSinkColumnLineage lineage = physicalLineages.get(sink);
            if (lineage == null) {
                physicalLineages.remove(sinks.get(0));
                return;
            }
            contributions.add(lineage);
        }
        physicalLineages.put(sinks.get(0), merge(contributions));
    }

    public void finishRoots(List<RelNode> roots) {
        observe(() -> finishRootsChecked(roots));
    }

    private void finishRootsChecked(List<RelNode> roots) {
        optimizedLineages = new ArrayList<>();
        optimizedTables = new ArrayList<>();
        for (RelNode root : roots) {
            PlannerSinkTableLineage table = physicalTables.get(root);
            try {
                optimizedTables.add(
                        table == null
                                ? null
                                : new PlannerSinkTableLineage(
                                        table.getSinkKey(),
                                        table.getExpectedSources(),
                                        snapshotPrunedSources(root, table.getExpectedSources())));
            } catch (RuntimeException error) {
                optimizedTables.add(null);
                report("Unable to preserve logical table snapshots", error);
            }
            final PlannerSinkColumnLineage lineage = physicalLineages.get(root);
            if (lineage == null) {
                optimizedLineages.add(null);
                continue;
            }
            try {
                final List<PlannerPrunedSource> pruned =
                        snapshotPrunedSources(root, lineage.getExpectedSources());
                optimizedLineages.add(
                        new PlannerSinkColumnLineage(
                                lineage.getSinkKey(),
                                lineage.getExpectedOutputFields(),
                                lineage.getExpectedSources(),
                                lineage.getRelations(),
                                pruned));
            } catch (RuntimeException error) {
                optimizedLineages.add(null);
                report(
                        "Unable to preserve column snapshots for sink " + lineage.getSinkKey(),
                        error);
            }
        }
    }

    private List<PlannerPrunedSource> snapshotPrunedSources(
            RelNode root, List<PlannerLineageDataset> expected) {
        Set<String> physicalSources = new LinkedHashSet<>();
        for (TableSourceTable source : collectSourceTables(Collections.singletonList(root))) {
            physicalSources.add(
                    source.contextResolvedTable().getIdentifier().asSerializableString());
        }
        List<PlannerPrunedSource> pruned = new ArrayList<>();
        for (PlannerLineageDataset source : expected) {
            if (!physicalSources.contains(source.asSerializableString())) {
                pruned.add(snapshotPrunedSource(source));
            }
        }
        return pruned;
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
        // An eliminated scan has no runtime source. Use only metadata already exposed by the
        // table source; otherwise retain its logical catalog identity and frozen schema.
        String namespace;
        try {
            namespace =
                    TableLineageUtils.createTableLineageDataset(
                                    source.contextResolvedTable(),
                                    TableLineageUtils.extractLineageDataset(source.tableSource()))
                            .namespace();
        } catch (RuntimeException error) {
            report("Unable to obtain connector identity for pruned source " + dataset, error);
            namespace =
                    TableLineageUtils.createTableLineageDataset(
                                    source.contextResolvedTable(), Optional.empty())
                            .namespace();
        }
        return new PlannerPrunedSource(
                dataset, namespace, source.contextResolvedTable().getResolvedTable());
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
            node.accept(
                    new org.apache.calcite.rex.RexShuttle() {
                        @Override
                        public org.apache.calcite.rex.RexNode visitSubQuery(
                                org.apache.calcite.rex.RexSubQuery subQuery) {
                            pending.add(
                                    subQuery.rel.accept(
                                            new org.apache.flink.table.planner.plan.utils
                                                    .ExpandTableScanShuttle()));
                            return super.visitSubQuery(subQuery);
                        }
                    });
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
        observe(() -> bindChecked(execNodeGraph));
        if (failureReason != null) {
            for (ExecNode<?> root : execNodeGraph.getRootNodes()) {
                if (root instanceof CommonExecSink) {
                    ((CommonExecSink) root).getTableSinkSpec().setColumnLineage(null);
                    ((CommonExecSink) root).getTableSinkSpec().setTableLineage(null);
                }
            }
        }
        return execNodeGraph;
    }

    private ExecNodeGraph bindChecked(ExecNodeGraph execNodeGraph) {
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
            if (optimizedTables != null && optimizedTables.size() == optimizedLineages.size()) {
                sink.getTableSinkSpec().setTableLineage(optimizedTables.get(i));
            }
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
            if (lineage != null && !sinkKey.equals(lineage.getSinkKey())) {
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

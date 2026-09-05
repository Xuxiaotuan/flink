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
 *
 */

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.api.transformations.TransformationWithLineage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Utils for building lineage graph from transformations. */
@Internal
public class LineageGraphUtils {

    /** Convert transforms to LineageGraph. */
    public static LineageGraph convertToLineageGraph(List<Transformation<?>> transformations) {
        DefaultLineageGraph.LineageGraphBuilder builder = DefaultLineageGraph.builder();
        List<ColumnLineageContribution> columnLineageContributions = new ArrayList<>();

        // find complete edges
        for (Transformation<?> transformation : transformations) {
            List<LineageEdge> edges =
                    processSink(transformation, builder, columnLineageContributions);
            for (LineageEdge lineageEdge : edges) {
                builder.addLineageEdge(lineageEdge);
            }
        }

        // find standalone sources
        for (Transformation<?> transformation : transformations) {
            Optional<SourceLineageVertex> sourceOpt = processSource(transformation);
            sourceOpt.ifPresent(builder::addSourceVertex);
        }

        addMergedColumnLineage(columnLineageContributions, builder);

        return builder.build();
    }

    private static List<LineageEdge> processSink(
            Transformation<?> transformation,
            DefaultLineageGraph.LineageGraphBuilder builder,
            List<ColumnLineageContribution> columnLineageContributions) {
        List<LineageEdge> lineageEdges = new ArrayList<>();
        LineageVertex sinkLineageVertex = null;
        TransformationColumnLineage columnLineage = null;
        if (transformation instanceof TransformationWithLineage) {
            TransformationWithLineage<?> carrier = (TransformationWithLineage<?>) transformation;
            columnLineage = carrier.getColumnLineage();
            if (columnLineage != null) {
                sinkLineageVertex = validateColumnLineageSinkVertex(transformation, carrier);
            }
        }
        if (sinkLineageVertex == null && transformation instanceof SinkTransformation) {
            sinkLineageVertex = ((SinkTransformation<?, ?>) transformation).getLineageVertex();
        } else if (sinkLineageVertex == null
                && transformation instanceof LegacySinkTransformation) {
            sinkLineageVertex = ((LegacySinkTransformation) transformation).getLineageVertex();
        }

        if (sinkLineageVertex != null) {
            if (columnLineage != null) {
                columnLineageContributions.add(
                        validateColumnLineageContribution(
                                transformation, sinkLineageVertex, columnLineage));
            }
            List<Transformation<?>> predecessors = transformation.getTransitivePredecessors();
            boolean hasEdge = false;
            if (columnLineage != null) {
                // The lineage graph describes SQL dependencies, including scans optimized away.
                // Keep those out of the execution topology: they are metadata, not runtime reads.
                for (SourceLineageVertex prunedSource : columnLineage.getPrunedSources()) {
                    lineageEdges.add(new DefaultLineageEdge(prunedSource, sinkLineageVertex));
                    hasEdge = true;
                }
            }
            for (Transformation<?> predecessor : predecessors) {
                Optional<SourceLineageVertex> sourceOpt = processSource(predecessor);
                if (sourceOpt.isPresent()) {
                    lineageEdges.add(new DefaultLineageEdge(sourceOpt.get(), sinkLineageVertex));
                    hasEdge = true;
                }
            }

            if (!hasEdge) {
                // In case all of the source connectors haven't integrated with lineage provider
                builder.addSinkVertex(sinkLineageVertex);
            }
        } else {
            List<Transformation<?>> predecessors = transformation.getTransitivePredecessors();
            for (Transformation<?> predecessor : predecessors) {
                Optional<SourceLineageVertex> sourceOpt = processSource(predecessor);
                sourceOpt.ifPresent(builder::addSourceVertex);
            }
        }
        return lineageEdges;
    }

    private static LineageVertex validateColumnLineageSinkVertex(
            Transformation<?> transformation, TransformationWithLineage<?> carrier) {
        LineageVertex lineageVertex = carrier.getLineageVertex();
        if (lineageVertex == null) {
            throw invalidCarrier(transformation, "sink lineage vertex must not be null");
        }
        if (lineageVertex instanceof SourceLineageVertex) {
            throw invalidCarrier(transformation, "sink lineage vertex must not be a source vertex");
        }
        if (lineageVertex.datasets() == null
                || lineageVertex.datasets().size() != 1
                || lineageVertex.datasets().get(0) == null) {
            throw invalidCarrier(
                    transformation,
                    "sink lineage vertex must identify exactly one non-null dataset");
        }
        return lineageVertex;
    }

    private static ColumnLineageContribution validateColumnLineageContribution(
            Transformation<?> transformation,
            LineageVertex sinkLineageVertex,
            TransformationColumnLineage columnLineage) {
        LineageDataset sinkDataset = sinkLineageVertex.datasets().get(0);
        Set<String> expectedFields = new LinkedHashSet<>();
        for (String field : columnLineage.getExpectedOutputFields()) {
            if (!expectedFields.add(field)) {
                throw invalidCarrier(
                        transformation, "duplicate expected output field '" + field + "'");
            }
        }
        Set<String> relationFields = new LinkedHashSet<>();
        for (ColumnLineageRelation relation : columnLineage.getRelations()) {
            if (relation == null) {
                throw invalidCarrier(transformation, "column lineage relation must not be null");
            }
            if (!sameDataset(sinkDataset, relation.outputDataset())) {
                throw invalidCarrier(
                        transformation,
                        "column lineage relation output dataset does not match its sink dataset");
            }
            if (!expectedFields.contains(relation.outputField())) {
                throw invalidCarrier(
                        transformation,
                        "unexpected column lineage relation for field '"
                                + relation.outputField()
                                + "'");
            }
            if (!relationFields.add(relation.outputField())) {
                throw invalidCarrier(
                        transformation,
                        "duplicate column lineage relation for field '"
                                + relation.outputField()
                                + "'");
            }
        }
        if (!relationFields.equals(expectedFields)) {
            Set<String> missingFields = new LinkedHashSet<>(expectedFields);
            missingFields.removeAll(relationFields);
            throw invalidCarrier(
                    transformation,
                    "sink dataset '"
                            + sinkDataset.namespace()
                            + "."
                            + sinkDataset.name()
                            + "', field '"
                            + missingFields.iterator().next()
                            + "': missing output relation");
        }
        return new ColumnLineageContribution(
                sinkDataset, expectedFields, columnLineage.getRelations());
    }

    private static void addMergedColumnLineage(
            List<ColumnLineageContribution> contributions,
            DefaultLineageGraph.LineageGraphBuilder builder) {
        Map<DatasetKey, Set<String>> expectedFieldsByDataset = new LinkedHashMap<>();
        Map<OutputFieldKey, MergedRelation> mergedRelations = new LinkedHashMap<>();
        for (ColumnLineageContribution contribution : contributions) {
            DatasetKey datasetKey = DatasetKey.of(contribution.sinkDataset);
            Set<String> previousExpectedFields =
                    expectedFieldsByDataset.putIfAbsent(
                            datasetKey, new LinkedHashSet<>(contribution.expectedFields));
            if (previousExpectedFields != null) {
                previousExpectedFields.addAll(contribution.expectedFields);
            }
            for (ColumnLineageRelation relation : contribution.relations) {
                mergedRelations
                        .computeIfAbsent(
                                OutputFieldKey.of(contribution.sinkDataset, relation.outputField()),
                                ignored ->
                                        new MergedRelation(
                                                contribution.sinkDataset, relation.outputField()))
                        .add(relation);
            }
        }

        expectedFieldsByDataset.forEach(
                (datasetKey, fields) -> {
                    LineageDataset dataset = datasetKey.dataset;
                    fields.forEach(field -> builder.addExpectedOutputField(dataset, field));
                });
        mergedRelations
                .values()
                .forEach(merged -> builder.addColumnLineageRelation(merged.build()));
    }

    private static boolean sameDataset(LineageDataset left, LineageDataset right) {
        return left != null
                && right != null
                && Objects.equals(left.namespace(), right.namespace())
                && Objects.equals(left.name(), right.name());
    }

    private static IllegalArgumentException invalidCarrier(
            Transformation<?> transformation, String reason) {
        return new IllegalArgumentException(
                "Invalid column lineage on transformation '"
                        + transformation.getName()
                        + "': "
                        + reason
                        + ".");
    }

    private static Optional<SourceLineageVertex> processSource(Transformation<?> transformation) {
        if (transformation instanceof SourceTransformation) {
            if (((SourceTransformation) transformation).getLineageVertex() != null) {
                return Optional.of(
                        (SourceLineageVertex)
                                ((SourceTransformation) transformation).getLineageVertex());
            }
        } else if (transformation instanceof LegacySourceTransformation) {
            if (((LegacySourceTransformation) transformation).getLineageVertex() != null) {
                return Optional.of(
                        (SourceLineageVertex)
                                ((LegacySourceTransformation) transformation).getLineageVertex());
            }
        }
        return Optional.empty();
    }

    private static final class ColumnLineageContribution {
        private final LineageDataset sinkDataset;
        private final Set<String> expectedFields;
        private final List<ColumnLineageRelation> relations;

        private ColumnLineageContribution(
                LineageDataset sinkDataset,
                Set<String> expectedFields,
                List<ColumnLineageRelation> relations) {
            this.sinkDataset = sinkDataset;
            this.expectedFields = expectedFields;
            this.relations = relations;
        }
    }

    private static final class MergedRelation {
        private final LineageDataset outputDataset;
        private final String outputField;
        private final Map<InputKey, ColumnLineageInput> inputs = new LinkedHashMap<>();
        private final Set<String> transformations = new LinkedHashSet<>();
        private ColumnLineageRelation firstRelation;
        private boolean hasDirectInput;
        private boolean hasSystemOrigin;
        private int physicalWriteCount;

        private MergedRelation(LineageDataset outputDataset, String outputField) {
            this.outputDataset = outputDataset;
            this.outputField = outputField;
        }

        private void add(ColumnLineageRelation relation) {
            physicalWriteCount++;
            if (firstRelation == null) {
                firstRelation = relation;
            }
            for (ColumnLineageInput input : relation.inputs()) {
                inputs.putIfAbsent(InputKey.of(input), input);
                hasDirectInput |= input.dependencyType() == ColumnLineageDependencyType.DIRECT;
            }
            hasSystemOrigin |= relation.origin() == ColumnLineageOrigin.SYSTEM;
            relation.transformation()
                    .ifPresent(
                            value -> {
                                for (String token : value.split(",")) {
                                    String normalized = token.trim();
                                    if (!normalized.isEmpty()) {
                                        transformations.add(normalized);
                                    }
                                }
                            });
        }

        private ColumnLineageRelation build() {
            if (physicalWriteCount == 1) {
                return firstRelation;
            }
            if (physicalWriteCount > 1) {
                transformations.add("UNION");
            }
            ColumnLineageOrigin origin =
                    hasDirectInput
                            ? ColumnLineageOrigin.INPUT_FIELDS
                            : hasSystemOrigin
                                    ? ColumnLineageOrigin.SYSTEM
                                    : ColumnLineageOrigin.CONSTANT;
            String transformation =
                    transformations.isEmpty() ? null : String.join(",", transformations);
            return new DefaultColumnLineageRelation(
                    outputDataset,
                    outputField,
                    new ArrayList<>(inputs.values()),
                    origin,
                    transformation);
        }
    }

    private static final class DatasetKey {
        private final LineageDataset dataset;
        private final String namespace;
        private final String name;

        private DatasetKey(LineageDataset dataset) {
            this.dataset = dataset;
            this.namespace = dataset.namespace();
            this.name = dataset.name();
        }

        private static DatasetKey of(LineageDataset dataset) {
            return new DatasetKey(dataset);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof DatasetKey)) {
                return false;
            }
            DatasetKey that = (DatasetKey) object;
            return Objects.equals(namespace, that.namespace) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, name);
        }

        @Override
        public String toString() {
            return namespace + "." + name;
        }
    }

    private static final class OutputFieldKey {
        private final DatasetKey dataset;
        private final String field;

        private OutputFieldKey(DatasetKey dataset, String field) {
            this.dataset = dataset;
            this.field = field;
        }

        private static OutputFieldKey of(LineageDataset dataset, String field) {
            return new OutputFieldKey(DatasetKey.of(dataset), field);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof OutputFieldKey)) {
                return false;
            }
            OutputFieldKey that = (OutputFieldKey) object;
            return dataset.equals(that.dataset) && Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataset, field);
        }
    }

    private static final class InputKey {
        private final DatasetKey dataset;
        private final String field;
        private final ColumnLineageDependencyType dependencyType;

        private InputKey(
                DatasetKey dataset, String field, ColumnLineageDependencyType dependencyType) {
            this.dataset = dataset;
            this.field = field;
            this.dependencyType = dependencyType;
        }

        private static InputKey of(ColumnLineageInput input) {
            return new InputKey(
                    DatasetKey.of(input.inputDataset()),
                    input.inputField(),
                    input.dependencyType());
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof InputKey)) {
                return false;
            }
            InputKey that = (InputKey) object;
            return dataset.equals(that.dataset)
                    && Objects.equals(field, that.field)
                    && dependencyType == that.dependencyType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataset, field, dependencyType);
        }
    }
}

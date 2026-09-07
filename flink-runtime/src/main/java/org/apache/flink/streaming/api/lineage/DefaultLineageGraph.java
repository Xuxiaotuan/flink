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

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.Internal;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Default implementation for {@link LineageGraph}. */
@Internal
public class DefaultLineageGraph implements LineageGraph {
    @JsonProperty private final List<LineageEdge> lineageEdges;
    @JsonProperty private final List<ColumnLineageRelation> columnLineageRelations;
    @JsonProperty private final Set<SourceLineageVertex> sources;
    @JsonProperty private final Set<LineageVertex> sinks;

    private DefaultLineageGraph(
            List<LineageEdge> lineageEdges, List<ColumnLineageRelation> columnLineageRelations) {
        this.lineageEdges = List.copyOf(lineageEdges);
        this.columnLineageRelations = List.copyOf(columnLineageRelations);
        this.sources = new HashSet<>();
        this.sinks = new HashSet<>();
        for (LineageEdge lineageEdge : this.lineageEdges) {
            sources.add(lineageEdge.source());
            sinks.add(lineageEdge.sink());
        }
    }

    @Override
    public List<SourceLineageVertex> sources() {
        return List.copyOf(sources);
    }

    @Override
    public List<LineageVertex> sinks() {
        return List.copyOf(sinks);
    }

    @Override
    public List<LineageEdge> relations() {
        return lineageEdges;
    }

    @Override
    public List<ColumnLineageRelation> columnRelations() {
        return columnLineageRelations;
    }

    void addSources(List<SourceLineageVertex> partialSources) {
        this.sources.addAll(partialSources);
    }

    void addSinks(List<LineageVertex> partialSinks) {
        this.sinks.addAll(partialSinks);
    }

    public static LineageGraphBuilder builder() {
        return new LineageGraphBuilder();
    }

    /** Build the default lineage graph from {@link LineageEdge}. */
    @Internal
    public static class LineageGraphBuilder {
        private final List<LineageEdge> lineageEdges;
        private final List<ColumnLineageRelation> columnLineageRelations;
        private final List<ExpectedOutputField> expectedOutputFields;
        private final List<SourceLineageVertex> sources;
        private final List<LineageVertex> sinks;

        private LineageGraphBuilder() {
            this.lineageEdges = new ArrayList<>();
            this.columnLineageRelations = new ArrayList<>();
            this.expectedOutputFields = new ArrayList<>();
            this.sources = new ArrayList<>();
            this.sinks = new ArrayList<>();
        }

        public LineageGraphBuilder addLineageEdge(LineageEdge lineageEdge) {
            this.lineageEdges.add(lineageEdge);
            return this;
        }

        public LineageGraphBuilder addLineageEdges(LineageEdge... lineageEdges) {
            this.lineageEdges.addAll(Arrays.asList(lineageEdges));
            return this;
        }

        public LineageGraphBuilder addColumnLineageRelation(
                ColumnLineageRelation columnLineageRelation) {
            this.columnLineageRelations.add(columnLineageRelation);
            return this;
        }

        public LineageGraphBuilder addColumnLineageRelations(
                ColumnLineageRelation... columnLineageRelations) {
            this.columnLineageRelations.addAll(Arrays.asList(columnLineageRelations));
            return this;
        }

        public LineageGraphBuilder addExpectedOutputField(
                LineageDataset outputDataset, String outputField) {
            this.expectedOutputFields.add(new ExpectedOutputField(outputDataset, outputField));
            return this;
        }

        public LineageGraphBuilder addSourceVertex(SourceLineageVertex sourceVertex) {
            this.sources.add(sourceVertex);
            return this;
        }

        public LineageGraphBuilder addSourceVertexes(SourceLineageVertex... sourceVertexes) {
            this.sources.addAll(Arrays.asList(sourceVertexes));
            return this;
        }

        public LineageGraphBuilder addSinkVertex(LineageVertex sinkVertex) {
            this.sinks.add(sinkVertex);
            return this;
        }

        public LineageGraphBuilder addSinkVertexes(List<LineageVertex> sinkVertex) {
            this.sinks.addAll(sinkVertex);
            return this;
        }

        public LineageGraph build() {
            validateColumnLineage(
                    lineageEdges, sources, sinks, expectedOutputFields, columnLineageRelations);
            DefaultLineageGraph lineageGraph =
                    new DefaultLineageGraph(lineageEdges, columnLineageRelations);
            lineageGraph.addSinks(sinks);
            lineageGraph.addSources(sources);
            return lineageGraph;
        }
    }

    private static void validateColumnLineage(
            List<LineageEdge> lineageEdges,
            List<SourceLineageVertex> sourceVertices,
            List<LineageVertex> sinkVertices,
            List<ExpectedOutputField> expectedOutputFields,
            List<ColumnLineageRelation> columnLineageRelations) {
        if (expectedOutputFields.isEmpty() && columnLineageRelations.isEmpty()) {
            return;
        }

        Set<DatasetKey> graphSinkDatasets = collectGraphSinkDatasets(lineageEdges, sinkVertices);
        Set<DatasetKey> graphSourceDatasets =
                collectGraphSourceDatasets(lineageEdges, sourceVertices);
        Set<OutputFieldKey> expectedOutputs = new LinkedHashSet<>();
        for (ExpectedOutputField expectedOutput : expectedOutputFields) {
            validateDatasetIdentity(
                    expectedOutput.outputDataset,
                    expectedOutput.outputDataset,
                    expectedOutput.outputField,
                    "output dataset must have a stable dataset identity");
            validateField(
                    expectedOutput.outputField,
                    expectedOutput.outputDataset,
                    expectedOutput.outputField,
                    "output field must not be blank");
            validateGraphSinkDataset(
                    graphSinkDatasets, expectedOutput.outputDataset, expectedOutput.outputField);
            expectedOutputs.add(
                    OutputFieldKey.of(expectedOutput.outputDataset, expectedOutput.outputField));
        }

        Set<OutputFieldKey> relationOutputs = new LinkedHashSet<>();
        for (ColumnLineageRelation relation : columnLineageRelations) {
            if (relation == null) {
                throw invalidColumnLineage(null, null, "output relation must not be null");
            }
            LineageDataset outputDataset = relation.outputDataset();
            String outputField = relation.outputField();
            validateDatasetIdentity(
                    outputDataset,
                    outputDataset,
                    outputField,
                    "output dataset must have a stable dataset identity");
            validateField(
                    outputField, outputDataset, outputField, "output field must not be blank");
            validateGraphSinkDataset(graphSinkDatasets, outputDataset, outputField);

            OutputFieldKey outputKey = OutputFieldKey.of(outputDataset, outputField);
            if (!expectedOutputs.contains(outputKey)) {
                throw invalidColumnLineage(
                        outputDataset, outputField, "unexpected output relation");
            }
            if (!relationOutputs.add(outputKey)) {
                throw invalidColumnLineage(outputDataset, outputField, "duplicate output relation");
            }

            validateRelation(relation, outputDataset, outputField, graphSourceDatasets);
        }

        for (OutputFieldKey expectedOutput : expectedOutputs) {
            if (!relationOutputs.contains(expectedOutput)) {
                throw invalidColumnLineage(
                        expectedOutput.namespace,
                        expectedOutput.name,
                        expectedOutput.field,
                        "missing output relation");
            }
        }
    }

    private static Set<DatasetKey> collectGraphSinkDatasets(
            List<LineageEdge> lineageEdges, List<LineageVertex> sinkVertices) {
        Set<LineageVertex> graphSinkVertices = new LinkedHashSet<>(sinkVertices);
        for (LineageEdge lineageEdge : lineageEdges) {
            graphSinkVertices.add(lineageEdge.sink());
        }

        Set<DatasetKey> graphSinkDatasets = new LinkedHashSet<>();
        for (LineageVertex sinkVertex : graphSinkVertices) {
            if (sinkVertex == null || sinkVertex.datasets() == null) {
                continue;
            }
            for (LineageDataset dataset : sinkVertex.datasets()) {
                if (dataset != null && !isBlank(dataset.namespace()) && !isBlank(dataset.name())) {
                    graphSinkDatasets.add(DatasetKey.of(dataset));
                }
            }
        }
        return graphSinkDatasets;
    }

    private static Set<DatasetKey> collectGraphSourceDatasets(
            List<LineageEdge> lineageEdges, List<SourceLineageVertex> sourceVertices) {
        Set<SourceLineageVertex> graphSourceVertices = new LinkedHashSet<>(sourceVertices);
        for (LineageEdge lineageEdge : lineageEdges) {
            graphSourceVertices.add(lineageEdge.source());
        }

        Set<DatasetKey> graphSourceDatasets = new LinkedHashSet<>();
        for (SourceLineageVertex sourceVertex : graphSourceVertices) {
            if (sourceVertex == null || sourceVertex.datasets() == null) {
                continue;
            }
            for (LineageDataset dataset : sourceVertex.datasets()) {
                if (dataset != null && !isBlank(dataset.namespace()) && !isBlank(dataset.name())) {
                    graphSourceDatasets.add(DatasetKey.of(dataset));
                }
            }
        }
        return graphSourceDatasets;
    }

    private static void validateGraphSinkDataset(
            Set<DatasetKey> graphSinkDatasets, LineageDataset outputDataset, String outputField) {
        if (!graphSinkDatasets.contains(DatasetKey.of(outputDataset))) {
            throw invalidColumnLineage(
                    outputDataset, outputField, "output dataset does not belong to a graph sink");
        }
    }

    private static void validateRelation(
            ColumnLineageRelation relation,
            LineageDataset outputDataset,
            String outputField,
            Set<DatasetKey> graphSourceDatasets) {
        List<ColumnLineageInput> inputs = relation.inputs();
        if (inputs == null) {
            throw invalidColumnLineage(outputDataset, outputField, "inputs must not be null");
        }
        ColumnLineageOrigin origin = relation.origin();
        if (origin == null) {
            throw invalidColumnLineage(outputDataset, outputField, "origin must not be null");
        }

        boolean hasDirectInput = false;
        for (ColumnLineageInput input : inputs) {
            if (input == null) {
                throw invalidColumnLineage(
                        outputDataset, outputField, "input field must not be null");
            }
            validateDatasetIdentity(
                    input.inputDataset(),
                    outputDataset,
                    outputField,
                    "input dataset must have a stable dataset identity");
            validateField(
                    input.inputField(),
                    outputDataset,
                    outputField,
                    "input field must not be blank");
            if (!graphSourceDatasets.contains(DatasetKey.of(input.inputDataset()))) {
                throw invalidColumnLineage(
                        outputDataset,
                        outputField,
                        "input dataset does not belong to a graph source");
            }
            ColumnLineageDependencyType dependencyType = input.dependencyType();
            if (dependencyType == null) {
                throw invalidColumnLineage(
                        outputDataset, outputField, "input dependency type must not be null");
            }
            hasDirectInput |= dependencyType == ColumnLineageDependencyType.DIRECT;
        }

        if (origin == ColumnLineageOrigin.INPUT_FIELDS && !hasDirectInput) {
            throw invalidColumnLineage(
                    outputDataset,
                    outputField,
                    "INPUT_FIELDS requires at least one DIRECT input field");
        }
        if (origin != ColumnLineageOrigin.INPUT_FIELDS && hasDirectInput) {
            throw invalidColumnLineage(
                    outputDataset, outputField, origin + " inputs must be INDIRECT");
        }
    }

    private static void validateDatasetIdentity(
            LineageDataset dataset,
            LineageDataset outputDataset,
            String outputField,
            String reason) {
        if (dataset == null || isBlank(dataset.namespace()) || isBlank(dataset.name())) {
            throw invalidColumnLineage(outputDataset, outputField, reason);
        }
    }

    private static void validateField(
            String field, LineageDataset outputDataset, String outputField, String reason) {
        if (isBlank(field)) {
            throw invalidColumnLineage(outputDataset, outputField, reason);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static IllegalArgumentException invalidColumnLineage(
            LineageDataset outputDataset, String outputField, String reason) {
        String namespace = outputDataset == null ? "<unknown>" : outputDataset.namespace();
        String name = outputDataset == null ? "<unknown>" : outputDataset.name();
        return invalidColumnLineage(namespace, name, outputField, reason);
    }

    private static IllegalArgumentException invalidColumnLineage(
            String namespace, String name, String outputField, String reason) {
        String field = outputField == null ? "<unknown>" : outputField;
        return new IllegalArgumentException(
                String.format(
                        "Invalid column lineage for sink '%s.%s', field '%s': %s",
                        namespace, name, field, reason));
    }

    private static final class ExpectedOutputField {
        private final LineageDataset outputDataset;
        private final String outputField;

        private ExpectedOutputField(LineageDataset outputDataset, String outputField) {
            this.outputDataset = outputDataset;
            this.outputField = outputField;
        }
    }

    private static final class OutputFieldKey {
        private final String namespace;
        private final String name;
        private final String field;

        private OutputFieldKey(String namespace, String name, String field) {
            this.namespace = namespace;
            this.name = name;
            this.field = field;
        }

        private static OutputFieldKey of(LineageDataset dataset, String field) {
            return new OutputFieldKey(dataset.namespace(), dataset.name(), field);
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
            return namespace.equals(that.namespace)
                    && name.equals(that.name)
                    && field.equals(that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, name, field);
        }
    }

    private static final class DatasetKey {
        private final String namespace;
        private final String name;

        private DatasetKey(String namespace, String name) {
            this.namespace = namespace;
            this.name = name;
        }

        private static DatasetKey of(LineageDataset dataset) {
            return new DatasetKey(dataset.namespace(), dataset.name());
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
            return namespace.equals(that.namespace) && name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, name);
        }
    }
}

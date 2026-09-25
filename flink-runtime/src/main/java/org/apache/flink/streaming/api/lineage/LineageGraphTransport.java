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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Versioned, runtime-neutral transport for a lineage graph. */
@Internal
public final class LineageGraphTransport {
    public static final int FORMAT_VERSION = 1;
    public static final String CONFIG_KEY = "internal.lineage.graph";

    /** Dataset facet containing connector-safe table metadata for remote consumers. */
    public static final String TABLE_METADATA_FACET = "internal.lineage.table-metadata";

    private static final ObjectMapper MAPPER = JacksonMapperFactory.createObjectMapper();

    private LineageGraphTransport() {}

    public static String serialize(LineageGraph graph) {
        Objects.requireNonNull(graph, "graph");
        try {
            final ObjectNode root = MAPPER.createObjectNode();
            root.put("formatVersion", FORMAT_VERSION);
            final DatasetRegistry datasets = DatasetRegistry.from(graph);
            root.set("datasets", datasets.toJson(MAPPER));

            final ArrayNode sources = root.putArray("sources");
            for (SourceLineageVertex source : graph.sources()) {
                final ObjectNode sourceNode = sources.addObject();
                sourceNode.put("boundedness", source.boundedness().name());
                writeDatasetReferences(
                        sourceNode.putArray("datasets"), source.datasets(), datasets);
            }

            final ArrayNode sinks = root.putArray("sinks");
            for (LineageVertex sink : graph.sinks()) {
                final ObjectNode sinkNode = sinks.addObject();
                writeDatasetReferences(sinkNode.putArray("datasets"), sink.datasets(), datasets);
            }

            final ArrayNode edges = root.putArray("relations");
            for (LineageEdge edge : graph.relations()) {
                final ObjectNode edgeNode = edges.addObject();
                edgeNode.put("source", indexOf(graph.sources(), edge.source()));
                edgeNode.put("sink", indexOf(graph.sinks(), edge.sink()));
            }

            final ArrayNode columns = root.putArray("columnRelations");
            for (ColumnLineageRelation relation : graph.columnRelations()) {
                final ObjectNode relationNode = columns.addObject();
                relationNode.put("outputDataset", datasets.idOf(relation.outputDataset()));
                relationNode.put("outputField", relation.outputField());
                relationNode.put("origin", relation.origin().name());
                relation.transformation()
                        .ifPresent(value -> relationNode.put("transformation", value));
                final ArrayNode inputs = relationNode.putArray("inputs");
                for (ColumnLineageInput input : relation.inputs()) {
                    final ObjectNode inputNode = inputs.addObject();
                    inputNode.put("dataset", datasets.idOf(input.inputDataset()));
                    inputNode.put("field", input.inputField());
                    inputNode.put("dependencyType", input.dependencyType().name());
                }
            }

            if (graph instanceof LineageGraphObservation) {
                final LineageGraphObservation observation = (LineageGraphObservation) graph;
                root.put("tableStatus", observation.getTableStatus());
                root.put("columnStatus", observation.getColumnStatus());
                root.set("issues", MAPPER.valueToTree(observation.getIssues()));
                root.set("columnStatuses", MAPPER.valueToTree(observation.getColumnStatuses()));
                root.set("tableStatuses", MAPPER.valueToTree(observation.getTableStatuses()));
            }
            return MAPPER.writeValueAsString(root);
        } catch (JsonProcessingException | RuntimeException e) {
            throw new LineageGraphTransportException("Could not serialize lineage graph", e);
        }
    }

    public static LineageGraph deserialize(String payload) {
        if (payload == null || payload.trim().isEmpty()) {
            throw new LineageGraphTransportException("Lineage graph payload is empty");
        }
        try {
            final JsonNode root = MAPPER.readTree(payload);
            final int version = root.path("formatVersion").asInt(-1);
            if (version != FORMAT_VERSION) {
                throw new LineageGraphTransportException(
                        "Unsupported lineage graph format version: " + version);
            }
            final List<LineageDataset> datasets = readDatasets(root.path("datasets"));
            final List<SourceLineageVertex> sources = readSources(root.path("sources"), datasets);
            final List<LineageVertex> sinks = readSinks(root.path("sinks"), datasets);
            final DefaultLineageGraph.LineageGraphBuilder builder = DefaultLineageGraph.builder();
            builder.addSourceVertexes(sources.toArray(new SourceLineageVertex[0]));
            builder.addSinkVertexes(sinks);
            for (JsonNode edge : requiredArray(root, "relations")) {
                builder.addLineageEdge(
                        new DefaultLineageEdge(
                                sourceAt(sources, edge.path("source").asInt(-1)),
                                sinkAt(sinks, edge.path("sink").asInt(-1))));
            }
            for (JsonNode relation : requiredArray(root, "columnRelations")) {
                final LineageDataset outputDataset =
                        datasetAt(datasets, relation.path("outputDataset").asInt(-1));
                final String outputField = requiredText(relation, "outputField");
                builder.addExpectedOutputField(outputDataset, outputField);
                final List<ColumnLineageInput> inputs = new ArrayList<>();
                for (JsonNode input : requiredArray(relation, "inputs")) {
                    inputs.add(
                            new DefaultColumnLineageInput(
                                    datasetAt(datasets, input.path("dataset").asInt(-1)),
                                    requiredText(input, "field"),
                                    enumValue(
                                            ColumnLineageDependencyType.class,
                                            requiredText(input, "dependencyType"))));
                }
                builder.addColumnLineageRelation(
                        new DefaultColumnLineageRelation(
                                outputDataset,
                                outputField,
                                inputs,
                                enumValue(
                                        ColumnLineageOrigin.class,
                                        requiredText(relation, "origin")),
                                relation.has("transformation")
                                        ? requiredText(relation, "transformation")
                                        : null));
            }
            final LineageGraph graph = builder.build();
            if (!root.has("columnStatus")) {
                return graph;
            }
            return new LineageGraphObservation(
                    graph,
                    requiredText(root, "tableStatus"),
                    requiredText(root, "columnStatus"),
                    readStrings(root.path("issues")),
                    readStatuses(root.path("columnStatuses")),
                    readStatuses(root.path("tableStatuses")));
        } catch (LineageGraphTransportException e) {
            throw e;
        } catch (Exception e) {
            throw new LineageGraphTransportException("Could not deserialize lineage graph", e);
        }
    }

    private static void writeDatasetReferences(
            ArrayNode references, List<LineageDataset> values, DatasetRegistry datasets) {
        for (LineageDataset value : values) {
            references.add(datasets.idOf(value));
        }
    }

    private static int indexOf(List<? extends LineageVertex> vertices, LineageVertex target) {
        final int index = vertices.indexOf(target);
        if (index < 0) {
            throw new LineageGraphTransportException("Lineage edge references an unknown vertex");
        }
        return index;
    }

    private static List<LineageDataset> readDatasets(JsonNode node) {
        if (!node.isArray()) {
            throw new LineageGraphTransportException("Lineage graph datasets must be an array");
        }
        final List<LineageDataset> result = new ArrayList<>();
        for (JsonNode dataset : node) {
            final Map<String, LineageDatasetFacet> facets = new LinkedHashMap<>();
            for (JsonNode facet : dataset.path("facets")) {
                final String name = requiredText(facet, "name");
                facets.put(
                        name,
                        new TransportFacet(
                                name,
                                facet.has("payload") ? facet.get("payload").toString() : "null"));
            }
            if (dataset.has("tableMetadata")) {
                facets.put(
                        TABLE_METADATA_FACET,
                        new TransportFacet(
                                TABLE_METADATA_FACET, dataset.get("tableMetadata").toString()));
            }
            result.add(
                    new DefaultLineageDataset(
                            requiredText(dataset, "name"),
                            requiredText(dataset, "namespace"),
                            facets));
        }
        return result;
    }

    private static List<SourceLineageVertex> readSources(
            JsonNode node, List<LineageDataset> datasets) {
        if (!node.isArray()) {
            throw new LineageGraphTransportException("Lineage graph sources must be an array");
        }
        final List<SourceLineageVertex> result = new ArrayList<>();
        for (JsonNode source : node) {
            result.add(
                    new TransportSourceVertex(
                            readDatasetReferences(source.path("datasets"), datasets),
                            enumValue(Boundedness.class, requiredText(source, "boundedness"))));
        }
        return result;
    }

    private static List<LineageVertex> readSinks(JsonNode node, List<LineageDataset> datasets) {
        if (!node.isArray()) {
            throw new LineageGraphTransportException("Lineage graph sinks must be an array");
        }
        final List<LineageVertex> result = new ArrayList<>();
        for (JsonNode sink : node) {
            result.add(new TransportVertex(readDatasetReferences(sink.path("datasets"), datasets)));
        }
        return result;
    }

    private static List<LineageDataset> readDatasetReferences(
            JsonNode node, List<LineageDataset> datasets) {
        if (!node.isArray()) {
            throw new LineageGraphTransportException("Lineage vertex datasets must be an array");
        }
        final List<LineageDataset> result = new ArrayList<>();
        for (JsonNode id : node) {
            result.add(datasetAt(datasets, id.asInt(-1)));
        }
        return result;
    }

    private static ArrayNode requiredArray(JsonNode node, String field) {
        final JsonNode value = node.get(field);
        if (value == null || !value.isArray()) {
            throw new LineageGraphTransportException(
                    "Lineage graph field must be an array: " + field);
        }
        return (ArrayNode) value;
    }

    private static String requiredText(JsonNode node, String field) {
        final JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.asText().trim().isEmpty()) {
            throw new LineageGraphTransportException("Lineage graph field must be text: " + field);
        }
        return value.asText();
    }

    private static List<String> readStrings(JsonNode node) {
        if (!node.isArray()) {
            return Collections.emptyList();
        }
        final List<String> result = new ArrayList<>();
        node.forEach(value -> result.add(value.asText()));
        return result;
    }

    private static Map<String, Map<String, String>> readStatuses(JsonNode node) {
        if (!node.isObject()) {
            return Collections.emptyMap();
        }
        final Map<String, Map<String, String>> result = new LinkedHashMap<>();
        node.fields()
                .forEachRemaining(
                        entry -> {
                            final Map<String, String> statuses = new LinkedHashMap<>();
                            entry.getValue()
                                    .fields()
                                    .forEachRemaining(
                                            status ->
                                                    statuses.put(
                                                            status.getKey(),
                                                            status.getValue().asText()));
                            result.put(entry.getKey(), statuses);
                        });
        return result;
    }

    private static <E extends Enum<E>> E enumValue(Class<E> type, String value) {
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException e) {
            throw new LineageGraphTransportException("Unknown lineage enum value: " + value, e);
        }
    }

    private static LineageDataset datasetAt(List<LineageDataset> datasets, int index) {
        if (index < 0 || index >= datasets.size()) {
            throw new LineageGraphTransportException("Lineage graph references an unknown dataset");
        }
        return datasets.get(index);
    }

    private static SourceLineageVertex sourceAt(List<SourceLineageVertex> sources, int index) {
        if (index < 0 || index >= sources.size()) {
            throw new LineageGraphTransportException(
                    "Lineage graph references an unknown source vertex");
        }
        return sources.get(index);
    }

    private static LineageVertex sinkAt(List<LineageVertex> sinks, int index) {
        if (index < 0 || index >= sinks.size()) {
            throw new LineageGraphTransportException(
                    "Lineage graph references an unknown sink vertex");
        }
        return sinks.get(index);
    }

    private static final class TransportSourceVertex implements SourceLineageVertex {
        private final List<LineageDataset> datasets;
        private final Boundedness boundedness;

        private TransportSourceVertex(List<LineageDataset> datasets, Boundedness boundedness) {
            this.datasets = List.copyOf(datasets);
            this.boundedness = boundedness;
        }

        @Override
        public List<LineageDataset> datasets() {
            return datasets;
        }

        @Override
        public Boundedness boundedness() {
            return boundedness;
        }
    }

    private static final class TransportVertex implements LineageVertex {
        private final List<LineageDataset> datasets;

        private TransportVertex(List<LineageDataset> datasets) {
            this.datasets = List.copyOf(datasets);
        }

        @Override
        public List<LineageDataset> datasets() {
            return datasets;
        }
    }

    private static final class TransportFacet implements LineageDatasetFacetPayload {
        private final String name;
        private final String payload;

        private TransportFacet(String name, String payload) {
            this.name = name;
            this.payload = payload;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String payload() {
            return payload;
        }
    }

    private static final class DatasetRegistry {
        private final List<LineageDataset> datasets;
        private final Map<DatasetKey, Integer> ids;

        private DatasetRegistry(List<LineageDataset> datasets, Map<DatasetKey, Integer> ids) {
            this.datasets = datasets;
            this.ids = ids;
        }

        private static DatasetRegistry from(LineageGraph graph) {
            final List<LineageDataset> datasets = new ArrayList<>();
            final Map<DatasetKey, Integer> ids = new LinkedHashMap<>();
            graph.sources()
                    .forEach(
                            source ->
                                    source.datasets()
                                            .forEach(dataset -> add(dataset, datasets, ids)));
            graph.sinks()
                    .forEach(
                            sink ->
                                    sink.datasets()
                                            .forEach(dataset -> add(dataset, datasets, ids)));
            graph.columnRelations()
                    .forEach(
                            relation -> {
                                add(relation.outputDataset(), datasets, ids);
                                relation.inputs()
                                        .forEach(input -> add(input.inputDataset(), datasets, ids));
                            });
            graph.relations()
                    .forEach(
                            edge -> {
                                edge.source()
                                        .datasets()
                                        .forEach(dataset -> add(dataset, datasets, ids));
                                edge.sink()
                                        .datasets()
                                        .forEach(dataset -> add(dataset, datasets, ids));
                            });
            return new DatasetRegistry(datasets, ids);
        }

        private static void add(
                LineageDataset dataset,
                List<LineageDataset> datasets,
                Map<DatasetKey, Integer> ids) {
            ids.computeIfAbsent(
                    new DatasetKey(dataset.namespace(), dataset.name()),
                    ignored -> {
                        datasets.add(dataset);
                        return datasets.size() - 1;
                    });
        }

        private int idOf(LineageDataset dataset) {
            final Integer id = ids.get(new DatasetKey(dataset.namespace(), dataset.name()));
            if (id == null) {
                throw new LineageGraphTransportException(
                        "Lineage graph references an unknown dataset");
            }
            return id;
        }

        private ArrayNode toJson(ObjectMapper mapper) {
            final ArrayNode result = mapper.createArrayNode();
            for (LineageDataset dataset : datasets) {
                final ObjectNode value = result.addObject();
                value.put("name", dataset.name());
                value.put("namespace", dataset.namespace());
                tableMetadata(dataset).ifPresent(metadata -> value.set("tableMetadata", metadata));
                final ArrayNode facets = value.putArray("facets");
                dataset.facets()
                        .forEach(
                                (name, facet) -> {
                                    final ObjectNode facetNode = facets.addObject();
                                    facetNode.put("name", name);
                                    facetNode.put("className", facet.getClass().getName());
                                    try {
                                        if (facet instanceof LineageDatasetFacetPayload) {
                                            facetNode.set(
                                                    "payload",
                                                    mapper.readTree(
                                                            ((LineageDatasetFacetPayload) facet)
                                                                    .payload()));
                                        } else {
                                            facetNode.set("payload", mapper.valueToTree(facet));
                                        }
                                    } catch (IllegalArgumentException ignored) {
                                        facetNode.putNull("payload");
                                    } catch (JsonProcessingException ignored) {
                                        facetNode.putNull("payload");
                                    }
                                });
            }
            return result;
        }

        private static Optional<ObjectNode> tableMetadata(LineageDataset dataset) {
            try {
                final Method tableMethod = dataset.getClass().getMethod("table");
                final Object table = tableMethod.invoke(dataset);
                if (table == null) {
                    return Optional.empty();
                }
                final Object options = table.getClass().getMethod("getOptions").invoke(table);
                if (!(options instanceof Map)) {
                    return Optional.empty();
                }
                final ObjectNode metadata = MAPPER.createObjectNode();
                metadata.set("options", MAPPER.valueToTree(options));
                final Object kind = table.getClass().getMethod("getTableKind").invoke(table);
                if (kind != null) {
                    metadata.put("tableKind", kind.toString());
                }
                final Object comment = table.getClass().getMethod("getComment").invoke(table);
                if (comment != null) {
                    metadata.put("comment", comment.toString());
                }
                try {
                    final Object fields =
                            dataset.getClass().getMethod("fieldNames").invoke(dataset);
                    if (fields instanceof List) {
                        metadata.set("fieldNames", MAPPER.valueToTree(fields));
                    }
                } catch (ReflectiveOperationException ignored) {
                    // Field names are optional metadata; connector options remain authoritative.
                }
                return Optional.of(metadata);
            } catch (ReflectiveOperationException | IllegalArgumentException ignored) {
                return Optional.empty();
            }
        }
    }

    private static final class DatasetKey {
        private final String namespace;
        private final String name;

        private DatasetKey(String namespace, String name) {
            this.namespace = namespace;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof DatasetKey)) {
                return false;
            }
            DatasetKey that = (DatasetKey) o;
            return Objects.equals(namespace, that.namespace) && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, name);
        }
    }
}

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

import org.apache.flink.api.connector.source.Boundedness;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testing for lineage graph. */
class DefaultLineageGraphTest {
    @Test
    void testLineageGraph() {
        SourceLineageVertex source1 = new TestingSourceLineageVertex("source1");
        SourceLineageVertex source2 = new TestingSourceLineageVertex("source2");
        SourceLineageVertex source3 = new TestingSourceLineageVertex("source3");
        LineageVertex sink1 = new TestingLineageVertex("sink1");
        LineageVertex sink2 = new TestingLineageVertex("sink2");
        LineageGraph lineageGraph =
                DefaultLineageGraph.builder()
                        .addLineageEdge(new TestingLineageEdge(source1, sink1))
                        .addLineageEdges(
                                new TestingLineageEdge(source2, sink2),
                                new TestingLineageEdge(source3, sink1),
                                new TestingLineageEdge(source1, sink2))
                        .build();
        assertThat(lineageGraph.sources()).containsExactlyInAnyOrder(source1, source2, source3);
        assertThat(lineageGraph.sinks()).containsExactlyInAnyOrder(sink1, sink2);
        assertThat(lineageGraph.relations()).hasSize(4);
        assertThat(lineageGraph.columnRelations()).isEmpty();
    }

    @Test
    void testThirdPartyLineageGraphDefaultsColumnRelationsToEmpty() {
        LineageGraph lineageGraph =
                new LineageGraph() {
                    @Override
                    public List<SourceLineageVertex> sources() {
                        return Collections.emptyList();
                    }

                    @Override
                    public List<LineageVertex> sinks() {
                        return Collections.emptyList();
                    }

                    @Override
                    public List<LineageEdge> relations() {
                        return Collections.emptyList();
                    }
                };

        assertThat(lineageGraph.columnRelations()).isEmpty();
    }

    @Test
    void testColumnLineageGraph() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageInput input =
                new DefaultColumnLineageInput(
                        source, "customer_id", ColumnLineageDependencyType.DIRECT);
        ColumnLineageInput controlInput =
                new DefaultColumnLineageInput(
                        source, "enabled", ColumnLineageDependencyType.INDIRECT);
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sink,
                        "customer_id",
                        List.of(input, controlInput),
                        ColumnLineageOrigin.INPUT_FIELDS,
                        "CAST");

        LineageGraph lineageGraph =
                DefaultLineageGraph.builder()
                        .addLineageEdge(
                                new TestingLineageEdge(
                                        new TestingSourceLineageVertex("source", source),
                                        new TestingLineageVertex("sink", sink)))
                        .addExpectedOutputField(sink, "customer_id")
                        .addColumnLineageRelation(relation)
                        .build();

        assertThat(lineageGraph.columnRelations()).containsExactly(relation);
        assertThat(relation.outputDataset()).isSameAs(sink);
        assertThat(relation.outputField()).isEqualTo("customer_id");
        assertThat(relation.inputs()).containsExactly(input, controlInput);
        assertThat(relation.origin()).isEqualTo(ColumnLineageOrigin.INPUT_FIELDS);
        assertThat(relation.transformation()).contains("CAST");
        assertThat(input.inputDataset()).isSameAs(source);
        assertThat(input.inputField()).isEqualTo("customer_id");
        assertThat(input.dependencyType()).isEqualTo(ColumnLineageDependencyType.DIRECT);
        assertThatThrownBy(() -> lineageGraph.columnRelations().add(relation))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> relation.inputs().add(input))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testRejectsMissingOutputRelation() {
        LineageDataset sink = dataset("orders", "warehouse");

        assertThatThrownBy(
                        () ->
                                lineageBuilder(sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("missing output relation");
    }

    @Test
    void testRejectsDuplicateOutputRelation() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        LineageDataset equivalentSink = dataset("orders", "warehouse");
        ColumnLineageRelation firstRelation = inputRelation(source, "id", sink, "customer_id");
        ColumnLineageRelation secondRelation =
                inputRelation(source, "id", equivalentSink, "customer_id");

        assertThatThrownBy(
                        () ->
                                lineageBuilder(source, sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelations(firstRelation, secondRelation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("duplicate output relation");
    }

    @Test
    void testRejectsOutputWithoutStableDatasetIdentity() {
        LineageDataset sink = dataset(" ", "warehouse");
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sink,
                        "customer_id",
                        Collections.emptyList(),
                        ColumnLineageOrigin.CONSTANT,
                        null);

        assertThatThrownBy(
                        () ->
                                lineageBuilder(sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("stable dataset identity");
    }

    @Test
    void testRejectsUnexpectedOutputRelation() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageRelation relation = inputRelation(source, "id", sink, "unexpected");

        assertThatThrownBy(
                        () ->
                                lineageBuilder(sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'unexpected'")
                .hasMessageContaining("unexpected output relation");
    }

    @Test
    void testRejectsBlankOutputField() {
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sink, " ", Collections.emptyList(), ColumnLineageOrigin.CONSTANT, null);

        assertThatThrownBy(
                        () ->
                                lineageBuilder(sink)
                                        .addExpectedOutputField(sink, " ")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field ' '")
                .hasMessageContaining("output field must not be blank");
    }

    @Test
    void testRejectsInputFieldsOriginWithoutInputs() {
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sink,
                        "customer_id",
                        Collections.emptyList(),
                        ColumnLineageOrigin.INPUT_FIELDS,
                        null);

        assertThatThrownBy(
                        () ->
                                lineageBuilder(sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("INPUT_FIELDS requires at least one DIRECT input field");
    }

    @Test
    void testRejectsInputFieldsOriginWithoutDirectInput() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageInput input =
                new DefaultColumnLineageInput(source, "id", ColumnLineageDependencyType.INDIRECT);
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sink,
                        "customer_id",
                        Collections.singletonList(input),
                        ColumnLineageOrigin.INPUT_FIELDS,
                        null);

        assertThatThrownBy(
                        () ->
                                lineageBuilder(source, sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("INPUT_FIELDS requires at least one DIRECT input field");
    }

    @Test
    void testAllowsNonInputOriginsWithoutInputs() {
        LineageDataset sink = dataset("orders", "warehouse");

        for (ColumnLineageOrigin origin :
                new ColumnLineageOrigin[] {
                    ColumnLineageOrigin.CONSTANT, ColumnLineageOrigin.SYSTEM
                }) {
            ColumnLineageRelation relation =
                    new DefaultColumnLineageRelation(
                            sink, "generated_id", Collections.emptyList(), origin, null);

            LineageGraph graph =
                    lineageBuilder(sink)
                            .addExpectedOutputField(sink, "generated_id")
                            .addColumnLineageRelation(relation)
                            .build();

            assertThat(graph.columnRelations()).containsExactly(relation);
        }
    }

    @Test
    void testAllowsNonInputOriginsWithIndirectInputs() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageInput input =
                new DefaultColumnLineageInput(
                        source, "enabled", ColumnLineageDependencyType.INDIRECT);

        for (ColumnLineageOrigin origin :
                new ColumnLineageOrigin[] {
                    ColumnLineageOrigin.CONSTANT, ColumnLineageOrigin.SYSTEM
                }) {
            ColumnLineageRelation relation =
                    new DefaultColumnLineageRelation(
                            sink, "generated_id", Collections.singletonList(input), origin, null);

            LineageGraph graph =
                    lineageBuilder(source, sink)
                            .addExpectedOutputField(sink, "generated_id")
                            .addColumnLineageRelation(relation)
                            .build();

            assertThat(graph.columnRelations()).containsExactly(relation);
        }
    }

    @Test
    void testRejectsNonInputOriginsWithDirectInputs() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageInput input =
                new DefaultColumnLineageInput(source, "id", ColumnLineageDependencyType.DIRECT);

        for (ColumnLineageOrigin origin :
                new ColumnLineageOrigin[] {
                    ColumnLineageOrigin.CONSTANT, ColumnLineageOrigin.SYSTEM
                }) {
            ColumnLineageRelation relation =
                    new DefaultColumnLineageRelation(
                            sink, "generated_id", Collections.singletonList(input), origin, null);

            assertThatThrownBy(
                            () ->
                                    lineageBuilder(source, sink)
                                            .addExpectedOutputField(sink, "generated_id")
                                            .addColumnLineageRelation(relation)
                                            .build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sink 'warehouse.orders'")
                    .hasMessageContaining("field 'generated_id'")
                    .hasMessageContaining(origin + " inputs must be INDIRECT");
        }
    }

    @Test
    void testRejectsInputWithoutStableDatasetIdentityOrField() {
        LineageDataset source = dataset("customers", " ");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageRelation relation = inputRelation(source, " ", sink, "customer_id");

        assertThatThrownBy(
                        () ->
                                lineageBuilder(source, sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("input dataset must have a stable dataset identity");
    }

    @Test
    void testRejectsBlankInputField() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageRelation relation = inputRelation(source, " ", sink, "customer_id");

        assertThatThrownBy(
                        () ->
                                lineageBuilder(sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("input field must not be blank");
    }

    @Test
    void testRejectsInputWithoutDependencyType() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageInput input = new DefaultColumnLineageInput(source, "id", null);
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sink,
                        "customer_id",
                        Collections.singletonList(input),
                        ColumnLineageOrigin.INPUT_FIELDS,
                        null);

        assertThatThrownBy(
                        () ->
                                lineageBuilder(source, sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("input dependency type must not be null");
    }

    @Test
    void testCopiesColumnLineageCollections() {
        LineageDataset source = dataset("customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        List<ColumnLineageInput> inputs = new ArrayList<>();
        inputs.add(new DefaultColumnLineageInput(source, "id", ColumnLineageDependencyType.DIRECT));
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sink, "customer_id", inputs, ColumnLineageOrigin.INPUT_FIELDS, null);

        inputs.clear();

        LineageGraph lineageGraph =
                lineageBuilder(source, sink)
                        .addExpectedOutputField(sink, "customer_id")
                        .addColumnLineageRelation(relation)
                        .build();

        assertThat(relation.inputs()).hasSize(1);
        assertThat(lineageGraph.columnRelations()).containsExactly(relation);
        assertThat(relation.transformation()).isEqualTo(Optional.empty());
    }

    @Test
    void testRejectsInputDatasetThatIsNotGraphSource() {
        LineageDataset graphSource = dataset("customers", "warehouse");
        LineageDataset unrelatedSource = dataset("audit_customers", "warehouse");
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageRelation relation = inputRelation(unrelatedSource, "id", sink, "customer_id");

        assertThatThrownBy(
                        () ->
                                lineageBuilder(graphSource, sink)
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("input dataset does not belong to a graph source");
    }

    @Test
    void testRejectsColumnLineageForDatasetThatIsNotGraphSink() {
        LineageDataset graphSink = dataset("orders", "warehouse");
        LineageDataset unrelatedSink = dataset("audit_orders", "warehouse");
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        unrelatedSink,
                        "customer_id",
                        Collections.emptyList(),
                        ColumnLineageOrigin.CONSTANT,
                        null);

        assertThatThrownBy(
                        () ->
                                lineageBuilder(graphSink)
                                        .addExpectedOutputField(unrelatedSink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.audit_orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("does not belong to a graph sink");
    }

    @Test
    void testRejectsColumnLineageWhenGraphHasNoSinkDataset() {
        LineageDataset sink = dataset("orders", "warehouse");
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sink,
                        "customer_id",
                        Collections.emptyList(),
                        ColumnLineageOrigin.CONSTANT,
                        null);

        assertThatThrownBy(
                        () ->
                                DefaultLineageGraph.builder()
                                        .addExpectedOutputField(sink, "customer_id")
                                        .addColumnLineageRelation(relation)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sink 'warehouse.orders'")
                .hasMessageContaining("field 'customer_id'")
                .hasMessageContaining("does not belong to a graph sink");
    }

    private static ColumnLineageRelation inputRelation(
            LineageDataset source, String inputField, LineageDataset sink, String outputField) {
        ColumnLineageInput input =
                new DefaultColumnLineageInput(
                        source, inputField, ColumnLineageDependencyType.DIRECT);
        return new DefaultColumnLineageRelation(
                sink,
                outputField,
                Collections.singletonList(input),
                ColumnLineageOrigin.INPUT_FIELDS,
                null);
    }

    private static LineageDataset dataset(String name, String namespace) {
        return new DefaultLineageDataset(name, namespace, Collections.emptyMap());
    }

    private static DefaultLineageGraph.LineageGraphBuilder lineageBuilder(
            LineageDataset sinkDataset) {
        return DefaultLineageGraph.builder()
                .addSinkVertex(new TestingLineageVertex("sink", sinkDataset));
    }

    private static DefaultLineageGraph.LineageGraphBuilder lineageBuilder(
            LineageDataset sourceDataset, LineageDataset sinkDataset) {
        return DefaultLineageGraph.builder()
                .addSourceVertex(new TestingSourceLineageVertex("source", sourceDataset))
                .addSinkVertex(new TestingLineageVertex("sink", sinkDataset));
    }

    /** Testing sink lineage vertex. */
    static class TestingLineageVertex implements LineageVertex {
        private final String id;
        private final List<LineageDataset> datasets;

        private TestingLineageVertex(String id) {
            this(id, Collections.emptyList());
        }

        private TestingLineageVertex(String id, LineageDataset dataset) {
            this(id, Collections.singletonList(dataset));
        }

        private TestingLineageVertex(String id, List<LineageDataset> datasets) {
            this.id = id;
            this.datasets = List.copyOf(datasets);
        }

        private String id() {
            return id;
        }

        @Override
        public List<LineageDataset> datasets() {
            return datasets;
        }
    }

    /** Testing source lineage vertex. */
    static class TestingSourceLineageVertex extends TestingLineageVertex
            implements SourceLineageVertex {

        private TestingSourceLineageVertex(String id) {
            super(id);
        }

        private TestingSourceLineageVertex(String id, LineageDataset dataset) {
            super(id, dataset);
        }

        @Override
        public Boundedness boundedness() {
            return Boundedness.BOUNDED;
        }
    }

    /** Testing lineage edge. */
    static class TestingLineageEdge implements LineageEdge {
        private final SourceLineageVertex source;
        private final LineageVertex sink;

        private TestingLineageEdge(SourceLineageVertex source, LineageVertex sink) {
            this.source = source;
            this.sink = sink;
        }

        @Override
        public SourceLineageVertex source() {
            return source;
        }

        @Override
        public LineageVertex sink() {
            return sink;
        }
    }
}

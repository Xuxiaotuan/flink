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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LineageGraphTransportTest {

    @Test
    void roundTripPreservesColumnRelationsAndDatasetIdentities() throws Exception {
        final LineageDataset sourceDataset =
                new DefaultLineageDataset(
                        "orders",
                        "catalog://warehouse",
                        Map.of("schema", new TestingFacet("schema")));
        final LineageDataset sinkDataset =
                new DefaultLineageDataset(
                        "order_summary",
                        "catalog://warehouse",
                        Map.of("schema", new TestingFacet("schema")));
        final SourceLineageVertex source =
                new TestingSourceVertex(List.of(sourceDataset), Boundedness.BOUNDED);
        final LineageVertex sink = new TestingVertex(List.of(sinkDataset));
        final ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sinkDataset,
                        "total",
                        List.of(
                                new DefaultColumnLineageInput(
                                        sourceDataset,
                                        "amount",
                                        ColumnLineageDependencyType.DIRECT),
                                new DefaultColumnLineageInput(
                                        sourceDataset,
                                        "region",
                                        ColumnLineageDependencyType.INDIRECT)),
                        ColumnLineageOrigin.INPUT_FIELDS,
                        "sum(amount) grouped by region");
        final LineageGraph graph =
                DefaultLineageGraph.builder()
                        .addLineageEdge(new DefaultLineageEdge(source, sink))
                        .addExpectedOutputField(sinkDataset, "total")
                        .addColumnLineageRelation(relation)
                        .build();

        final LineageGraph restored =
                LineageGraphTransport.deserialize(LineageGraphTransport.serialize(graph));

        assertThat(restored.sources())
                .singleElement()
                .satisfies(v -> assertThat(v.boundedness()).isEqualTo(Boundedness.BOUNDED));
        assertThat(restored.relations()).hasSize(1);
        assertThat(restored.columnRelations())
                .singleElement()
                .satisfies(
                        r -> {
                            assertThat(r.outputDataset().name()).isEqualTo("order_summary");
                            assertThat(r.outputDataset().namespace())
                                    .isEqualTo("catalog://warehouse");
                            assertThat(r.outputField()).isEqualTo("total");
                            assertThat(r.inputs())
                                    .extracting(ColumnLineageInput::inputField)
                                    .containsExactly("amount", "region");
                            assertThat(r.inputs())
                                    .extracting(ColumnLineageInput::dependencyType)
                                    .containsExactly(
                                            ColumnLineageDependencyType.DIRECT,
                                            ColumnLineageDependencyType.INDIRECT);
                            assertThat(r.origin()).isEqualTo(ColumnLineageOrigin.INPUT_FIELDS);
                            assertThat(r.transformation())
                                    .contains("sum(amount) grouped by region");
                        });
        assertThat(restored.sources().get(0).datasets().get(0).facets()).containsKey("schema");
    }

    @Test
    void unknownFormatVersionIsRejected() {
        assertThatThrownBy(
                        () ->
                                LineageGraphTransport.deserialize(
                                        "{\"formatVersion\":999,\"sources\":[],\"sinks\":[],\"relations\":[],\"columnRelations\":[]}"))
                .isInstanceOf(LineageGraphTransportException.class)
                .hasMessageContaining("Unsupported lineage graph format version");
    }

    @Test
    void malformedPayloadIsRejected() {
        assertThatThrownBy(() -> LineageGraphTransport.deserialize("{"))
                .isInstanceOf(LineageGraphTransportException.class);
    }

    private static final class TestingFacet implements LineageDatasetFacet {
        private final String name;

        private TestingFacet(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }
    }

    private static final class TestingSourceVertex implements SourceLineageVertex {
        private final List<LineageDataset> datasets;
        private final Boundedness boundedness;

        private TestingSourceVertex(List<LineageDataset> datasets, Boundedness boundedness) {
            this.datasets = datasets;
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

    private static final class TestingVertex implements LineageVertex {
        private final List<LineageDataset> datasets;

        private TestingVertex(List<LineageDataset> datasets) {
            this.datasets = datasets;
        }

        @Override
        public List<LineageDataset> datasets() {
            return datasets;
        }
    }
}

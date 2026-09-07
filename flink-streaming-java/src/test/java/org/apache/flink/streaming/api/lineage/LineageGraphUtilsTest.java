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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.transformations.GlobalCommitterTransform;
import org.apache.flink.streaming.api.transformations.TransformationWithLineage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Testing for lineage graph util. */
class LineageGraphUtilsTest {
    @Test
    void tableCoverageTracksEveryWriterAndPreservesIndependentCompleteOutputs() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(
                        new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "source");
        DataStreamSink<Long> good = annotatedSink(source, "good", "result");
        DataStreamSink<Long> shared = annotatedSink(source, "shared", "result");
        ((TransformationWithLineage<?>) good.getTransformation())
                .setTableLineage(new TransformationTableLineage(List.of()));
        ((TransformationWithLineage<?>) shared.getTransformation())
                .setTableLineage(new TransformationTableLineage(List.of()));
        DataStreamSink<Long> unknownWriter = source.sinkTo(new LineageSink());
        ((TransformationWithLineage<?>) unknownWriter.getTransformation())
                .setLineageVertex(lineageVertexOf(dataset("shared", SINK_DATASET_NAMESPACE)));
        String snapshot =
                env.getStreamGraph()
                        .getJobConfiguration()
                        .getString("internal.lineage.table-statuses", "{}");
        org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode json =
                org.apache.flink.util.jackson.JacksonMapperFactory.createObjectMapper()
                        .readTree(snapshot);
        assertThat(json.path(SINK_DATASET_NAMESPACE).path("good").asText()).isEqualTo("COMPLETE");
        assertThat(json.path(SINK_DATASET_NAMESPACE).path("shared").asText())
                .isEqualTo("UNAVAILABLE");
    }

    @Test
    void unknownWriterInvalidatesKnownColumnCoverageButPreservesInventory() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(
                        new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "source");
        DataStreamSink<Long> good = annotatedSink(source, "good", "result");
        ((TransformationWithLineage<?>) good.getTransformation())
                .setTableLineage(
                        new TransformationTableLineage(
                                LineageGraphUtils.convertToLineageGraph(
                                                List.of(good.getTransformation()))
                                        .sources()));
        java.util.concurrent.atomic.AtomicBoolean metadataUnavailable =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        LineageSink writer =
                new LineageSink() {
                    @Override
                    public LineageVertex getLineageVertex() {
                        if (metadataUnavailable.get()) {
                            throw new IllegalStateException(
                                    "Sink metadata temporarily unavailable");
                        }
                        return lineageVertexOf(dataset("good", SINK_DATASET_NAMESPACE));
                    }
                };
        DataStreamSink<Long> unknown = source.sinkTo(writer);
        LineageGraphObservation observation =
                LineageGraphUtils.observe(
                        List.of(good.getTransformation(), unknown.getTransformation()));
        metadataUnavailable.set(false);
        assertThat(writer.getLineageVertex().datasets())
                .extracting(LineageDataset::name)
                .containsExactly("good");
        assertThat(observation.getColumnStatus()).isEqualTo("UNAVAILABLE");
        assertThat(observation.getTableStatuses().get(SINK_DATASET_NAMESPACE))
                .containsEntry("good", "UNAVAILABLE");
        assertThat(observation.columnRelations()).isEmpty();
        assertThat(observation.sources()).isNotEmpty();
        assertThat(observation.sinks())
                .flatExtracting(LineageVertex::datasets)
                .extracting(LineageDataset::name)
                .containsExactly("good");
        assertThat(observation.getColumnStatuses().get(SINK_DATASET_NAMESPACE))
                .containsEntry("good", "UNAVAILABLE");
        assertThat(observation.getIssues()).isNotEmpty();
    }

    @Test
    void independentlyValidPartialWritesMergeWithoutDiscardingSeparateOutput() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(
                        new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "source");
        DataStreamSink<Long> left = annotatedSink(source, "shared", "left_result");
        DataStreamSink<Long> right = annotatedSink(source, "shared", "right_result");
        DataStreamSink<Long> other = annotatedSink(source, "other", "result");
        LineageGraphObservation observation =
                LineageGraphUtils.observe(
                        List.of(
                                left.getTransformation(),
                                right.getTransformation(),
                                other.getTransformation()));
        assertThat(observation.getColumnStatus()).isEqualTo("COMPLETE");
        assertThat(observation.columnRelations())
                .extracting(
                        relation -> relation.outputDataset().name() + "." + relation.outputField())
                .containsExactlyInAnyOrder(
                        "shared.left_result", "shared.right_result", "other.result");
    }

    private static DataStreamSink<Long> annotatedSink(
            DataStreamSource<Long> source, String name, String field) {
        DataStreamSink<Long> sink = source.sinkTo(new LineageSink());
        LineageDataset output = dataset(name, SINK_DATASET_NAMESPACE);
        ((TransformationWithLineage<?>) sink.getTransformation())
                .setLineageVertex(lineageVertexOf(output));
        setColumnLineage(
                sink,
                relation(
                        dataset(SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE),
                        "value",
                        ColumnLineageDependencyType.DIRECT,
                        output,
                        field,
                        ColumnLineageOrigin.INPUT_FIELDS,
                        null));
        return sink;
    }

    @Test
    void unavailableSinkDoesNotDiscardOtherDatasetColumns() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(
                        new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "source");
        DataStreamSink<Long> annotated = source.sinkTo(new LineageSink());
        DataStreamSink<Long> unavailable = source.sinkTo(new LineageSink());
        ((TransformationWithLineage<?>) unavailable.getTransformation())
                .setLineageVertex(lineageVertexOf(dataset("other", "sink")));
        unavailable.getTransformation().setLineageFailure("Unsupported column expression");
        setColumnLineage(
                annotated,
                relation(
                        dataset(SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE),
                        "value",
                        ColumnLineageDependencyType.DIRECT,
                        dataset(SINK_DATASET_NAME, SINK_DATASET_NAMESPACE),
                        "result",
                        ColumnLineageOrigin.INPUT_FIELDS,
                        null));

        LineageGraphObservation observation =
                LineageGraphUtils.observe(
                        List.of(annotated.getTransformation(), unavailable.getTransformation()));

        assertThat(
                        env.getStreamGraph()
                                .getJobConfiguration()
                                .getString("internal.lineage.column-statuses", "{}"))
                .contains("\"sink://LineageSink\":{\"LineageSink\":\"COMPLETE\"}")
                .contains("\"sink\":{\"other\":\"UNAVAILABLE\"}");
        assertThat(observation.getTableStatus()).isEqualTo("PARTIAL");
        assertThat(observation.getColumnStatus()).isEqualTo("PARTIAL");
        assertThat(observation.columnRelations())
                .extracting(relation -> relation.outputDataset().name())
                .containsExactly(SINK_DATASET_NAME);
    }

    @Test
    void sameDatasetWritersRequireColumnMetadataForEveryWriter() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(
                        new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "source");
        DataStreamSink<Long> annotated = source.sinkTo(new LineageSink());
        DataStreamSink<Long> unannotated = source.sinkTo(new LineageSink());
        setColumnLineage(
                annotated,
                relation(
                        dataset(SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE),
                        "value",
                        ColumnLineageDependencyType.DIRECT,
                        dataset(SINK_DATASET_NAME, SINK_DATASET_NAMESPACE),
                        "result",
                        ColumnLineageOrigin.INPUT_FIELDS,
                        null));

        LineageGraphObservation observation =
                LineageGraphUtils.observe(
                        List.of(annotated.getTransformation(), unannotated.getTransformation()));

        assertThat(observation.getTableStatus()).isEqualTo("PARTIAL");
        assertThat(observation.getColumnStatus()).isEqualTo("UNAVAILABLE");
        assertThat(observation.columnRelations()).isEmpty();
        assertThat(observation.relations()).hasSize(2);
        assertThat(observation.getIssues()).isNotEmpty();
    }

    @Test
    void failedColumnObservationRetainsKnownTableRelations() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSink<Long> sink =
                env.fromSource(
                                new LineageSource(1L, 5L),
                                WatermarkStrategy.noWatermarks(),
                                "source")
                        .sinkTo(new LineageSink());
        sink.getTransformation().setLineageFailure("Unsupported field expression");
        LineageGraph graph = env.getStreamGraph().getLineageGraph();
        assertThat(graph).isNotNull();
        assertThat(graph.relations()).hasSize(1);
        assertThat(graph.columnRelations()).isEmpty();
        assertThat(((LineageGraphObservation) graph).getTableStatus()).isEqualTo("PARTIAL");
        assertThat(((LineageGraphObservation) graph).getColumnStatus()).isEqualTo("UNAVAILABLE");
        assertThat(((LineageGraphObservation) graph).getIssues()).isNotEmpty();
    }

    @Test
    void sourceLineageCallbackFailureDoesNotPreventGraphCreation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(
                        new LineageSource(1L, 5L) {
                            @Override
                            public LineageVertex getLineageVertex() {
                                throw new IllegalStateException("lineage callback failed");
                            }
                        },
                        WatermarkStrategy.noWatermarks(),
                        "source")
                .sinkTo(new DiscardingSink<>());
        LineageGraphObservation observation =
                (LineageGraphObservation) env.getStreamGraph().getLineageGraph();
        assertThat(observation.getColumnStatus()).isEqualTo("UNAVAILABLE");
        assertThat(observation.getIssues()).isNotEmpty();
    }

    @Test
    void sinkLineageCallbackFailureDoesNotPreventGraphCreation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(new NumberSequenceSource(1L, 5L), WatermarkStrategy.noWatermarks(), "source")
                .sinkTo(
                        new LineageSink() {
                            @Override
                            public LineageVertex getLineageVertex() {
                                throw new IllegalStateException("lineage callback failed");
                            }
                        });
        LineageGraphObservation observation =
                (LineageGraphObservation) env.getStreamGraph().getLineageGraph();
        assertThat(observation.getColumnStatus()).isEqualTo("UNAVAILABLE");
        assertThat(observation.getIssues()).isNotEmpty();
    }

    private static final String SOURCE_DATASET_NAME = "LineageSource";
    private static final String SOURCE_DATASET_NAMESPACE = "source://LineageSource";
    private static final String SINK_DATASET_NAME = "LineageSink";
    private static final String SINK_DATASET_NAMESPACE = "sink://LineageSink";

    private static final String LEGACY_SOURCE_DATASET_NAME = "LineageSourceFunction";
    private static final String LEGACY_SOURCE_DATASET_NAMESPACE = "source://LineageSourceFunction";
    private static final String LEGACY_SINK_DATASET_NAME = "LineageSinkFunction";
    private static final String LEGACY_SINK_DATASET_NAMESPACE = "sink://LineageSinkFunction";

    @Test
    void testExtractLineageGraphFromLegacyTransformations() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source = env.addSource(new LineageSourceFunction());
        DataStreamSink<Long> sink = source.addSink(new LineageSinkFunction());

        LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(Arrays.asList(sink.getTransformation()));

        assertThat(lineageGraph.sources().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).boundedness())
                .isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
        assertThat(lineageGraph.sources().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).name())
                .isEqualTo(LEGACY_SOURCE_DATASET_NAME);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).namespace())
                .isEqualTo(LEGACY_SOURCE_DATASET_NAMESPACE);

        assertThat(lineageGraph.sinks().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).name())
                .isEqualTo(LEGACY_SINK_DATASET_NAME);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).namespace())
                .isEqualTo(LEGACY_SINK_DATASET_NAMESPACE);

        assertThat(lineageGraph.relations().size()).isEqualTo(1);
    }

    @Test
    void testExtractLineageGraphFromTransformations() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        DataStreamSink<Long> sink = source.sinkTo(new LineageSink());

        LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(Arrays.asList(sink.getTransformation()));

        assertThat(lineageGraph.sources().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).boundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(lineageGraph.sources().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).name())
                .isEqualTo(SOURCE_DATASET_NAME);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).namespace())
                .isEqualTo(SOURCE_DATASET_NAMESPACE);

        assertThat(lineageGraph.sinks().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).name())
                .isEqualTo(SINK_DATASET_NAME);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).namespace())
                .isEqualTo(SINK_DATASET_NAMESPACE);

        assertThat(lineageGraph.relations().size()).isEqualTo(1);
    }

    @Test
    void testExtractPartialLineageGraphWithSourceOnly() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        source.sinkTo(new DiscardingSink<>());

        LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(env.getTransformations());

        assertThat(lineageGraph.sources().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).boundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(lineageGraph.sources().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).name())
                .isEqualTo(SOURCE_DATASET_NAME);
        assertThat(lineageGraph.sources().get(0).datasets().get(0).namespace())
                .isEqualTo(SOURCE_DATASET_NAMESPACE);

        assertThat(lineageGraph.sinks()).isEmpty();
        assertThat(lineageGraph.relations()).isEmpty();
    }

    @Test
    void testExtractPartialLineageGraphWithSinkOnly() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(
                        new NumberSequenceSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        source.sinkTo(new LineageSink());

        LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(env.getTransformations());

        assertThat(lineageGraph.sinks().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).name())
                .isEqualTo(SINK_DATASET_NAME);
        assertThat(lineageGraph.sinks().get(0).datasets().get(0).namespace())
                .isEqualTo(SINK_DATASET_NAMESPACE);

        assertThat(lineageGraph.sources()).isEmpty();
        assertThat(lineageGraph.relations()).isEmpty();
    }

    @Test
    void testSourceDeduplication() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        source.sinkTo(new DiscardingSink<>());
        List<Transformation<?>> list = new ArrayList<>();
        list.addAll(env.getTransformations());
        list.addAll(env.getTransformations());
        LineageGraph lineageGraph = LineageGraphUtils.convertToLineageGraph(list);

        assertThat(lineageGraph.sources().size()).isEqualTo(1);
        assertThat(lineageGraph.sinks().size()).isEqualTo(0);
        assertThat(lineageGraph.relations()).isEmpty();
    }

    @Test
    void testSinkDuduplication() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(
                        new NumberSequenceSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        source.sinkTo(new LineageSink());

        List<Transformation<?>> list = new ArrayList<>();
        list.addAll(env.getTransformations());
        list.addAll(env.getTransformations());
        LineageGraph lineageGraph = LineageGraphUtils.convertToLineageGraph(list);

        assertThat(lineageGraph.sources().size()).isEqualTo(0);
        assertThat(lineageGraph.sinks().size()).isEqualTo(1);
        assertThat(lineageGraph.relations()).isEmpty();
    }

    @Test
    void testExtractColumnLineageFromSinkTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        DataStreamSink<Long> sink = source.sinkTo(new LineageSink());

        LineageDataset sourceDataset =
                new DefaultLineageDataset(
                        SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE, new HashMap<>());
        LineageDataset sinkDataset =
                new DefaultLineageDataset(
                        SINK_DATASET_NAME, SINK_DATASET_NAMESPACE, new HashMap<>());
        ColumnLineageRelation relation =
                new DefaultColumnLineageRelation(
                        sinkDataset,
                        "result",
                        Arrays.asList(
                                new DefaultColumnLineageInput(
                                        sourceDataset,
                                        "value",
                                        ColumnLineageDependencyType.DIRECT)),
                        ColumnLineageOrigin.INPUT_FIELDS,
                        "EXPRESSION");
        TransformationWithLineage<?> sinkTransformation =
                (TransformationWithLineage<?>) sink.getTransformation();
        sinkTransformation.setColumnLineage(
                new TransformationColumnLineage(Arrays.asList("result"), Arrays.asList(relation)));

        LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(Arrays.asList(sink.getTransformation()));

        assertThat(lineageGraph.columnRelations()).containsExactly(relation);
    }

    @Test
    void testMissingTransformationColumnLineageRelationIsRejected() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        DataStreamSink<Long> sink = source.sinkTo(new LineageSink());
        TransformationWithLineage<?> sinkTransformation =
                (TransformationWithLineage<?>) sink.getTransformation();
        sinkTransformation.setColumnLineage(
                new TransformationColumnLineage(
                        Collections.singletonList("result"), Collections.emptyList()));

        assertThatThrownBy(
                        () ->
                                LineageGraphUtils.convertToLineageGraph(
                                        Collections.singletonList(sink.getTransformation())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(SINK_DATASET_NAME)
                .hasMessageContaining("result")
                .hasMessageContaining("missing output relation");
    }

    @Test
    void testDataStreamTransformationWithoutColumnLineageRemainsCompatible() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        DataStreamSink<Long> sink = source.sinkTo(new LineageSink());

        LineageGraph lineageGraph =
                LineageGraphUtils.convertToLineageGraph(Arrays.asList(sink.getTransformation()));

        assertThat(lineageGraph.columnRelations()).isEmpty();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testExtractsColumnLineageFromArbitraryCarrierTransformation() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        GlobalCommitterTransform<?> carrier =
                new GlobalCommitterTransform(source, ignored -> null, () -> null);
        LineageDataset sourceDataset = dataset(SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE);
        LineageDataset sinkDataset = dataset(SINK_DATASET_NAME, SINK_DATASET_NAMESPACE);
        ColumnLineageRelation relation =
                relation(
                        sourceDataset,
                        "value",
                        ColumnLineageDependencyType.DIRECT,
                        sinkDataset,
                        "result",
                        ColumnLineageOrigin.INPUT_FIELDS,
                        "EXPRESSION");
        carrier.setLineageVertex(lineageVertexOf(sinkDataset));
        carrier.setColumnLineage(
                new TransformationColumnLineage(List.of("result"), List.of(relation)));

        LineageGraph graph = LineageGraphUtils.convertToLineageGraph(List.of(carrier));

        assertThat(graph.sinks()).containsExactly(carrier.getLineageVertex());
        assertThat(graph.columnRelations()).containsExactly(relation);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testRejectsCarrierWithoutExactlyOneNonSourceLineageDataset() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        LineageDataset sinkDataset = dataset(SINK_DATASET_NAME, SINK_DATASET_NAMESPACE);

        for (LineageVertex invalidVertex :
                Arrays.asList(
                        null,
                        sourceLineageVertexOf(Boundedness.BOUNDED, sinkDataset),
                        lineageVertexOf(),
                        lineageVertexOf(sinkDataset, dataset("other", "sink://other")))) {
            GlobalCommitterTransform<?> carrier =
                    new GlobalCommitterTransform(source, ignored -> null, () -> null);
            carrier.setLineageVertex(invalidVertex);
            carrier.setColumnLineage(
                    new TransformationColumnLineage(
                            List.of("result"),
                            List.of(
                                    new DefaultColumnLineageRelation(
                                            sinkDataset,
                                            "result",
                                            Collections.emptyList(),
                                            ColumnLineageOrigin.CONSTANT,
                                            null))));

            assertThatThrownBy(() -> LineageGraphUtils.convertToLineageGraph(List.of(carrier)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("column lineage")
                    .hasMessageContaining("sink lineage vertex");
        }
    }

    @Test
    void testMergesMultiplePhysicalWritesWithoutDroppingDependencyTypes() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        DataStreamSink<Long> first = source.sinkTo(new LineageSink());
        DataStreamSink<Long> second = source.sinkTo(new LineageSink());
        LineageDataset sourceDataset = dataset(SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE);
        LineageDataset sinkDataset = dataset(SINK_DATASET_NAME, SINK_DATASET_NAMESPACE);
        setColumnLineage(
                first,
                relation(
                        sourceDataset,
                        "value",
                        ColumnLineageDependencyType.DIRECT,
                        sinkDataset,
                        "result",
                        ColumnLineageOrigin.INPUT_FIELDS,
                        "EXPRESSION"));
        setColumnLineage(
                second,
                relation(
                        sourceDataset,
                        "value",
                        ColumnLineageDependencyType.INDIRECT,
                        sinkDataset,
                        "result",
                        ColumnLineageOrigin.SYSTEM,
                        "FILTER"));

        LineageGraph graph =
                LineageGraphUtils.convertToLineageGraph(
                        Arrays.asList(first.getTransformation(), second.getTransformation()));

        assertThat(graph.columnRelations()).hasSize(1);
        ColumnLineageRelation merged = graph.columnRelations().get(0);
        assertThat(merged.inputs())
                .extracting(ColumnLineageInput::dependencyType)
                .containsExactlyInAnyOrder(
                        ColumnLineageDependencyType.DIRECT, ColumnLineageDependencyType.INDIRECT);
        assertThat(merged.origin()).isEqualTo(ColumnLineageOrigin.INPUT_FIELDS);
        assertThat(merged.transformation()).hasValue("EXPRESSION,FILTER,UNION");
    }

    @Test
    void testUnionsExpectedFieldsFromPartialPhysicalWrites() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> source =
                env.fromSource(new LineageSource(1L, 5L), WatermarkStrategy.noWatermarks(), "");
        DataStreamSink<Long> first = source.sinkTo(new LineageSink());
        DataStreamSink<Long> second = source.sinkTo(new LineageSink());
        LineageDataset sourceDataset = dataset(SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE);
        LineageDataset sinkDataset = dataset(SINK_DATASET_NAME, SINK_DATASET_NAMESPACE);
        setColumnLineage(
                first,
                relation(
                        sourceDataset,
                        "value",
                        ColumnLineageDependencyType.DIRECT,
                        sinkDataset,
                        "left_result",
                        ColumnLineageOrigin.INPUT_FIELDS,
                        null));
        setColumnLineage(
                second,
                relation(
                        sourceDataset,
                        "value",
                        ColumnLineageDependencyType.DIRECT,
                        sinkDataset,
                        "right_result",
                        ColumnLineageOrigin.INPUT_FIELDS,
                        null));

        LineageGraph graph =
                LineageGraphUtils.convertToLineageGraph(
                        Arrays.asList(first.getTransformation(), second.getTransformation()));

        assertThat(graph.columnRelations())
                .extracting(ColumnLineageRelation::outputField)
                .containsExactlyInAnyOrder("left_result", "right_result");
    }

    private static void setColumnLineage(
            DataStreamSink<Long> sink, ColumnLineageRelation relation) {
        ((TransformationWithLineage<?>) sink.getTransformation())
                .setColumnLineage(
                        new TransformationColumnLineage(
                                List.of(relation.outputField()), List.of(relation)));
    }

    private static ColumnLineageRelation relation(
            LineageDataset source,
            String inputField,
            ColumnLineageDependencyType dependencyType,
            LineageDataset sink,
            String outputField,
            ColumnLineageOrigin origin,
            String transformation) {
        return new DefaultColumnLineageRelation(
                sink,
                outputField,
                List.of(new DefaultColumnLineageInput(source, inputField, dependencyType)),
                origin,
                transformation);
    }

    private static LineageDataset dataset(String name, String namespace) {
        return new DefaultLineageDataset(name, namespace, new HashMap<>());
    }

    private static LineageVertex lineageVertexOf(LineageDataset... datasets) {
        DefaultLineageVertex vertex = new DefaultLineageVertex();
        Arrays.stream(datasets).forEach(vertex::addLineageDataset);
        return vertex;
    }

    private static SourceLineageVertex sourceLineageVertexOf(
            Boundedness boundedness, LineageDataset... datasets) {
        DefaultSourceLineageVertex vertex = new DefaultSourceLineageVertex(boundedness);
        Arrays.stream(datasets).forEach(vertex::addDataset);
        return vertex;
    }

    private static class LineageSink extends DiscardingSink<Long> implements LineageVertexProvider {
        public LineageSink() {
            super();
        }

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            SINK_DATASET_NAME, SINK_DATASET_NAMESPACE, new HashMap<>());
            DefaultLineageVertex lineageVertex = new DefaultLineageVertex();
            lineageVertex.addLineageDataset(lineageDataset);
            return lineageVertex;
        }
    }

    private static class LineageSource extends NumberSequenceSource
            implements LineageVertexProvider {

        public LineageSource(long from, long to) {
            super(from, to);
        }

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            SOURCE_DATASET_NAME, SOURCE_DATASET_NAMESPACE, new HashMap<>());
            DefaultSourceLineageVertex lineageVertex =
                    new DefaultSourceLineageVertex(Boundedness.BOUNDED);
            lineageVertex.addDataset(lineageDataset);
            return lineageVertex;
        }
    }

    private static class LineageSourceFunction
            implements SourceFunction<Long>, LineageVertexProvider {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long next = 0;
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(next++);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            LEGACY_SOURCE_DATASET_NAME,
                            LEGACY_SOURCE_DATASET_NAMESPACE,
                            new HashMap<>());
            DefaultSourceLineageVertex lineageVertex =
                    new DefaultSourceLineageVertex(Boundedness.CONTINUOUS_UNBOUNDED);
            lineageVertex.addDataset(lineageDataset);
            return lineageVertex;
        }
    }

    private static class LineageSinkFunction implements SinkFunction<Long>, LineageVertexProvider {

        @Override
        public LineageVertex getLineageVertex() {
            LineageDataset lineageDataset =
                    new DefaultLineageDataset(
                            LEGACY_SINK_DATASET_NAME,
                            LEGACY_SINK_DATASET_NAMESPACE,
                            new HashMap<>());
            DefaultLineageVertex lineageVertex = new DefaultLineageVertex();
            lineageVertex.addLineageDataset(lineageDataset);
            return lineageVertex;
        }
    }
}

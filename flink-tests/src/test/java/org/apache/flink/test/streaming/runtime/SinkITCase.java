/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.datagen.source.TestDataGenerators;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.apache.flink.streaming.runtime.operators.sink.TestSink.END_OF_INPUT_STR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link org.apache.flink.api.connector.sink.Sink} run time implementation.
 */
class SinkITCase extends AbstractTestBase {
    static final List<Integer> SOURCE_DATA =
            Arrays.asList(
                    895, 127, 148, 161, 148, 662, 822, 491, 275, 122, 850, 630, 682, 765, 434, 970,
                    714, 795, 288, 422);

    // source send data two times
    static final int STREAMING_SOURCE_SEND_ELEMENTS_NUM = SOURCE_DATA.size() * 2;

    static final String[] EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE =
            SOURCE_DATA.stream()
                    // source send data two times
                    .flatMap(
                            x ->
                                    Collections.nCopies(
                                            2, Tuple3.of(x, null, Long.MIN_VALUE).toString())
                                            .stream())
                    .toArray(String[]::new);

    static final String[] EXPECTED_COMMITTED_DATA_IN_BATCH_MODE =
            SOURCE_DATA.stream()
                    .map(x -> Tuple3.of(x, null, Long.MIN_VALUE).toString())
                    .toArray(String[]::new);

    static final String[] EXPECTED_GLOBAL_COMMITTED_DATA_IN_STREAMING_MODE =
            SOURCE_DATA.stream()
                    // source send data two times
                    .flatMap(
                            x ->
                                    Collections.nCopies(
                                            2, Tuple3.of(x, null, Long.MIN_VALUE).toString())
                                            .stream())
                    .toArray(String[]::new);

    static final String EXPECTED_GLOBAL_COMMITTED_DATA_IN_BATCH_MODE =
            SOURCE_DATA.stream()
                    .map(x -> Tuple3.of(x, null, Long.MIN_VALUE).toString())
                    .sorted()
                    .collect(joining("+"));

    static final Queue<String> COMMIT_QUEUE = new ConcurrentLinkedQueue<>();

    static final Queue<String> GLOBAL_COMMIT_QUEUE = new ConcurrentLinkedQueue<>();

    static final BooleanSupplier COMMIT_QUEUE_RECEIVE_ALL_DATA =
            (BooleanSupplier & Serializable)
                    () -> COMMIT_QUEUE.size() == STREAMING_SOURCE_SEND_ELEMENTS_NUM;

    static final BooleanSupplier GLOBAL_COMMIT_QUEUE_RECEIVE_ALL_DATA =
            (BooleanSupplier & Serializable)
                    () ->
                            getSplitGlobalCommittedData().size()
                                    == STREAMING_SOURCE_SEND_ELEMENTS_NUM;

    static final BooleanSupplier BOTH_QUEUE_RECEIVE_ALL_DATA =
            (BooleanSupplier & Serializable)
                    () ->
                            COMMIT_QUEUE_RECEIVE_ALL_DATA.getAsBoolean()
                                    && GLOBAL_COMMIT_QUEUE_RECEIVE_ALL_DATA.getAsBoolean();

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    @BeforeEach
    public void init() {
        COMMIT_QUEUE.clear();
        GLOBAL_COMMIT_QUEUE.clear();
    }

    @Test
    void writerAndCommitterAndGlobalCommitterExecuteInStreamingMode() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();

        final DataStream<Integer> stream =
                env.fromSource(
                        TestDataGenerators.fromDataWithSnapshotsLatch(
                                SOURCE_DATA, Types.INT, BOTH_QUEUE_RECEIVE_ALL_DATA),
                        WatermarkStrategy.noWatermarks(),
                        "Test Source");

        stream.sinkTo(
                TestSink.newBuilder()
                        .setDefaultCommitter(
                                (Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE)
                        .setGlobalCommitter(
                                (Supplier<Queue<String>> & Serializable) () -> GLOBAL_COMMIT_QUEUE)
                        .build());

        executeAndVerifyStreamGraph(env);

        // TODO: At present, for a bounded scenario, the occurrence of final checkpoint is not a
        // deterministic event, so
        // we do not need to verify this matter. After the final checkpoint becomes ready in the
        // future,
        // the verification of "end of input" would be restored.
        GLOBAL_COMMIT_QUEUE.remove(END_OF_INPUT_STR);

        assertThat(COMMIT_QUEUE)
                .containsExactlyInAnyOrder(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE);

        assertThat(getSplitGlobalCommittedData())
                .containsExactlyInAnyOrder(EXPECTED_GLOBAL_COMMITTED_DATA_IN_STREAMING_MODE);
    }

    @Test
    void writerAndCommitterAndGlobalCommitterExecuteInBatchMode() throws Exception {
        final StreamExecutionEnvironment env = buildBatchEnv();

        env.fromData(SOURCE_DATA)
                .sinkTo(
                        TestSink.newBuilder()
                                .setDefaultCommitter(
                                        (Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE)
                                .setGlobalCommitter(
                                        (Supplier<Queue<String>> & Serializable)
                                                () -> GLOBAL_COMMIT_QUEUE)
                                .build());

        executeAndVerifyStreamGraph(env);

        assertThat(COMMIT_QUEUE).containsExactlyInAnyOrder(EXPECTED_COMMITTED_DATA_IN_BATCH_MODE);

        assertThat(GLOBAL_COMMIT_QUEUE)
                .containsExactlyInAnyOrder(EXPECTED_GLOBAL_COMMITTED_DATA_IN_BATCH_MODE);
    }

    @Test
    void writerAndCommitterExecuteInStreamingMode() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();

        final DataStream<Integer> stream =
                env.fromSource(
                        TestDataGenerators.fromDataWithSnapshotsLatch(
                                SOURCE_DATA, Types.INT, COMMIT_QUEUE_RECEIVE_ALL_DATA),
                        WatermarkStrategy.noWatermarks(),
                        "Test Source");

        stream.sinkTo(
                TestSink.newBuilder()
                        .setDefaultCommitter(
                                (Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE)
                        .build());

        executeAndVerifyStreamGraph(env);

        assertThat(COMMIT_QUEUE)
                .containsExactlyInAnyOrder(EXPECTED_COMMITTED_DATA_IN_STREAMING_MODE);
    }

    /**
     * Creates a bounded stream with a failing committer. The test verifies that the Sink correctly
     * recovers and handles multiple endInput().
     */
    @Test
    void duplicateEndInput() throws Exception {
        // we need at least 2 attempts but add a bit of a safety margin for unexpected retries
        int maxAttempts = 10;
        final Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, maxAttempts);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, maxAttempts);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(100);

        AtomicBoolean failedOnce = new AtomicBoolean(false);
        List<String> committed = new ArrayList<>();
        FailingOnceCommitter committer =
                new FailingOnceCommitter(
                        sharedObjects.add(failedOnce), sharedObjects.add(committed));
        env.<Object>fromData("bounded")
                .sinkTo(TestSinkV2.newBuilder().setCommitter(committer).build());

        JobClient jobClient = env.executeAsync();
        // wait for job to finish including restarts
        jobClient.getJobExecutionResult().get();
        // Did we successfully finish?
        Assertions.assertThat(jobClient.getJobStatus().get()).isEqualTo(JobStatus.FINISHED);

        // check that we error'ed once as expected
        Assertions.assertThat(failedOnce).isTrue();
        // but also eventually succeed to commit (size > 1 in case of unexpected retries)
        Assertions.assertThat(committed).isNotEmpty();
    }

    @Test
    void writerAndCommitterExecuteInBatchMode() throws Exception {
        final StreamExecutionEnvironment env = buildBatchEnv();

        env.fromData(SOURCE_DATA)
                .sinkTo(
                        TestSink.newBuilder()
                                .setDefaultCommitter(
                                        (Supplier<Queue<String>> & Serializable) () -> COMMIT_QUEUE)
                                .build());

        executeAndVerifyStreamGraph(env);

        assertThat(COMMIT_QUEUE).containsExactlyInAnyOrder(EXPECTED_COMMITTED_DATA_IN_BATCH_MODE);
    }

    @Test
    void writerAndGlobalCommitterExecuteInStreamingMode() throws Exception {
        final StreamExecutionEnvironment env = buildStreamEnv();

        final DataStream<Integer> stream =
                env.fromSource(
                        TestDataGenerators.fromDataWithSnapshotsLatch(
                                SOURCE_DATA, Types.INT, GLOBAL_COMMIT_QUEUE_RECEIVE_ALL_DATA),
                        WatermarkStrategy.noWatermarks(),
                        "Test Source");

        stream.sinkTo(
                TestSink.newBuilder()
                        .setGlobalCommitter(
                                (Supplier<Queue<String>> & Serializable) () -> GLOBAL_COMMIT_QUEUE)
                        .build());

        executeAndVerifyStreamGraph(env);

        // TODO: At present, for a bounded scenario, the occurrence of final checkpoint is not a
        // deterministic event, so
        // we do not need to verify this matter. After the final checkpoint becomes ready in the
        // future,
        // the verification of "end of input" would be restored.
        GLOBAL_COMMIT_QUEUE.remove(END_OF_INPUT_STR);

        assertThat(getSplitGlobalCommittedData())
                .containsExactlyInAnyOrder(EXPECTED_GLOBAL_COMMITTED_DATA_IN_STREAMING_MODE);
    }

    @Test
    void writerAndGlobalCommitterExecuteInBatchMode() throws Exception {
        final StreamExecutionEnvironment env = buildBatchEnv();

        env.fromData(SOURCE_DATA)
                .sinkTo(
                        TestSink.newBuilder()
                                .setGlobalCommitter(
                                        (Supplier<Queue<String>> & Serializable)
                                                () -> GLOBAL_COMMIT_QUEUE)
                                .build());

        executeAndVerifyStreamGraph(env);

        assertThat(GLOBAL_COMMIT_QUEUE)
                .containsExactlyInAnyOrder(EXPECTED_GLOBAL_COMMITTED_DATA_IN_BATCH_MODE);
    }

    private void executeAndVerifyStreamGraph(StreamExecutionEnvironment env) throws Exception {
        StreamGraph streamGraph = env.getStreamGraph();
        assertNoUnalignedCheckpointInSink(streamGraph);
        env.execute(streamGraph);
    }

    private void assertNoUnalignedCheckpointInSink(StreamGraph streamGraph) {
        // all the edges out of the sink nodes should not support unaligned checkpoints
        // we rely on other tests that this property is correctly used.
        assertThat(streamGraph.getStreamNodes())
                .filteredOn(t -> t.getOperatorName().contains("Sink"))
                .flatMap(StreamNode::getOutEdges)
                .allMatch(e -> !e.supportsUnalignedCheckpoints())
                .isNotEmpty();
    }

    private static List<String> getSplitGlobalCommittedData() {
        return GLOBAL_COMMIT_QUEUE.stream()
                .flatMap(x -> Arrays.stream(x.split("\\+")))
                .collect(Collectors.toList());
    }

    private StreamExecutionEnvironment buildStreamEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(100);
        return env;
    }

    private StreamExecutionEnvironment buildBatchEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return env;
    }

    static class FailingOnceCommitter extends TestSinkV2.DefaultCommitter {
        private final SharedReference<AtomicBoolean> failedOnce;
        private final SharedReference<List<String>> committed;

        public FailingOnceCommitter(
                SharedReference<AtomicBoolean> failedOnce,
                SharedReference<List<String>> committed) {
            this.failedOnce = failedOnce;
            this.committed = committed;
        }

        @Override
        public void commit(Collection<CommitRequest<String>> committables) {
            if (failedOnce.get().compareAndSet(false, true)) {
                throw new RuntimeException("Fail to commit");
            }
            for (CommitRequest<String> committable : committables) {
                this.committed.get().add(committable.getCommittable());
            }
        }
    }
}

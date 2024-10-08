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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.tests.artificialstate.ArtificalOperatorStateMapper;
import org.apache.flink.streaming.tests.artificialstate.ArtificialKeyedStateMapper;
import org.apache.flink.streaming.tests.artificialstate.builder.ArtificialListStateBuilder;
import org.apache.flink.streaming.tests.artificialstate.builder.ArtificialStateBuilder;
import org.apache.flink.streaming.tests.artificialstate.builder.ArtificialValueStateBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.streaming.tests.TestOperatorEnum.EVENT_IDENTITY_MAPPER;
import static org.apache.flink.streaming.tests.TestOperatorEnum.MAPPER_RETURNS_OUT_WITH_CUSTOM_SER;
import static org.apache.flink.streaming.tests.TestOperatorEnum.RESULT_TYPE_QUERYABLE_MAPPER_WITH_CUSTOM_SER;

/**
 * A factory for components of general purpose test jobs for Flink's DataStream API operators and
 * primitives.
 *
 * <p>The components can be configured for different state backends, including memory, file, and
 * RocksDB state backends. It also allows specifying the processing guarantee semantics, which will
 * also be verified by the job itself according to the specified semantic.
 *
 * <p>Program parameters:
 *
 * <ul>
 *   <li>test.semantics (String, default - 'exactly-once'): This configures the semantics to test.
 *       Can be 'exactly-once' or 'at-least-once'.
 *   <li>test.simulate_failure (boolean, default - false): This configures whether or not to
 *       simulate failures by throwing exceptions within the job.
 *   <li>test.simulate_failure.num_records (long, default - 100L): The number of records to process
 *       before throwing an exception, per job execution attempt. Only relevant if configured to
 *       simulate failures.
 *   <li>test.simulate_failure.num_checkpoints (long, default - 1L): The number of complete
 *       checkpoints before throwing an exception, per job execution attempt. Only relevant if
 *       configured to simulate failures.
 *   <li>test.simulate_failure.max_failures (int, default - 1): The maximum number of times to fail
 *       the job. This also takes into account failures that were not triggered by the job's own
 *       failure simulation, e.g. TaskManager or JobManager failures. Only relevant if configured to
 *       simulate failures.
 *   <li>environment.checkpoint_interval (long, default - 1000): the checkpoint interval.
 *   <li>environment.externalize_checkpoint (boolean, default - false): whether or not checkpoints
 *       should be externalized.
 *   <li>environment.externalize_checkpoint.cleanup (String, default - 'retain'): Configures the
 *       cleanup mode for externalized checkpoints. Can be 'retain' or 'delete'.
 *   <li>environment.tolerable_checkpoint_failure_number (int, default - 0): Sets the expected
 *       behaviour for the job manager in case that it received declined checkpoints from tasks.
 *   <li>environment.parallelism (int, default - 1): parallelism to use for the job.
 *   <li>environment.max_parallelism (int, default - 128): max parallelism to use for the job
 *   <li>environment.restart_strategy (String, default - 'fixed_delay'): The failure restart
 *       strategy to use. Can be 'fixed_delay' or 'no_restart'.
 *   <li>environment.restart_strategy.fixed_delay.attempts (Integer, default - Integer.MAX_VALUE):
 *       The number of allowed attempts to restart the job, when using 'fixed_delay' restart.
 *   <li>environment.restart_strategy.fixed_delay.delay (long, default - 0): delay between restart
 *       attempts, in milliseconds, when using 'fixed_delay' restart.
 *   <li>state_backend (String, default - 'file'): Supported values are 'file' for FsStateBackend
 *       and 'rocks' for RocksDBStateBackend.
 *   <li>state_backend.checkpoint_directory (String): The checkpoint directory.
 *   <li>state_backend.rocks.incremental (boolean, default - false): Activate or deactivate
 *       incremental snapshots if RocksDBStateBackend is selected.
 *   <li>state_backend.file.async (boolean, default - true): Activate or deactivate asynchronous
 *       snapshots if FileStateBackend is selected.
 *   <li>sequence_generator_source.keyspace (int, default - 1000): Number of different keys for
 *       events emitted by the sequence generator.
 *   <li>sequence_generator_source.payload_size (int, default - 20): Length of message payloads
 *       emitted by the sequence generator.
 *   <li>sequence_generator_source.sleep_time (long, default - 0): Milliseconds to sleep after
 *       emitting events in the sequence generator. Set to 0 to disable sleeping.
 *   <li>sequence_generator_source.sleep_after_elements (long, default - 0): Number of elements to
 *       emit before sleeping in the sequence generator. Set to 0 to disable sleeping.
 *   <li>sequence_generator_source.event_time.max_out_of_order (long, default - 500): Max event time
 *       out-of-orderness for events emitted by the sequence generator.
 *   <li>sequence_generator_source.event_time.clock_progress (long, default - 100): The amount of
 *       event time to progress per event generated by the sequence generator.
 *   <li>tumbling_window_operator.num_events (long, default - 20L): The duration of the window,
 *       indirectly determined by the target number of events in each window. Total duration is
 *       (sliding_window_operator.num_events) *
 *       (sequence_generator_source.event_time.clock_progress).
 *   <li>test_slide_factor (int, default - 3): test_slide_factor (int, default - 3): how many slides
 *       there are in a single window (in other words at most how many windows may be opened at time
 *       for a given key) The length of a window will be calculated as (test_slide_size) *
 *       (test_slide_factor)
 *   <li>test_slide_size (long, default - 250): length of a slide of sliding window in milliseconds.
 *       The length of a window will be calculated as (test_slide_size) * (test_slide_factor)
 * </ul>
 */
public class DataStreamAllroundTestJobFactory {
    private static final ConfigOption<String> TEST_SEMANTICS =
            ConfigOptions.key("test.semantics")
                    .stringType()
                    .defaultValue("exactly-once")
                    .withDescription(
                            "This configures the semantics to test. Can be 'exactly-once' or 'at-least-once'");

    private static final ConfigOption<Boolean> TEST_SIMULATE_FAILURE =
            ConfigOptions.key("test.simulate_failure")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "This configures whether or not to simulate failures by throwing exceptions within the job.");

    private static final ConfigOption<Long> TEST_SIMULATE_FAILURE_NUM_RECORDS =
            ConfigOptions.key("test.simulate_failure.num_records")
                    .longType()
                    .defaultValue(100L)
                    .withDescription(
                            "The number of records to process before throwing an exception, per job execution attempt."
                                    + " Only relevant if configured to simulate failures.");

    private static final ConfigOption<Long> TEST_SIMULATE_FAILURE_NUM_CHECKPOINTS =
            ConfigOptions.key("test.simulate_failure.num_checkpoints")
                    .longType()
                    .defaultValue(1L)
                    .withDescription(
                            "The number of complete checkpoints before throwing an exception, per job execution attempt."
                                    + " Only relevant if configured to simulate failures.");

    private static final ConfigOption<Integer> TEST_SIMULATE_FAILURE_MAX_FAILURES =
            ConfigOptions.key("test.simulate_failure.max_failures")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The maximum number of times to fail the job. This also takes into account failures that were not triggered"
                                    + " by the job's own failure simulation, e.g. TaskManager or JobManager failures."
                                    + " Only relevant if configured to simulate failures.");

    private static final ConfigOption<Long> ENVIRONMENT_CHECKPOINT_INTERVAL =
            ConfigOptions.key("environment.checkpoint_interval").longType().defaultValue(1000L);

    private static final ConfigOption<Boolean> ENVIRONMENT_EXTERNALIZE_CHECKPOINT =
            ConfigOptions.key("environment.externalize_checkpoint")
                    .booleanType()
                    .defaultValue(false);

    private static final ConfigOption<String> ENVIRONMENT_EXTERNALIZE_CHECKPOINT_CLEANUP =
            ConfigOptions.key("environment.externalize_checkpoint.cleanup")
                    .stringType()
                    .defaultValue("retain");

    private static final ConfigOption<Integer> ENVIRONMENT_TOLERABLE_DECLINED_CHECKPOINT_NUMBER =
            ConfigOptions.key("environment.tolerable_declined_checkpoint_number ")
                    .intType()
                    .defaultValue(0);

    private static final ConfigOption<Integer> ENVIRONMENT_PARALLELISM =
            ConfigOptions.key("environment.parallelism").intType().defaultValue(1);

    private static final ConfigOption<Integer> ENVIRONMENT_MAX_PARALLELISM =
            ConfigOptions.key("environment.max_parallelism").intType().defaultValue(128);

    private static final ConfigOption<String> ENVIRONMENT_RESTART_STRATEGY =
            ConfigOptions.key("environment.restart_strategy")
                    .stringType()
                    .defaultValue("fixed_delay");

    private static final ConfigOption<Integer> ENVIRONMENT_RESTART_STRATEGY_FIXED_ATTEMPTS =
            ConfigOptions.key("environment.restart_strategy.fixed_delay.attempts")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE);

    private static final ConfigOption<Long> ENVIRONMENT_RESTART_STRATEGY_FIXED_DELAY =
            ConfigOptions.key("environment.restart_strategy.fixed.delay")
                    .longType()
                    .defaultValue(0L);

    private static final ConfigOption<String> STATE_BACKEND =
            ConfigOptions.key("state_backend")
                    .stringType()
                    .defaultValue("hashmap")
                    .withDescription(
                            "Supported values are 'hashmap' for HashMapStateBackend and 'rocks' "
                                    + "for EmbeddedRocksDBStateBackend.");

    private static final ConfigOption<String> STATE_BACKEND_CHECKPOINT_DIR =
            ConfigOptions.key("state_backend.checkpoint_directory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The checkpoint directory.");

    static final ConfigOption<Boolean> STATE_BACKEND_ROCKS_INCREMENTAL =
            ConfigOptions.key("state_backend.rocks.incremental")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Activate or deactivate incremental snapshots if RocksDBStateBackend is selected.");

    private static final ConfigOption<Integer> SEQUENCE_GENERATOR_SRC_KEYSPACE =
            ConfigOptions.key("sequence_generator_source.keyspace").intType().defaultValue(200);

    private static final ConfigOption<Integer> SEQUENCE_GENERATOR_SRC_PAYLOAD_SIZE =
            ConfigOptions.key("sequence_generator_source.payload_size").intType().defaultValue(20);

    private static final ConfigOption<Long> SEQUENCE_GENERATOR_SRC_SLEEP_TIME =
            ConfigOptions.key("sequence_generator_source.sleep_time").longType().defaultValue(0L);

    private static final ConfigOption<Long> SEQUENCE_GENERATOR_SRC_SLEEP_AFTER_ELEMENTS =
            ConfigOptions.key("sequence_generator_source.sleep_after_elements")
                    .longType()
                    .defaultValue(0L);

    private static final ConfigOption<Long> SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS =
            ConfigOptions.key("sequence_generator_source.event_time.max_out_of_order")
                    .longType()
                    .defaultValue(0L);

    private static final ConfigOption<Long> SEQUENCE_GENERATOR_SRC_EVENT_TIME_CLOCK_PROGRESS =
            ConfigOptions.key("sequence_generator_source.event_time.clock_progress")
                    .longType()
                    .defaultValue(100L);

    private static final ConfigOption<Long> TUMBLING_WINDOW_OPERATOR_NUM_EVENTS =
            ConfigOptions.key("tumbling_window_operator.num_events").longType().defaultValue(20L);

    private static final ConfigOption<Integer> TEST_SLIDE_FACTOR =
            ConfigOptions.key("test_slide_factor").intType().defaultValue(3);

    private static final ConfigOption<Long> TEST_SLIDE_SIZE =
            ConfigOptions.key("test_slide_size").longType().defaultValue(250L);

    public static void setupEnvironment(StreamExecutionEnvironment env, ParameterTool pt)
            throws Exception {
        setupEnvironment(env, pt, true);
    }

    public static void setupEnvironment(
            StreamExecutionEnvironment env, ParameterTool pt, boolean setupStateBackend)
            throws Exception {
        setupCheckpointing(env, pt);
        setupParallelism(env, pt);
        setupRestartStrategy(env, pt);

        if (setupStateBackend) {
            setupStateBackend(env, pt);
        }

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(pt);
    }

    private static void setupCheckpointing(
            final StreamExecutionEnvironment env, final ParameterTool pt) {
        String semantics = pt.get(TEST_SEMANTICS.key(), TEST_SEMANTICS.defaultValue());
        long checkpointInterval =
                pt.getLong(
                        ENVIRONMENT_CHECKPOINT_INTERVAL.key(),
                        ENVIRONMENT_CHECKPOINT_INTERVAL.defaultValue());
        CheckpointingMode checkpointingMode =
                semantics.equalsIgnoreCase("exactly-once")
                        ? CheckpointingMode.EXACTLY_ONCE
                        : CheckpointingMode.AT_LEAST_ONCE;

        env.enableCheckpointing(checkpointInterval, checkpointingMode);

        final String checkpointDir = pt.getRequired(STATE_BACKEND_CHECKPOINT_DIR.key());
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
        env.configure(configuration);

        boolean enableExternalizedCheckpoints =
                pt.getBoolean(
                        ENVIRONMENT_EXTERNALIZE_CHECKPOINT.key(),
                        ENVIRONMENT_EXTERNALIZE_CHECKPOINT.defaultValue());

        if (enableExternalizedCheckpoints) {
            String cleanupModeConfig =
                    pt.get(
                            ENVIRONMENT_EXTERNALIZE_CHECKPOINT_CLEANUP.key(),
                            ENVIRONMENT_EXTERNALIZE_CHECKPOINT_CLEANUP.defaultValue());

            ExternalizedCheckpointRetention cleanupMode;
            switch (cleanupModeConfig) {
                case "retain":
                    cleanupMode = ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION;
                    break;
                case "delete":
                    cleanupMode = ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION;
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown clean up mode for externalized checkpoints: "
                                    + cleanupModeConfig);
            }
            env.getCheckpointConfig().setExternalizedCheckpointRetention(cleanupMode);

            final int tolerableDeclinedCheckpointNumber =
                    pt.getInt(
                            ENVIRONMENT_TOLERABLE_DECLINED_CHECKPOINT_NUMBER.key(),
                            ENVIRONMENT_TOLERABLE_DECLINED_CHECKPOINT_NUMBER.defaultValue());
            env.getCheckpointConfig()
                    .setTolerableCheckpointFailureNumber(tolerableDeclinedCheckpointNumber);
        }
    }

    private static void setupParallelism(
            final StreamExecutionEnvironment env, final ParameterTool pt) {
        env.setParallelism(
                pt.getInt(ENVIRONMENT_PARALLELISM.key(), ENVIRONMENT_PARALLELISM.defaultValue()));
        env.setMaxParallelism(
                pt.getInt(
                        ENVIRONMENT_MAX_PARALLELISM.key(),
                        ENVIRONMENT_MAX_PARALLELISM.defaultValue()));
    }

    private static void setupRestartStrategy(
            final StreamExecutionEnvironment env, final ParameterTool pt) {
        String restartStrategyConfig = pt.get(ENVIRONMENT_RESTART_STRATEGY.key());
        if (restartStrategyConfig != null) {
            switch (restartStrategyConfig) {
                case "fixed_delay":
                    Configuration configuration = new Configuration();
                    configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
                    configuration.set(
                            RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS,
                            pt.getInt(
                                    ENVIRONMENT_RESTART_STRATEGY_FIXED_ATTEMPTS.key(),
                                    ENVIRONMENT_RESTART_STRATEGY_FIXED_ATTEMPTS.defaultValue()));
                    configuration.set(
                            RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
                            Duration.ofMillis(
                                    pt.getLong(
                                            ENVIRONMENT_RESTART_STRATEGY_FIXED_DELAY.key(),
                                            ENVIRONMENT_RESTART_STRATEGY_FIXED_DELAY
                                                    .defaultValue())));

                    env.configure(configuration);
                    break;
                case "no_restart":
                    env.configure(
                            new Configuration()
                                    .set(RestartStrategyOptions.RESTART_STRATEGY, "none"));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unknown restart strategy: " + restartStrategyConfig);
            }
        }
    }

    private static void setupStateBackend(
            final StreamExecutionEnvironment env, final ParameterTool pt) throws IOException {
        final String stateBackend = pt.get(STATE_BACKEND.key(), STATE_BACKEND.defaultValue());

        if ("hashmap".equalsIgnoreCase(stateBackend)) {
            env.configure(new Configuration().set(StateBackendOptions.STATE_BACKEND, "hashmap"));
        } else if ("rocks".equalsIgnoreCase(stateBackend)) {
            boolean incrementalCheckpoints =
                    pt.getBoolean(
                            STATE_BACKEND_ROCKS_INCREMENTAL.key(),
                            STATE_BACKEND_ROCKS_INCREMENTAL.defaultValue());

            env.configure(
                    new Configuration()
                            .set(StateBackendOptions.STATE_BACKEND, "rocksdb")
                            .set(
                                    CheckpointingOptions.INCREMENTAL_CHECKPOINTS,
                                    incrementalCheckpoints));
        } else {
            throw new IllegalArgumentException("Unknown backend requested: " + stateBackend);
        }
    }

    /**
     * @deprecated This method relies on the {@link
     *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
     *     removed. Use the new {@link org.apache.flink.api.connector.source.Source} API instead.
     */
    @Deprecated
    static SourceFunction<Event> createEventSource(ParameterTool pt) {
        return new SequenceGeneratorSource(
                pt.getInt(
                        SEQUENCE_GENERATOR_SRC_KEYSPACE.key(),
                        SEQUENCE_GENERATOR_SRC_KEYSPACE.defaultValue()),
                pt.getInt(
                        SEQUENCE_GENERATOR_SRC_PAYLOAD_SIZE.key(),
                        SEQUENCE_GENERATOR_SRC_PAYLOAD_SIZE.defaultValue()),
                pt.getLong(
                        SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS.key(),
                        SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS.defaultValue()),
                pt.getLong(
                        SEQUENCE_GENERATOR_SRC_EVENT_TIME_CLOCK_PROGRESS.key(),
                        SEQUENCE_GENERATOR_SRC_EVENT_TIME_CLOCK_PROGRESS.defaultValue()),
                pt.getLong(
                        SEQUENCE_GENERATOR_SRC_SLEEP_TIME.key(),
                        SEQUENCE_GENERATOR_SRC_SLEEP_TIME.defaultValue()),
                pt.getLong(
                        SEQUENCE_GENERATOR_SRC_SLEEP_AFTER_ELEMENTS.key(),
                        SEQUENCE_GENERATOR_SRC_SLEEP_AFTER_ELEMENTS.defaultValue()));
    }

    static BoundedOutOfOrdernessTimestampExtractor<Event> createTimestampExtractor(
            ParameterTool pt) {
        return new BoundedOutOfOrdernessTimestampExtractor<Event>(
                Duration.ofMillis(
                        pt.getLong(
                                SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS.key(),
                                SEQUENCE_GENERATOR_SRC_EVENT_TIME_MAX_OUT_OF_ORDERNESS
                                        .defaultValue()))) {

            private static final long serialVersionUID = -3154419724891779938L;

            @Override
            public long extractTimestamp(Event element) {
                return element.getEventTime();
            }
        };
    }

    static WindowedStream<Event, Integer, TimeWindow> applyTumblingWindows(
            KeyedStream<Event, Integer> keyedStream, ParameterTool pt) {

        long eventTimeProgressPerEvent =
                pt.getLong(
                        SEQUENCE_GENERATOR_SRC_EVENT_TIME_CLOCK_PROGRESS.key(),
                        SEQUENCE_GENERATOR_SRC_EVENT_TIME_CLOCK_PROGRESS.defaultValue());

        return keyedStream.window(
                TumblingEventTimeWindows.of(
                        Duration.ofMillis(
                                pt.getLong(
                                                TUMBLING_WINDOW_OPERATOR_NUM_EVENTS.key(),
                                                TUMBLING_WINDOW_OPERATOR_NUM_EVENTS.defaultValue())
                                        * eventTimeProgressPerEvent)));
    }

    static FlatMapFunction<Event, String> createSemanticsCheckMapper(ParameterTool pt) {

        String semantics = pt.get(TEST_SEMANTICS.key(), TEST_SEMANTICS.defaultValue());

        SemanticsCheckMapper.ValidatorFunction validatorFunction;

        if (semantics.equalsIgnoreCase("exactly-once")) {
            validatorFunction = SemanticsCheckMapper.ValidatorFunction.exactlyOnce();
        } else if (semantics.equalsIgnoreCase("at-least-once")) {
            validatorFunction = SemanticsCheckMapper.ValidatorFunction.atLeastOnce();
        } else {
            throw new IllegalArgumentException("Unknown semantics requested: " + semantics);
        }

        return new SemanticsCheckMapper(validatorFunction);
    }

    static boolean isSimulateFailures(ParameterTool pt) {
        return pt.getBoolean(TEST_SIMULATE_FAILURE.key(), TEST_SIMULATE_FAILURE.defaultValue());
    }

    static MapFunction<Event, Event> createFailureMapper(ParameterTool pt) {
        return new FailureMapper<>(
                pt.getLong(
                        TEST_SIMULATE_FAILURE_NUM_RECORDS.key(),
                        TEST_SIMULATE_FAILURE_NUM_RECORDS.defaultValue()),
                pt.getLong(
                        TEST_SIMULATE_FAILURE_NUM_CHECKPOINTS.key(),
                        TEST_SIMULATE_FAILURE_NUM_CHECKPOINTS.defaultValue()),
                pt.getInt(
                        TEST_SIMULATE_FAILURE_MAX_FAILURES.key(),
                        TEST_SIMULATE_FAILURE_MAX_FAILURES.defaultValue()));
    }

    static <IN, OUT, STATE> ArtificialKeyedStateMapper<IN, OUT> createArtificialKeyedStateMapper(
            MapFunction<IN, OUT> mapFunction,
            JoinFunction<IN, STATE, STATE> inputAndOldStateToNewState,
            List<TypeSerializer<STATE>> stateSerializers,
            List<Class<STATE>> stateClasses) {

        List<ArtificialStateBuilder<IN>> artificialStateBuilders =
                new ArrayList<>(stateSerializers.size());
        for (TypeSerializer<STATE> typeSerializer : stateSerializers) {
            artificialStateBuilders.add(
                    createValueStateBuilder(
                            inputAndOldStateToNewState,
                            new ValueStateDescriptor<>(
                                    "valueState-" + typeSerializer.getClass().getSimpleName(),
                                    typeSerializer)));

            artificialStateBuilders.add(
                    createListStateBuilder(
                            inputAndOldStateToNewState,
                            new ListStateDescriptor<>(
                                    "listState-" + typeSerializer.getClass().getSimpleName(),
                                    typeSerializer)));
        }

        for (Class<STATE> stateClass : stateClasses) {
            artificialStateBuilders.add(
                    createValueStateBuilder(
                            inputAndOldStateToNewState,
                            new ValueStateDescriptor<>(
                                    "valueState-" + stateClass.getSimpleName(), stateClass)));

            artificialStateBuilders.add(
                    createListStateBuilder(
                            inputAndOldStateToNewState,
                            new ListStateDescriptor<>(
                                    "listState-" + stateClass.getSimpleName(), stateClass)));
        }

        return new ArtificialKeyedStateMapper<>(mapFunction, artificialStateBuilders);
    }

    static <IN, OUT> ArtificalOperatorStateMapper<IN, OUT> createArtificialOperatorStateMapper(
            MapFunction<IN, OUT> mapFunction) {

        return new ArtificalOperatorStateMapper<>(mapFunction);
    }

    private static <IN, STATE> ArtificialStateBuilder<IN> createValueStateBuilder(
            JoinFunction<IN, STATE, STATE> inputAndOldStateToNewState,
            ValueStateDescriptor<STATE> valueStateDescriptor) {

        return new ArtificialValueStateBuilder<>(
                valueStateDescriptor.getName(), inputAndOldStateToNewState, valueStateDescriptor);
    }

    private static <IN, STATE> ArtificialStateBuilder<IN> createListStateBuilder(
            JoinFunction<IN, STATE, STATE> inputAndOldStateToNewState,
            ListStateDescriptor<STATE> listStateDescriptor) {

        JoinFunction<IN, Iterable<STATE>, List<STATE>> listStateGenerator =
                (first, second) -> {
                    List<STATE> newState = new ArrayList<>();
                    for (STATE s : second) {
                        newState.add(inputAndOldStateToNewState.join(first, s));
                    }
                    return newState;
                };

        return new ArtificialListStateBuilder<>(
                listStateDescriptor.getName(),
                listStateGenerator,
                listStateGenerator,
                listStateDescriptor);
    }

    static SlidingEventTimeWindows createSlidingWindow(ParameterTool pt) {
        long slideSize = pt.getLong(TEST_SLIDE_SIZE.key(), TEST_SLIDE_SIZE.defaultValue());

        long slideFactor = pt.getInt(TEST_SLIDE_FACTOR.key(), TEST_SLIDE_FACTOR.defaultValue());

        return SlidingEventTimeWindows.of(
                Duration.ofMillis(slideSize * slideFactor), Duration.ofMillis(slideSize));
    }

    static FlatMapFunction<Tuple2<Integer, List<Event>>, String> createSlidingWindowCheckMapper(
            ParameterTool pt) {
        return new SlidingWindowCheckMapper(
                pt.getInt(TEST_SLIDE_FACTOR.key(), TEST_SLIDE_FACTOR.defaultValue()));
    }

    static DataStream<Event> verifyCustomStatefulTypeSerializer(DataStream<Event> eventStream) {
        return eventStream
                .map(new EventIdentityFunctionWithCustomEventTypeInformation())
                .name(RESULT_TYPE_QUERYABLE_MAPPER_WITH_CUSTOM_SER.getName())
                .uid(RESULT_TYPE_QUERYABLE_MAPPER_WITH_CUSTOM_SER.getUid())
                // apply a keyBy so that we have a non-chained operator with Event as input type
                // that goes through serialization
                .keyBy(new EventKeySelectorWithCustomKeyTypeInformation())
                .map(e -> e)
                .returns(new SingleThreadAccessCheckingTypeInfo<>(Event.class))
                .name(MAPPER_RETURNS_OUT_WITH_CUSTOM_SER.getName())
                .uid(MAPPER_RETURNS_OUT_WITH_CUSTOM_SER.getUid())
                // apply a keyBy so that we have a non-chained operator with Event as input type
                // that goes through serialization
                .keyBy(new EventKeySelectorWithCustomKeyTypeInformation())
                .map(e -> e)
                .name(EVENT_IDENTITY_MAPPER.getName())
                .uid(EVENT_IDENTITY_MAPPER.getUid());
    }

    private static class EventIdentityFunctionWithCustomEventTypeInformation
            implements MapFunction<Event, Event>, ResultTypeQueryable<Event> {

        private final SingleThreadAccessCheckingTypeInfo<Event> typeInformation =
                new SingleThreadAccessCheckingTypeInfo<>(Event.class);

        @Override
        public Event map(Event value) {
            return value;
        }

        @Override
        public TypeInformation<Event> getProducedType() {
            return typeInformation;
        }
    }

    private static class EventKeySelectorWithCustomKeyTypeInformation
            implements KeySelector<Event, Integer>, ResultTypeQueryable<Integer> {

        private final SingleThreadAccessCheckingTypeInfo<Integer> typeInformation =
                new SingleThreadAccessCheckingTypeInfo<>(Integer.class);

        @Override
        public Integer getKey(Event value) {
            return value.getKey();
        }

        @Override
        public TypeInformation<Integer> getProducedType() {
            return typeInformation;
        }
    }
}

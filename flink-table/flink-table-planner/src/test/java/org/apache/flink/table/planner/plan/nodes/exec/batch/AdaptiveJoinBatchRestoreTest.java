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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.BatchRestoreTestBase;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Compiled plan restore tests for {@link BatchExecAdaptiveJoin}. */
public class AdaptiveJoinBatchRestoreTest extends BatchRestoreTestBase {

    public AdaptiveJoinBatchRestoreTest() {
        super(BatchExecAdaptiveJoin.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Collections.singletonList(
                TableTestProgram.of(
                                "adaptive-join-runtime-broadcast",
                                "restore an adaptive join with duplicate and unmatched keys")
                        .setupConfig(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                                OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.RUNTIME_ONLY)
                        .setupTableSource(
                                SourceTestStep.newBuilder("left_input")
                                        .addSchema("k INT", "v INT")
                                        .producedValues(
                                                Row.of(1, 10),
                                                Row.of(2, 20),
                                                Row.of(3, 30),
                                                Row.of(null, 40))
                                        .build())
                        .setupTableSource(
                                SourceTestStep.newBuilder("right_input")
                                        .addSchema("k INT", "v INT")
                                        .producedValues(
                                                Row.of(1, 100),
                                                Row.of(1, 101),
                                                Row.of(2, 200),
                                                Row.of(4, 400))
                                        .build())
                        .setupTableSink(
                                SinkTestStep.newBuilder("sink")
                                        .addSchema("k INT", "v INT")
                                        .consumedValues(
                                                Row.of(1, 110), Row.of(1, 111), Row.of(2, 220))
                                        .build())
                        .runSql(
                                "INSERT INTO sink SELECT l.k, l.v + r.v"
                                        + " FROM left_input l JOIN right_input r ON l.k = r.k")
                        .build());
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    @Order(2)
    void testSnapshotContainsAdaptiveJoin(TableTestProgram program) throws Exception {
        JsonNode plan =
                new ObjectMapper()
                        .readTree(
                                Paths.get(
                                                System.getProperty("user.dir"),
                                                "src/test/resources/restore-tests",
                                                "batch-exec-adaptive-join_1",
                                                program.id,
                                                "plan",
                                                program.id + ".json")
                                        .toFile());
        assertThat(plan.path("nodes"))
                .anySatisfy(
                        node -> {
                            assertThat(node.path("type").asText())
                                    .isEqualTo("batch-exec-adaptive-join_1");
                            assertThat(node.path("originalJoin").asText())
                                    .isEqualTo("ShuffleHashJoin");
                        });
    }
}

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

/** Compiled plan restore tests for {@link BatchExecMultipleInput}. */
public class MultipleInputBatchRestoreTest extends BatchRestoreTestBase {

    public MultipleInputBatchRestoreTest() {
        super(BatchExecMultipleInput.class);
    }

    @Override
    public List<TableTestProgram> programs() {
        return Collections.singletonList(
                TableTestProgram.of(
                                "multiple-input-three-way-join",
                                "restore a fused subgraph containing multiple join operators")
                        .setupConfig(
                                OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, true)
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
                        .setupTableSource(
                                SourceTestStep.newBuilder("third_input")
                                        .addSchema("k INT", "v INT")
                                        .producedValues(
                                                Row.of(1, 1000), Row.of(2, 2000), Row.of(5, 5000))
                                        .build())
                        .setupTableSink(
                                SinkTestStep.newBuilder("sink")
                                        .addSchema("k INT", "v INT")
                                        .consumedValues(
                                                Row.of(1, 1110), Row.of(1, 1111), Row.of(2, 2220))
                                        .build())
                        .runSql(
                                "INSERT INTO sink SELECT /*+ BROADCAST(r, t) */ l.k, l.v + r.v + t.v"
                                        + " FROM left_input l JOIN right_input r ON l.k = r.k"
                                        + " JOIN third_input t ON l.k = t.k")
                        .build());
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    @Order(2)
    void testSnapshotContainsMultipleInput(TableTestProgram program) throws Exception {
        JsonNode plan =
                new ObjectMapper()
                        .readTree(
                                Paths.get(
                                                System.getProperty("user.dir"),
                                                "src/test/resources/restore-tests",
                                                "batch-exec-multiple-input_1",
                                                program.id,
                                                "plan",
                                                program.id + ".json")
                                        .toFile());
        assertThat(plan.path("nodes"))
                .anySatisfy(
                        node -> {
                            assertThat(node.path("type").asText())
                                    .isEqualTo("batch-exec-multiple-input_1");
                            assertThat(node.path("memberNodeIds").size()).isGreaterThan(1);
                            assertThat(node.path("originalEdgeReferences").size())
                                    .isGreaterThanOrEqualTo(3);
                        });
    }
}

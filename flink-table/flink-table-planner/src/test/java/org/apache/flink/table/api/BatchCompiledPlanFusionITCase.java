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

package org.apache.flink.table.api;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Ensures fusion execution cannot change the persisted compiled plan. */
@ExtendWith(MiniClusterExtension.class)
class BatchCompiledPlanFusionITCase {
    @AfterEach
    void cleanup() {
        TestValuesTableFactory.clearAllData();
    }

    @ParameterizedTest
    @ValueSource(strings = {"none", "json", "smile"})
    void testSerializeAndRestoreAfterFusionExecution(String serializedBeforeExecution)
            throws Exception {
        TableEnvironment environment = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        environment
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED, true);
        environment.getConfig().getConfiguration().setString("parallelism.default", "1");
        createSource(environment, "A", Row.of(1L, 2L), Row.of(2L, 3L));
        createSource(environment, "B", Row.of(1L, 3L), Row.of(2L, 7L), Row.of(9L, 8L));
        environment.executeSql("CREATE TABLE S (a BIGINT, b BIGINT) WITH ('connector'='values')");
        CompiledPlan plan =
                environment.compilePlanSql(
                        "INSERT INTO S SELECT /*+ BROADCAST(B) */ A.a, A.b + B.b FROM A JOIN B ON A.a = B.a");
        if (serializedBeforeExecution.equals("json")) {
            plan.asJsonString();
        } else if (serializedBeforeExecution.equals("smile")) {
            plan.asSmileBytes();
        }
        executeAndCheck(plan);
        String json = plan.asJsonString();
        byte[] smile = plan.asSmileBytes();
        JsonNode nodes = JacksonMapperFactory.createObjectMapper().readTree(json).get("nodes");
        boolean multiMemberFusion = false;
        for (JsonNode node : nodes) {
            if (node.get("type").asText().equals("batch-exec-multiple-input_1")) {
                assertThat(node.get("memberNodeIds").size()).isGreaterThanOrEqualTo(2);
                multiMemberFusion = true;
            }
        }
        assertThat(multiMemberFusion)
                .as("The fixture must exercise a fused Join and Calc")
                .isTrue();
        executeAndCheck(environment.loadPlan(PlanReference.fromJsonString(json)));
        executeAndCheck(environment.loadPlan(PlanReference.fromSmileBytes(smile)));
    }

    private static void executeAndCheck(CompiledPlan plan) throws Exception {
        plan.execute().await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("S"))
                .containsExactlyInAnyOrder("+I[1, 5]", "+I[2, 10]");
    }

    private static void createSource(TableEnvironment environment, String name, Row... rows) {
        String id = TestValuesTableFactory.registerData(Arrays.asList(rows));
        environment.executeSql(
                "CREATE TABLE "
                        + name
                        + " (a BIGINT, b BIGINT) WITH "
                        + "('connector'='values', 'data-id'='"
                        + id
                        + "', 'bounded'='true', 'runtime-source'='NewSource')");
    }
}

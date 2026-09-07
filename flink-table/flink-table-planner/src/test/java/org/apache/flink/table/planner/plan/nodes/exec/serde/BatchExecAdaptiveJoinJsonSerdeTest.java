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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.FlinkVersion;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies adaptive join strategy, estimates and configuration survive repeated plan restore. */
class BatchExecAdaptiveJoinJsonSerdeTest {
    @ParameterizedTest
    @EnumSource(
            value = OperatorType.class,
            names = {"ShuffleHashJoin", "SortMergeJoin"})
    void testAdaptiveJoinRoundTrip(OperatorType originalJoin) throws Exception {
        Configuration config = new Configuration();
        config.setString("table.exec.resource.hash-join.memory", "32 mb");
        config.setString("table.exec.sort.max-num-file-handles", "17");
        BatchExecAdaptiveJoin join =
                new BatchExecAdaptiveJoin(
                        config,
                        new JoinSpec(
                                FlinkJoinType.LEFT,
                                new int[] {1},
                                new int[] {0},
                                new boolean[] {true},
                                null),
                        16,
                        32,
                        123L,
                        456L,
                        false,
                        true,
                        Arrays.asList(InputProperty.DEFAULT, InputProperty.DEFAULT),
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD("left", DataTypes.BIGINT()),
                                                DataTypes.FIELD("right", DataTypes.STRING()))
                                        .getLogicalType(),
                        "Join(condition=[left = right])",
                        originalJoin);
        join.setInputEdges(Collections.emptyList());
        ExecNodeGraph graph =
                new ExecNodeGraph(FlinkVersion.current(), Collections.singletonList(join));
        SerdeContext context = JsonSerdeTestUtil.configuredSerdeContext();
        ObjectMapper mapper = new ObjectMapper();
        String json = JsonSerdeTestUtil.toJson(context, graph);
        JsonNode serialized = mapper.readTree(json).path("nodes").get(0);
        assertThat(serialized.path("type").asText()).isEqualTo("batch-exec-adaptive-join_1");
        assertThat(serialized.path("originalJoin").asText()).isEqualTo(originalJoin.name());
        assertThat(serialized.path("leftIsBuild").asBoolean()).isFalse();
        assertThat(serialized.path("tryDistinctBuildRow").asBoolean()).isTrue();
        assertThat(serialized.path("estimatedLeftAvgRowSize").asInt()).isEqualTo(16);
        assertThat(serialized.path("estimatedRightAvgRowSize").asInt()).isEqualTo(32);
        assertThat(serialized.path("estimatedLeftRowCount").asLong()).isEqualTo(123);
        assertThat(serialized.path("estimatedRightRowCount").asLong()).isEqualTo(456);
        assertThat(serialized.path("joinSpec").path("joinType").asText()).isEqualTo("LEFT");
        assertThat(
                        serialized
                                .path("configuration")
                                .path("table.exec.sort.max-num-file-handles")
                                .asText())
                .isEqualTo("17");
        for (int i = 0; i < 2; i++) {
            ExecNodeGraph restored = JsonSerdeTestUtil.toObject(context, json, ExecNodeGraph.class);
            assertThat(restored.getRootNodes().get(0)).isInstanceOf(BatchExecAdaptiveJoin.class);
            assertThat(restored.getRootNodes().get(0).getDescription())
                    .isEqualTo(join.getDescription());
            json = JsonSerdeTestUtil.toJson(context, restored);
            assertThat(mapper.readTree(json).path("nodes").get(0)).isEqualTo(serialized);
        }
    }
}

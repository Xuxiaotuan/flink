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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecUnion;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecValues;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Ensures fused subgraphs preserve shared sources and input-edge identity after restore. */
class BatchExecMultipleInputJsonSerdeTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSharedSourceAndInputOrderSurviveRoundTrip(boolean fusion) throws Exception {
        ExecNodeGraph graph = createGraph(fusion);
        SerdeContext context = JsonSerdeTestUtil.configuredSerdeContext();
        for (int i = 0; i < 2; i++) {
            String json = JsonSerdeTestUtil.toJson(context, graph);
            graph = JsonSerdeTestUtil.toObject(context, json, ExecNodeGraph.class);
            assertThat(graph.getRootNodes()).hasSize(2);
            BatchExecMultipleInput restored =
                    (BatchExecMultipleInput)
                            graph.getRootNodes().stream()
                                    .filter(n -> n instanceof BatchExecMultipleInput)
                                    .findFirst()
                                    .orElseThrow();
            assertThat(restored.getMemberExecNodes()).containsExactly(restored.getRootNode());
            assertThat(restored.getOriginalEdges().get(0))
                    .isSameAs(restored.getRootNode().getInputEdges().get(1));
            assertThat(restored.getOriginalEdges().get(1))
                    .isSameAs(restored.getRootNode().getInputEdges().get(0));
            assertThat(restored.getInputEdges().get(0).getSource())
                    .isSameAs(restored.getInputEdges().get(1).getSource());
            assertThat(restored.getOriginalEdges().get(0).getSource())
                    .isSameAs(restored.getInputEdges().get(0).getSource());
            assertThat(
                            restored.getPersistedConfig()
                                    .get(
                                            org.apache.flink.table.api.config.ExecutionConfigOptions
                                                    .TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED))
                    .isEqualTo(fusion);
            assertThat(
                            graph.getRootNodes().stream()
                                    .filter(n -> !(n instanceof BatchExecMultipleInput))
                                    .findFirst()
                                    .orElseThrow()
                                    .getInputEdges()
                                    .get(0)
                                    .getSource())
                    .isSameAs(restored.getInputEdges().get(0).getSource());
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "root",
                "missingMember",
                "duplicateMember",
                "index",
                "duplicateEdge",
                "target"
            })
    void testRejectsBrokenSubgraphReferences(String mutation) throws Exception {
        SerdeContext context = JsonSerdeTestUtil.configuredSerdeContext();
        JsonNode json =
                new ObjectMapper().readTree(JsonSerdeTestUtil.toJson(context, createGraph(false)));
        ObjectNode multiple = null;
        for (JsonNode node : json.get("nodes")) {
            if (node.has("memberNodeIds")) {
                multiple = (ObjectNode) node;
            }
        }
        assertThat(multiple).isNotNull();
        ArrayNode members = (ArrayNode) multiple.get("memberNodeIds");
        ArrayNode edges = (ArrayNode) multiple.get("originalEdgeReferences");
        String expected;
        switch (mutation) {
            case "root":
                multiple.put("rootNodeId", -1);
                expected = "Multiple-input root must be a member";
                break;
            case "missingMember":
                members.add(-1);
                expected = "Missing or invalid multiple-input member";
                break;
            case "duplicateMember":
                members.add(members.get(0).asInt());
                expected = "Duplicate multiple-input members";
                break;
            case "index":
                ((ObjectNode) edges.get(0)).put("inputIndex", 99);
                expected = "Invalid multiple-input edge index";
                break;
            case "duplicateEdge":
                edges.set(1, edges.get(0).deepCopy());
                expected = "Duplicate multiple-input edge reference";
                break;
            case "target":
                ((ObjectNode) edges.get(0)).put("targetNodeId", -1);
                expected = "Multiple-input edge target is not a member";
                break;
            default:
                throw new AssertionError(mutation);
        }
        assertThatThrownBy(
                        () ->
                                JsonSerdeTestUtil.toObject(
                                        context, json.toString(), ExecNodeGraph.class))
                .hasStackTraceContaining(expected);
    }

    private static ExecNodeGraph createGraph(boolean fusion) {
        Configuration config = new Configuration();
        config.setString("table.exec.operator-fusion-codegen.enabled", Boolean.toString(fusion));
        RowType type =
                (RowType) DataTypes.ROW(DataTypes.FIELD("v", DataTypes.BIGINT())).getLogicalType();
        BatchExecValues source =
                new BatchExecValues(config, Collections.emptyList(), type, "source");
        source.setInputEdges(Collections.emptyList());
        BatchExecUnion member =
                new BatchExecUnion(
                        config,
                        Arrays.asList(InputProperty.DEFAULT, InputProperty.DEFAULT),
                        type,
                        "member");
        ExecEdge first = ExecEdge.builder().source(source).target(member).build();
        ExecEdge second = ExecEdge.builder().source(source).target(member).build();
        member.setInputEdges(Arrays.asList(first, second));
        BatchExecMultipleInput multiple =
                new BatchExecMultipleInput(
                        config,
                        Arrays.asList(InputProperty.DEFAULT, InputProperty.DEFAULT),
                        member,
                        Collections.singletonList(member),
                        Arrays.asList(second, first),
                        "multiple");
        multiple.setInputEdges(
                Arrays.asList(
                        ExecEdge.builder().source(source).target(multiple).build(),
                        ExecEdge.builder().source(source).target(multiple).build()));
        BatchExecUnion otherRoot =
                new BatchExecUnion(
                        config, Collections.singletonList(InputProperty.DEFAULT), type, "other");
        otherRoot.setInputEdges(
                Collections.singletonList(
                        ExecEdge.builder().source(source).target(otherRoot).build()));
        return new ExecNodeGraph(Arrays.asList(multiple, otherRoot));
    }
}

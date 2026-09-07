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

import org.apache.flink.FlinkVersion;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.FusionCodegenUtil;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.generator.OneInputOpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.fusion.spec.OutputFusionCodegenSpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.fusion.OperatorFusionCodegenFactory;
import org.apache.flink.table.runtime.operators.multipleinput.BatchMultipleInputStreamOperatorFactory;
import org.apache.flink.table.runtime.operators.multipleinput.TableOperatorWrapperGenerator;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSelectionSpec;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Batch {@link ExecNode} for multiple input which contains a sub-graph of {@link ExecNode}s. The
 * root node of the sub-graph is {@link #rootNode}, and the leaf nodes of the sub-graph are the
 * output nodes of the {@link #getInputNodes()}.
 *
 * <p>The following example shows a graph of {@code ExecNode}s with multiple input node:
 *
 * <pre>{@code
 *          Sink
 *           |
 * +---------+--------+
 * |         |        |
 * |       Join       |
 * |     /     \      | BatchExecMultipleInput
 * |   Agg1    Agg2   |
 * |    |       |     |
 * +----+-------+-----+
 *      |       |
 * Exchange1 Exchange2
 *      |       |
 *    Scan1   Scan2
 * }</pre>
 *
 * <p>The multiple input node contains three nodes: `Join`, `Agg1` and `Agg2`. `Join` is the root
 * node ({@link #rootNode}) of the sub-graph, `Agg1` and `Agg2` are the leaf nodes of the sub-graph,
 * `Exchange1` and `Exchange2` are the input nodes of the multiple input node.
 */
@ExecNodeMetadata(
        name = "batch-exec-multiple-input",
        version = 1,
        consumedOptions = {"table.exec.operator-fusion-codegen.enabled"},
        minPlanVersion = FlinkVersion.v2_2,
        minStateVersion = FlinkVersion.v2_2)
public class BatchExecMultipleInput extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData>, SingleTransformationTranslator<RowData> {

    private static final String FIELD_NAME_ROOT_NODE_ID = "rootNodeId";
    private static final String FIELD_NAME_MEMBER_NODE_IDS = "memberNodeIds";
    private static final String FIELD_NAME_ORIGINAL_EDGE_REFERENCES = "originalEdgeReferences";

    @JsonProperty(FIELD_NAME_ROOT_NODE_ID)
    private final int rootNodeId;

    @JsonProperty(FIELD_NAME_MEMBER_NODE_IDS)
    private final List<Integer> memberNodeIds;

    @JsonProperty(FIELD_NAME_ORIGINAL_EDGE_REFERENCES)
    private final List<InputEdgeReference> originalEdgeReferences;

    private ExecNode<?> rootNode;
    private List<ExecNode<?>> memberExecNodes;
    private List<ExecEdge> originalEdges;

    public BatchExecMultipleInput(
            ReadableConfig tableConfig,
            List<InputProperty> inputProperties,
            ExecNode<?> rootNode,
            List<ExecNode<?>> memberExecNodes,
            List<ExecEdge> originalEdges,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecMultipleInput.class),
                ExecNodeContext.newPersistedConfig(BatchExecMultipleInput.class, tableConfig),
                inputProperties,
                (RowType) rootNode.getOutputType(),
                description,
                rootNode.getId(),
                memberExecNodes.stream().map(ExecNode::getId).collect(Collectors.toList()),
                originalEdges.stream()
                        .map(
                                edge ->
                                        new InputEdgeReference(
                                                edge.getTarget().getId(),
                                                edge.getTarget().getInputEdges().indexOf(edge)))
                        .collect(Collectors.toList()));
        this.rootNode = rootNode;
        this.memberExecNodes = memberExecNodes;
        this.originalEdges = originalEdges;
    }

    @JsonCreator
    public BatchExecMultipleInput(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_ROOT_NODE_ID) int rootNodeId,
            @JsonProperty(FIELD_NAME_MEMBER_NODE_IDS) List<Integer> memberNodeIds,
            @JsonProperty(FIELD_NAME_ORIGINAL_EDGE_REFERENCES)
                    List<InputEdgeReference> originalEdgeReferences) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.rootNodeId = rootNodeId;
        this.memberNodeIds =
                new ArrayList<>(checkNotNull(memberNodeIds, "Missing multiple-input members"));
        this.originalEdgeReferences =
                new ArrayList<>(
                        checkNotNull(
                                originalEdgeReferences, "Missing multiple-input edge references"));
        checkArgument(
                !memberNodeIds.isEmpty() && memberNodeIds.contains(rootNodeId),
                "Multiple-input root must be a member");
        checkArgument(
                new HashSet<>(memberNodeIds).size() == memberNodeIds.size(),
                "Duplicate multiple-input members");
        checkArgument(!memberNodeIds.contains(id), "Multiple-input node cannot contain itself");
        checkArgument(
                inputProperties.size() == originalEdgeReferences.size(),
                "Multiple-input edge count mismatch");
    }

    /** Binds all references to the graph's unique instances after its edges have been restored. */
    public void resolveSubgraph(Map<Integer, ExecNode<?>> nodes) {
        List<ExecNode<?>> members = new ArrayList<>();
        for (Integer memberId : memberNodeIds) {
            ExecNode<?> member = nodes.get(memberId);
            checkArgument(
                    member instanceof BatchExecNode,
                    "Missing or invalid multiple-input member: %s",
                    memberId);
            members.add(member);
        }
        ExecNode<?> root = nodes.get(rootNodeId);
        checkArgument(
                root != null && root.getOutputType().equals(getOutputType()),
                "Multiple-input root type mismatch");
        checkArgument(
                getInputEdges().size() == originalEdgeReferences.size(),
                "Multiple-input edge count mismatch");
        List<ExecEdge> originals = new ArrayList<>();
        for (int i = 0; i < originalEdgeReferences.size(); i++) {
            InputEdgeReference reference = originalEdgeReferences.get(i);
            ExecNode<?> target = nodes.get(reference.targetNodeId);
            checkArgument(members.contains(target), "Multiple-input edge target is not a member");
            checkArgument(
                    reference.inputIndex >= 0
                            && reference.inputIndex < target.getInputEdges().size(),
                    "Invalid multiple-input edge index");
            ExecEdge edge = target.getInputEdges().get(reference.inputIndex);
            checkArgument(
                    edge.getSource() == getInputEdges().get(i).getSource(),
                    "Multiple-input edge source mismatch");
            checkArgument(
                    !members.contains(edge.getSource()),
                    "Multiple-input boundary source cannot be a member");
            checkArgument(!originals.contains(edge), "Duplicate multiple-input edge reference");
            originals.add(edge);
        }
        Set<ExecNode<?>> visited = new HashSet<>();
        validateSubgraph(
                root, new HashSet<>(members), new HashSet<>(originals), visited, new HashSet<>());
        checkArgument(visited.size() == members.size(), "Unreachable multiple-input member");
        this.rootNode = root;
        this.memberExecNodes = members;
        this.originalEdges = originals;
    }

    private static void validateSubgraph(
            ExecNode<?> node,
            Set<ExecNode<?>> members,
            Set<ExecEdge> boundary,
            Set<ExecNode<?>> visited,
            Set<ExecNode<?>> active) {
        checkArgument(!active.contains(node), "Cyclic multiple-input subgraph");
        if (!visited.add(node)) {
            return;
        }
        active.add(node);
        for (ExecEdge edge : node.getInputEdges()) {
            if (members.contains(edge.getSource())) {
                validateSubgraph(edge.getSource(), members, boundary, visited, active);
            } else {
                checkArgument(boundary.contains(edge), "Unmapped multiple-input boundary edge");
            }
        }
        active.remove(node);
    }

    /** Identifies an existing edge without duplicating its source or target node. */
    public static final class InputEdgeReference {
        @JsonProperty("targetNodeId")
        private final int targetNodeId;

        @JsonProperty("inputIndex")
        private final int inputIndex;

        @JsonCreator
        public InputEdgeReference(
                @JsonProperty("targetNodeId") int targetNodeId,
                @JsonProperty("inputIndex") int inputIndex) {
            this.targetNodeId = targetNodeId;
            this.inputIndex = inputIndex;
        }
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        List<Transformation<?>> inputTransforms = new ArrayList<>();
        for (ExecEdge inputEdge : getInputEdges()) {
            inputTransforms.add(inputEdge.translateToPlan(planner));
        }
        final Transformation<?> outputTransform = rootNode.translateToPlan(planner);
        final int[] readOrders =
                getInputProperties().stream()
                        .map(InputProperty::getPriority)
                        .mapToInt(i -> i)
                        .toArray();

        StreamOperatorFactory<RowData> operatorFactory;
        int parallelism;
        int maxParallelism;
        long memoryBytes;
        ResourceSpec minResources = null;
        ResourceSpec preferredResources = null;

        boolean fusionCodegenEnabled = config.get(TABLE_EXEC_OPERATOR_FUSION_CODEGEN_ENABLED);
        // multiple operator fusion codegen
        if (fusionCodegenEnabled && allSupportFusionCodegen()) {
            final Map<ExecNode<?>, List<ExecEdge>> originalInputs = new IdentityHashMap<>();
            final Tuple2<OperatorFusionCodegenFactory<RowData>, Object> multipleOperatorTuple;
            try {
                final List<InputSelectionSpec> inputSelectionSpecs = new ArrayList<>();
                int i = 0;
                for (ExecEdge inputEdge : originalEdges) {
                    int multipleInputId = i + 1;
                    BatchExecNode<RowData> source = (BatchExecNode<RowData>) inputEdge.getSource();
                    BatchExecInputAdapter inputAdapter =
                            new BatchExecInputAdapter(
                                    multipleInputId,
                                    TableConfig.getDefault(),
                                    InputProperty.builder().priority(readOrders[i]).build(),
                                    source.getOutputType(),
                                    "BatchInputAdapter");
                    inputAdapter.setInputEdges(
                            Collections.singletonList(
                                    ExecEdge.builder()
                                            .source(source)
                                            .target(inputAdapter)
                                            .build()));

                    BatchExecNode<RowData> target = (BatchExecNode<RowData>) inputEdge.getTarget();
                    int edgeIdxInTargetNode = target.getInputEdges().indexOf(inputEdge);
                    checkArgument(edgeIdxInTargetNode >= 0);
                    originalInputs.computeIfAbsent(
                            target, node -> new ArrayList<>(node.getInputEdges()));

                    target.replaceInputEdge(
                            edgeIdxInTargetNode,
                            ExecEdge.builder().source(inputAdapter).target(target).build());

                    // The input id and read order
                    inputSelectionSpecs.add(new InputSelectionSpec(multipleInputId, readOrders[i]));
                    i++;
                }

                // Create a parent CodeGeneratorContext to ensure that all codes in multiple
                // operator
                // fusion codegen share the same ancestors CodeGeneratorContext. This way, we can
                // avoid naming conflicts.
                CodeGeneratorContext parentCtx =
                        new CodeGeneratorContext(
                                config, planner.getFlinkContext().getClassLoader());

                OpFusionCodegenSpecGenerator inputGenerator =
                        rootNode.translateToFusionCodegenSpec(planner, parentCtx);
                // wrap output operator spec generator of fusion codegen
                OpFusionCodegenSpecGenerator outputGenerator =
                        new OneInputOpFusionCodegenSpecGenerator(
                                inputGenerator,
                                0L,
                                (RowType) getOutputType(),
                                new OutputFusionCodegenSpec(
                                        new CodeGeneratorContext(
                                                config,
                                                planner.getFlinkContext().getClassLoader(),
                                                parentCtx)));
                inputGenerator.addOutput(1, outputGenerator);

                // generate fusion operator
                multipleOperatorTuple =
                        FusionCodegenUtil.generateFusionOperator(
                                outputGenerator, inputSelectionSpecs, parentCtx);
            } finally {
                // Input adapters belong to code generation, not to the persisted execution graph.
                originalInputs.forEach(ExecNode::setInputEdges);
            }
            operatorFactory = multipleOperatorTuple._1;

            Pair<Integer, Integer> parallelismPair = getInputMaxParallelism(inputTransforms);
            parallelism = parallelismPair.getLeft();
            maxParallelism = parallelismPair.getRight();
            memoryBytes = (long) multipleOperatorTuple._2;
        } else {
            final TableOperatorWrapperGenerator generator =
                    new TableOperatorWrapperGenerator(inputTransforms, outputTransform, readOrders);
            generator.generate();

            final List<Pair<Transformation<?>, InputSpec>> inputTransformAndInputSpecPairs =
                    generator.getInputTransformAndInputSpecPairs();
            operatorFactory =
                    new BatchMultipleInputStreamOperatorFactory(
                            inputTransformAndInputSpecPairs.stream()
                                    .map(Pair::getValue)
                                    .collect(Collectors.toList()),
                            generator.getHeadWrappers(),
                            generator.getTailWrapper());

            parallelism = generator.getParallelism();
            maxParallelism = generator.getMaxParallelism();
            final int memoryWeight = generator.getManagedMemoryWeight();
            memoryBytes = (long) memoryWeight << 20;

            minResources = generator.getMinResources();
            preferredResources = generator.getPreferredResources();
            // here set the all elements of InputTransformation and its id index indicates the order
            inputTransforms =
                    inputTransformAndInputSpecPairs.stream()
                            .map(Pair::getKey)
                            .collect(Collectors.toList());
        }

        final MultipleInputTransformation<RowData> multipleInputTransform =
                new MultipleInputTransformation<>(
                        createTransformationName(config),
                        operatorFactory,
                        InternalTypeInfo.of(getOutputType()),
                        parallelism,
                        false);
        multipleInputTransform.setDescription(createTransformationDescription(config));
        if (maxParallelism > 0) {
            multipleInputTransform.setMaxParallelism(maxParallelism);
        }

        for (Transformation input : inputTransforms) {
            multipleInputTransform.addInput(input);
        }

        // set resources
        if (minResources != null && preferredResources != null) {
            multipleInputTransform.setResources(minResources, preferredResources);
        }

        multipleInputTransform.setDescription(createTransformationDescription(config));
        ExecNodeUtil.setManagedMemoryWeight(multipleInputTransform, memoryBytes);

        // set chaining strategy for source chaining
        multipleInputTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);

        return multipleInputTransform;
    }

    @JsonIgnore
    public List<ExecEdge> getOriginalEdges() {
        return originalEdges;
    }

    @JsonIgnore
    public List<ExecNode<?>> getMemberExecNodes() {
        return memberExecNodes;
    }

    @VisibleForTesting
    @JsonIgnore
    public ExecNode<?> getRootNode() {
        return rootNode;
    }

    private boolean allSupportFusionCodegen() {
        return memberExecNodes.stream()
                .map(ExecNode::supportFusionCodegen)
                .reduce(true, (n1, n2) -> n1 && n2);
    }

    private Pair<Integer, Integer> getInputMaxParallelism(
            List<Transformation<?>> inputTransformations) {
        int parallelism = -1;
        int maxParallelism = -1;
        for (Transformation<?> transform : inputTransformations) {
            parallelism = Math.max(parallelism, transform.getParallelism());
            maxParallelism = Math.max(maxParallelism, transform.getMaxParallelism());
        }
        return Pair.of(parallelism, maxParallelism);
    }
}

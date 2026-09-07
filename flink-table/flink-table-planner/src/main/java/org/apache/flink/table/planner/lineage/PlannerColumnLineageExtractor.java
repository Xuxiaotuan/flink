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

package org.apache.flink.table.planner.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.ExpandTableScanShuttle;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNodeAndFieldIndex;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Extracts complete column dependencies from an unoptimized logical relational plan. */
@Internal
public final class PlannerColumnLineageExtractor {

    private PlannerColumnLineageExtractor() {}

    public static PlannerSinkColumnLineage extract(
            String sinkKey, List<String> outputFields, RelNode relNode) {
        Objects.requireNonNull(outputFields, "outputFields");
        Objects.requireNonNull(relNode, "relNode");

        final RelNode expandedRelNode = relNode.accept(new ExpandTableScanShuttle());
        final NodeLineage nodeLineage = extractNode(expandedRelNode);
        if (outputFields.size() != nodeLineage.fields.size()) {
            throw new TableLineageExtractionException(
                    "Sink '"
                            + sinkKey
                            + "' declares "
                            + outputFields.size()
                            + " output fields but the relational plan produces "
                            + nodeLineage.fields.size()
                            + ".");
        }

        final List<PlannerColumnLineageRelation> relations = new ArrayList<>(outputFields.size());
        for (int i = 0; i < outputFields.size(); i++) {
            relations.add(
                    nodeLineage
                            .fields
                            .get(i)
                            .toRelation(
                                    outputFields.get(i),
                                    nodeLineage.rowDependencies,
                                    nodeLineage.rowTransformations));
        }
        return new PlannerSinkColumnLineage(
                sinkKey, outputFields, new ArrayList<>(nodeLineage.sources), relations);
    }

    private static NodeLineage extractNode(RelNode relNode) {
        if (relNode instanceof RepeatUnion || relNode instanceof TableSpool) {
            throw unsupportedRecursiveCte(relNode);
        }
        if (relNode instanceof TableScan) {
            return extractTableScan((TableScan) relNode);
        }
        if (relNode instanceof Project) {
            return extractProject((Project) relNode);
        }
        if (relNode instanceof Calc) {
            return extractCalc((Calc) relNode);
        }
        if (relNode instanceof Filter) {
            return extractFilter((Filter) relNode);
        }
        if (relNode instanceof Join) {
            return extractJoin((Join) relNode);
        }
        if (relNode instanceof Aggregate) {
            return extractAggregate((Aggregate) relNode);
        }
        if (relNode instanceof Window) {
            return extractWindow((Window) relNode);
        }
        if (relNode instanceof Union) {
            return extractUnion((Union) relNode);
        }
        if (relNode instanceof Intersect || relNode instanceof Minus) {
            return extractMembershipSet((SetOp) relNode);
        }
        if (relNode instanceof Sort) {
            return extractSort((Sort) relNode);
        }
        if (relNode instanceof Values) {
            return constantFields(relNode);
        }
        if (relNode instanceof WatermarkAssigner) {
            // Watermarks change time attributes, but neither column values nor row membership.
            return extractNode(((WatermarkAssigner) relNode).getInput());
        }
        throw unsupported(relNode);
    }

    private static NodeLineage extractTableScan(TableScan scan) {
        final TableSourceTable sourceTable = scan.getTable().unwrap(TableSourceTable.class);
        if (sourceTable == null) {
            throw new TableLineageExtractionException(
                    "Table scan does not expose a TableSourceTable with a stable dataset identity: "
                            + scan.getTable().getClass().getSimpleName());
        }
        final ContextResolvedTable contextResolvedTable = sourceTable.contextResolvedTable();
        if (contextResolvedTable.isAnonymous()) {
            throw new TableLineageExtractionException(
                    "Anonymous source cannot provide a stable dataset identity for column lineage: "
                            + contextResolvedTable.getIdentifier().asSummaryString());
        }
        final PlannerLineageDataset dataset =
                new PlannerLineageDataset(contextResolvedTable.getIdentifier().toList());
        final List<String> fieldNames = scan.getRowType().getFieldNames();
        final List<FieldLineage> fields = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fields.add(
                    FieldLineage.input(
                            new PlannerColumnLineageInput(
                                    dataset,
                                    fieldName,
                                    PlannerColumnLineageDependencyType.DIRECT)));
        }
        return NodeLineage.source(fieldNames, fields, dataset);
    }

    private static NodeLineage extractProject(Project project) {
        final NodeLineage input = extractNode(project.getInput());
        final List<String> outputNames = project.getRowType().getFieldNames();
        final List<FieldLineage> fields = new ArrayList<>(project.getProjects().size());
        for (int i = 0; i < project.getProjects().size(); i++) {
            final RexNode expression = project.getProjects().get(i);
            final FieldLineage field = lineageFromExpression(expression, input);
            addAliasIfNeeded(field, expression, input, outputNames.get(i));
            fields.add(field);
        }
        return input.withFields(outputNames, fields);
    }

    private static NodeLineage extractCalc(Calc calc) {
        final NodeLineage input = extractNode(calc.getInput());
        final RexProgram program = calc.getProgram();
        final List<FieldLineage> fields = new ArrayList<>(program.getProjectList().size());
        final List<String> outputNames = calc.getRowType().getFieldNames();
        for (int i = 0; i < program.getProjectList().size(); i++) {
            final RexNode expression = program.expandLocalRef(program.getProjectList().get(i));
            final FieldLineage field = lineageFromExpression(expression, input);
            addAliasIfNeeded(field, expression, input, outputNames.get(i));
            fields.add(field);
        }
        NodeLineage result = input.withFields(outputNames, fields);
        if (program.getCondition() != null) {
            final RexNode condition = program.expandLocalRef(program.getCondition());
            result =
                    result.withRowDependency(
                            lineageFromExpression(condition, input),
                            PlannerColumnLineageTransformation.FILTER);
        }
        return result;
    }

    private static void addAliasIfNeeded(
            FieldLineage field, RexNode expression, NodeLineage input, String outputFieldName) {
        if (expression instanceof RexInputRef) {
            final String inputName = input.fieldNames.get(((RexInputRef) expression).getIndex());
            if (!inputName.equals(outputFieldName)) {
                field.transformations.add(PlannerColumnLineageTransformation.ALIAS);
            }
        }
    }

    private static NodeLineage extractFilter(Filter filter) {
        final NodeLineage input = extractNode(filter.getInput());
        return input.withRowDependency(
                lineageFromExpression(filter.getCondition(), input),
                PlannerColumnLineageTransformation.FILTER);
    }

    private static NodeLineage extractJoin(Join join) {
        final NodeLineage left = extractNode(join.getLeft());
        final NodeLineage right = extractNode(join.getRight());
        final List<String> conditionFields = new ArrayList<>(left.fieldNames);
        conditionFields.addAll(right.fieldNames);
        final NodeLineage combined = left.append(right, conditionFields);
        final NodeLineage result =
                combined.withRowDependency(
                        lineageFromExpression(join.getCondition(), combined),
                        PlannerColumnLineageTransformation.JOIN);
        return result.withFields(
                join.getRowType().getFieldNames(),
                join.getJoinType().projectsRight() ? result.fields : copyFields(left.fields));
    }

    private static NodeLineage extractMembershipSet(SetOp operation) {
        final List<NodeLineage> inputs = new ArrayList<>();
        final Set<PlannerColumnLineageInput> dependencies = new LinkedHashSet<>();
        final Set<PlannerColumnLineageTransformation> transformations = new LinkedHashSet<>();
        final Set<PlannerLineageDataset> sources = new LinkedHashSet<>();
        final int fieldCount = operation.getRowType().getFieldCount();
        for (RelNode node : operation.getInputs()) {
            final NodeLineage input = extractNode(node);
            if (input.fields.size() != fieldCount) {
                throw new TableLineageExtractionException(
                        "Set input field count does not match its output.");
            }
            inputs.add(input);
            addInputs(dependencies, input.rowDependencies);
            transformations.addAll(input.rowTransformations);
            sources.addAll(input.sources);
            // Every compared column controls whole-row membership, including ALL multiplicities.
            for (FieldLineage field : input.fields) {
                addIndirectInputs(dependencies, field.inputs);
            }
        }
        if (inputs.isEmpty()) {
            throw new TableLineageExtractionException(
                    "Set operation must have at least one input.");
        }
        final List<FieldLineage> fields = copyFields(inputs.get(0).fields);
        // Intersection is symmetric; subtraction can only return values from its first input.
        if (operation instanceof Intersect) {
            // Member metadata from only the first row must not survive merging other origins.
            for (FieldLineage field : fields) {
                field.nestedFields = null;
            }
            for (int n = 1; n < inputs.size(); n++) {
                for (int i = 0; i < fieldCount; i++) {
                    fields.get(i).merge(inputs.get(n).field(i));
                }
            }
        }
        transformations.add(PlannerColumnLineageTransformation.FILTER);
        return new NodeLineage(
                operation.getRowType().getFieldNames(),
                fields,
                dependencies,
                transformations,
                sources);
    }

    private static NodeLineage extractAggregate(Aggregate aggregate) {
        final NodeLineage input = extractNode(aggregate.getInput());
        final List<FieldLineage> fields = new ArrayList<>();
        final List<Integer> groupFields = aggregate.getGroupSet().asList();
        final Set<PlannerColumnLineageInput> rowDependencies =
                new LinkedHashSet<>(input.rowDependencies);
        final Set<PlannerColumnLineageTransformation> rowTransformations =
                new LinkedHashSet<>(input.rowTransformations);

        for (int groupField : groupFields) {
            final FieldLineage field = input.field(groupField).copy();
            field.transformations.add(PlannerColumnLineageTransformation.GROUP_BY);
            fields.add(field);
            addIndirectInputs(rowDependencies, input.field(groupField).inputs);
        }
        if (!groupFields.isEmpty()) {
            rowTransformations.add(PlannerColumnLineageTransformation.GROUP_BY);
        }

        for (AggregateCall call : aggregate.getAggCallList()) {
            final FieldLineage field = aggregateLineageFromFields(input, call.getArgList());
            if (call.filterArg >= 0) {
                field.addIndirect(input.field(call.filterArg));
                field.transformations.add(PlannerColumnLineageTransformation.FILTER);
            }
            for (RelFieldCollation collation : call.getCollation().getFieldCollations()) {
                field.addIndirect(input.field(collation.getFieldIndex()));
            }
            field.transformations.add(PlannerColumnLineageTransformation.AGGREGATION);
            fields.add(field);
        }
        return new NodeLineage(
                aggregate.getRowType().getFieldNames(),
                fields,
                rowDependencies,
                rowTransformations,
                input.sources);
    }

    private static NodeLineage extractWindow(Window window) {
        final NodeLineage input = extractNode(window.getInput());
        final NodeLineage expressionInput = input.appendConstants(window.getConstants());
        final List<FieldLineage> fields = copyFields(input.fields);
        for (Window.Group group : window.groups) {
            for (Window.RexWinAggCall call : group.aggCalls) {
                final FieldLineage field = aggregateLineage(expressionInput, call.getOperands());
                for (int partitionField : group.keys.asList()) {
                    field.addIndirect(expressionInput.field(partitionField));
                }
                for (RelFieldCollation collation : group.orderKeys.getFieldCollations()) {
                    field.addIndirect(expressionInput.field(collation.getFieldIndex()));
                }
                field.transformations.add(PlannerColumnLineageTransformation.WINDOW);
                fields.add(field);
            }
        }
        if (fields.size() != window.getRowType().getFieldCount()) {
            throw new TableLineageExtractionException(
                    "Window output field count cannot be matched to its aggregate calls.");
        }
        return input.withFields(window.getRowType().getFieldNames(), fields);
    }

    private static NodeLineage extractUnion(Union union) {
        final List<NodeLineage> inputs = new ArrayList<>(union.getInputs().size());
        for (RelNode input : union.getInputs()) {
            inputs.add(extractNode(input));
        }
        if (inputs.isEmpty()) {
            throw new TableLineageExtractionException("Union must have at least one input.");
        }

        final int fieldCount = union.getRowType().getFieldCount();
        final List<FieldLineage> fields = new ArrayList<>(fieldCount);
        final Set<PlannerColumnLineageInput> rowDependencies = new LinkedHashSet<>();
        final Set<PlannerColumnLineageTransformation> rowTransformations = new LinkedHashSet<>();
        final Set<PlannerLineageDataset> sources = new LinkedHashSet<>();
        for (NodeLineage input : inputs) {
            if (input.fields.size() != fieldCount) {
                throw new TableLineageExtractionException(
                        "Union input field count does not match its output.");
            }
            addInputs(rowDependencies, input.rowDependencies);
            rowTransformations.addAll(input.rowTransformations);
            sources.addAll(input.sources);
        }
        rowTransformations.add(PlannerColumnLineageTransformation.UNION);

        for (int i = 0; i < fieldCount; i++) {
            final FieldLineage merged = new FieldLineage();
            for (NodeLineage input : inputs) {
                merged.merge(input.field(i));
            }
            fields.add(merged);
        }
        return new NodeLineage(
                union.getRowType().getFieldNames(),
                fields,
                rowDependencies,
                rowTransformations,
                sources);
    }

    private static NodeLineage extractSort(Sort sort) {
        final NodeLineage input = extractNode(sort.getInput());
        final FieldLineage ordering = new FieldLineage();
        for (RelFieldCollation collation : sort.getCollation().getFieldCollations()) {
            ordering.addIndirect(input.field(collation.getFieldIndex()));
        }
        if (sort.offset != null) {
            ordering.addIndirect(lineageFromExpression(sort.offset, input));
        }
        if (sort.fetch != null) {
            ordering.addIndirect(lineageFromExpression(sort.fetch, input));
        }
        return input.withRowDependency(ordering, PlannerColumnLineageTransformation.SORT);
    }

    private static NodeLineage constantFields(RelNode relNode) {
        final List<FieldLineage> fields = new ArrayList<>(relNode.getRowType().getFieldCount());
        for (int i = 0; i < relNode.getRowType().getFieldCount(); i++) {
            fields.add(FieldLineage.constant());
        }
        return NodeLineage.of(relNode.getRowType().getFieldNames(), fields);
    }

    private static FieldLineage lineageFromExpression(RexNode expression, NodeLineage input) {
        return expression.accept(new LineageRexVisitor(input));
    }

    private static FieldLineage aggregateLineage(
            NodeLineage input, List<? extends RexNode> arguments) {
        final FieldLineage field = FieldLineage.system();
        for (RexNode argument : arguments) {
            field.merge(lineageFromExpression(argument, input));
        }
        field.useSystemOriginWithoutDirectInput();
        return field;
    }

    private static FieldLineage aggregateLineageFromFields(
            NodeLineage input, List<Integer> arguments) {
        final FieldLineage field = FieldLineage.system();
        for (int argument : arguments) {
            field.merge(input.field(argument));
        }
        field.useSystemOriginWithoutDirectInput();
        return field;
    }

    private static List<FieldLineage> copyFields(List<FieldLineage> fields) {
        final List<FieldLineage> copy = new ArrayList<>(fields.size());
        for (FieldLineage field : fields) {
            copy.add(field.copy());
        }
        return copy;
    }

    private static void addInputs(
            Set<PlannerColumnLineageInput> target, Iterable<PlannerColumnLineageInput> additions) {
        for (PlannerColumnLineageInput addition : additions) {
            target.add(addition);
        }
    }

    private static void addIndirectInputs(
            Set<PlannerColumnLineageInput> target,
            Iterable<PlannerColumnLineageInput> dependencies) {
        for (PlannerColumnLineageInput dependency : dependencies) {
            addInputs(target, Collections.singletonList(dependency.asIndirect()));
        }
    }

    private static TableLineageExtractionException unsupported(RelNode relNode) {
        return new TableLineageExtractionException(
                "Unsupported relational node for complete column lineage: "
                        + relNode.getClass().getSimpleName());
    }

    private static TableLineageExtractionException unsupportedRecursiveCte(RelNode relNode) {
        return new TableLineageExtractionException(
                "Recursive CTE node is not supported for complete column lineage: "
                        + relNode.getClass().getSimpleName());
    }

    private static TableLineageExtractionException unsupported(RexNode rexNode) {
        return new TableLineageExtractionException(
                "Unsupported Rex node for complete column lineage: "
                        + rexNode.getClass().getSimpleName());
    }

    private static final class LineageRexVisitor extends RexVisitorImpl<FieldLineage> {

        private final NodeLineage input;

        private LineageRexVisitor(NodeLineage input) {
            super(false);
            this.input = input;
        }

        @Override
        public FieldLineage visitInputRef(RexInputRef inputRef) {
            return input.field(inputRef.getIndex()).copy();
        }

        @Override
        public FieldLineage visitLocalRef(RexLocalRef localRef) {
            throw unsupported(localRef);
        }

        @Override
        public FieldLineage visitLiteral(RexLiteral literal) {
            return FieldLineage.constant();
        }

        @Override
        public FieldLineage visitCall(RexCall call) {
            if (call.getKind() == SqlKind.CASE) {
                return visitCase(call);
            }
            if (call.getKind() == SqlKind.ROW) {
                final FieldLineage field = new FieldLineage();
                final List<FieldLineage> nestedFields = new ArrayList<>();
                for (RexNode operand : call.getOperands()) {
                    final FieldLineage nested = operand.accept(this);
                    nestedFields.add(nested);
                    field.merge(nested);
                }
                field.nestedFields = nestedFields;
                field.transformations.add(PlannerColumnLineageTransformation.EXPRESSION);
                return field;
            }
            final FieldLineage field =
                    call.getOperands().isEmpty() ? FieldLineage.system() : new FieldLineage();
            for (RexNode operand : call.getOperands()) {
                field.merge(operand.accept(this));
            }
            if (call.getKind() == SqlKind.CAST) {
                field.transformations.add(PlannerColumnLineageTransformation.CAST);
            } else if (call.getKind() == SqlKind.OTHER_FUNCTION) {
                field.transformations.add(PlannerColumnLineageTransformation.UDF);
            } else {
                field.transformations.add(PlannerColumnLineageTransformation.EXPRESSION);
            }
            return field;
        }

        private FieldLineage visitCase(RexCall call) {
            final List<RexNode> operands = call.getOperands();
            if (operands.size() < 3 || operands.size() % 2 == 0) {
                throw unsupported(call);
            }

            final FieldLineage field = new FieldLineage();
            for (int i = 0; i < operands.size() - 1; i += 2) {
                field.addIndirect(operands.get(i).accept(this));
            }
            for (int i = 1; i < operands.size() - 1; i += 2) {
                field.merge(operands.get(i).accept(this));
            }
            field.merge(operands.get(operands.size() - 1).accept(this));
            field.transformations.add(PlannerColumnLineageTransformation.CONDITIONAL);
            return field;
        }

        @Override
        public FieldLineage visitOver(RexOver over) {
            final FieldLineage field = aggregateLineage(input, over.getOperands());
            for (RexNode partitionKey : over.getWindow().partitionKeys) {
                field.addIndirect(partitionKey.accept(this));
            }
            for (RexFieldCollation orderKey : over.getWindow().orderKeys) {
                field.addIndirect(orderKey.left.accept(this));
            }
            field.transformations.add(PlannerColumnLineageTransformation.WINDOW);
            return field;
        }

        @Override
        public FieldLineage visitCorrelVariable(RexCorrelVariable correlVariable) {
            throw unsupported(correlVariable);
        }

        @Override
        public FieldLineage visitDynamicParam(RexDynamicParam dynamicParam) {
            return FieldLineage.constant();
        }

        @Override
        public FieldLineage visitRangeRef(RexRangeRef rangeRef) {
            throw unsupported(rangeRef);
        }

        @Override
        public FieldLineage visitFieldAccess(RexFieldAccess fieldAccess) {
            final FieldLineage reference = fieldAccess.getReferenceExpr().accept(this);
            if (reference.nestedFields == null) {
                // Only explicit ROW constructors expose exact per-field dependencies here.
                throw unsupported(fieldAccess);
            }
            final FieldLineage field =
                    reference.nestedFields.get(fieldAccess.getField().getIndex()).copy();
            field.transformations.add(PlannerColumnLineageTransformation.EXPRESSION);
            return field;
        }

        @Override
        public FieldLineage visitSubQuery(RexSubQuery subQuery) {
            throw unsupported(subQuery);
        }

        @Override
        public FieldLineage visitTableInputRef(RexTableInputRef tableInputRef) {
            throw unsupported(tableInputRef);
        }

        @Override
        public FieldLineage visitPatternFieldRef(RexPatternFieldRef patternFieldRef) {
            throw unsupported(patternFieldRef);
        }

        @Override
        public FieldLineage visitLambda(RexLambda lambda) {
            throw unsupported(lambda);
        }

        @Override
        public FieldLineage visitLambdaRef(RexLambdaRef lambdaRef) {
            throw unsupported(lambdaRef);
        }

        @Override
        public FieldLineage visitNodeAndFieldIndex(RexNodeAndFieldIndex nodeAndFieldIndex) {
            throw unsupported(nodeAndFieldIndex);
        }
    }

    private static final class NodeLineage {

        private final List<String> fieldNames;
        private final List<FieldLineage> fields;
        private final Set<PlannerColumnLineageInput> rowDependencies;
        private final Set<PlannerColumnLineageTransformation> rowTransformations;
        private final Set<PlannerLineageDataset> sources;

        private NodeLineage(
                List<String> fieldNames,
                List<FieldLineage> fields,
                Set<PlannerColumnLineageInput> rowDependencies,
                Set<PlannerColumnLineageTransformation> rowTransformations,
                Set<PlannerLineageDataset> sources) {
            if (fieldNames.size() != fields.size()) {
                throw new TableLineageExtractionException(
                        "Relational field names cannot be matched to extracted column lineage.");
            }
            this.fieldNames = new ArrayList<>(fieldNames);
            this.fields = fields;
            this.rowDependencies = rowDependencies;
            this.rowTransformations = rowTransformations;
            this.sources = sources;
        }

        private static NodeLineage of(List<String> fieldNames, List<FieldLineage> fields) {
            return new NodeLineage(
                    fieldNames,
                    fields,
                    new LinkedHashSet<>(),
                    new LinkedHashSet<>(),
                    new LinkedHashSet<>());
        }

        private static NodeLineage source(
                List<String> fieldNames, List<FieldLineage> fields, PlannerLineageDataset source) {
            return new NodeLineage(
                    fieldNames,
                    fields,
                    new LinkedHashSet<>(),
                    new LinkedHashSet<>(),
                    new LinkedHashSet<>(Collections.singletonList(source)));
        }

        private FieldLineage field(int index) {
            if (index < 0 || index >= fields.size()) {
                throw new TableLineageExtractionException(
                        "Expression references input field "
                                + index
                                + " but only "
                                + fields.size()
                                + " fields are available.");
            }
            return fields.get(index);
        }

        private NodeLineage withFields(List<String> names, List<FieldLineage> newFields) {
            return new NodeLineage(
                    names,
                    newFields,
                    new LinkedHashSet<>(rowDependencies),
                    new LinkedHashSet<>(rowTransformations),
                    new LinkedHashSet<>(sources));
        }

        private NodeLineage withRowDependency(
                FieldLineage dependency, PlannerColumnLineageTransformation transformation) {
            final Set<PlannerColumnLineageInput> dependencies =
                    new LinkedHashSet<>(rowDependencies);
            addIndirectInputs(dependencies, dependency.inputs);
            final Set<PlannerColumnLineageTransformation> transformations =
                    new LinkedHashSet<>(rowTransformations);
            transformations.add(transformation);
            return new NodeLineage(
                    fieldNames,
                    fields,
                    dependencies,
                    transformations,
                    new LinkedHashSet<>(sources));
        }

        private NodeLineage append(NodeLineage other, List<String> outputNames) {
            final List<FieldLineage> appendedFields = copyFields(fields);
            appendedFields.addAll(copyFields(other.fields));
            final Set<PlannerColumnLineageInput> dependencies =
                    new LinkedHashSet<>(rowDependencies);
            addInputs(dependencies, other.rowDependencies);
            final Set<PlannerColumnLineageTransformation> transformations =
                    new LinkedHashSet<>(rowTransformations);
            transformations.addAll(other.rowTransformations);
            final Set<PlannerLineageDataset> appendedSources = new LinkedHashSet<>(sources);
            appendedSources.addAll(other.sources);
            return new NodeLineage(
                    outputNames, appendedFields, dependencies, transformations, appendedSources);
        }

        private NodeLineage appendConstants(List<? extends RexLiteral> constants) {
            final List<String> names = new ArrayList<>(fieldNames);
            final List<FieldLineage> fieldsWithConstants = copyFields(fields);
            for (int i = 0; i < constants.size(); i++) {
                names.add("$constant" + i);
                fieldsWithConstants.add(FieldLineage.constant());
            }
            return withFields(names, fieldsWithConstants);
        }
    }

    private static final class FieldLineage {

        private final Set<PlannerColumnLineageInput> inputs = new LinkedHashSet<>();
        private final Set<PlannerColumnLineageTransformation> transformations =
                new LinkedHashSet<>();
        private PlannerColumnLineageOrigin origin = PlannerColumnLineageOrigin.CONSTANT;
        private List<FieldLineage> nestedFields;

        private static FieldLineage input(PlannerColumnLineageInput input) {
            final FieldLineage lineage = new FieldLineage();
            lineage.origin = PlannerColumnLineageOrigin.INPUT_FIELDS;
            lineage.inputs.add(input);
            return lineage;
        }

        private static FieldLineage constant() {
            return new FieldLineage();
        }

        private static FieldLineage system() {
            final FieldLineage lineage = new FieldLineage();
            lineage.origin = PlannerColumnLineageOrigin.SYSTEM;
            return lineage;
        }

        private FieldLineage copy() {
            final FieldLineage copy = new FieldLineage();
            copy.origin = origin;
            addInputs(copy.inputs, inputs);
            copy.transformations.addAll(transformations);
            copy.nestedFields = nestedFields == null ? null : copyFields(nestedFields);
            return copy;
        }

        private void merge(FieldLineage other) {
            addInputs(inputs, other.inputs);
            transformations.addAll(other.transformations);
            if (hasDirectInput()) {
                origin = PlannerColumnLineageOrigin.INPUT_FIELDS;
            } else if (origin == PlannerColumnLineageOrigin.SYSTEM
                    || other.origin == PlannerColumnLineageOrigin.SYSTEM) {
                origin = PlannerColumnLineageOrigin.SYSTEM;
            }
        }

        private void addIndirect(FieldLineage dependency) {
            addIndirectInputs(inputs, dependency.inputs);
        }

        private void useSystemOriginWithoutDirectInput() {
            if (!hasDirectInput()) {
                origin = PlannerColumnLineageOrigin.SYSTEM;
            }
        }

        private boolean hasDirectInput() {
            return inputs.stream()
                    .anyMatch(
                            input ->
                                    input.getDependencyType()
                                            == PlannerColumnLineageDependencyType.DIRECT);
        }

        private PlannerColumnLineageRelation toRelation(
                String outputField,
                Set<PlannerColumnLineageInput> rowDependencies,
                Set<PlannerColumnLineageTransformation> rowTransformations) {
            final FieldLineage complete = copy();
            addInputs(complete.inputs, rowDependencies);
            complete.transformations.addAll(rowTransformations);
            return new PlannerColumnLineageRelation(
                    outputField,
                    new ArrayList<>(complete.inputs),
                    complete.origin,
                    new ArrayList<>(complete.transformations));
        }
    }
}

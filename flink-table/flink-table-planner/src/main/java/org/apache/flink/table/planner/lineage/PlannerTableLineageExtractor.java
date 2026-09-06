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
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.ExpandTableScanShuttle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Extracts source identities without requiring column-expression lineage. */
@Internal
public final class PlannerTableLineageExtractor {
    private PlannerTableLineageExtractor() {}

    public static List<PlannerLineageDataset> extract(RelNode root) {
        Set<PlannerLineageDataset> sources = new LinkedHashSet<>();
        collect(root.accept(new ExpandTableScanShuttle()), sources);
        return new ArrayList<>(sources);
    }

    private static void collect(RelNode node, Set<PlannerLineageDataset> sources) {
        if (node instanceof TableScan) {
            TableSourceTable table = ((TableScan) node).getTable().unwrap(TableSourceTable.class);
            if (table == null || table.contextResolvedTable().isAnonymous()) {
                throw new TableLineageExtractionException(
                        "Table lineage requires a stable catalog source identity.");
            }
            sources.add(
                    new PlannerLineageDataset(
                            table.contextResolvedTable().getIdentifier().toList()));
            return;
        }
        if (!(node instanceof Project
                || node instanceof Calc
                || node instanceof Filter
                || node instanceof Join
                || node instanceof Aggregate
                || node instanceof Window
                || node instanceof SetOp
                || node instanceof Sort
                || node instanceof Values
                || node instanceof WatermarkAssigner)) {
            throw new TableLineageExtractionException(
                    "Unsupported logical table lineage node: " + node.getClass().getSimpleName());
        }
        node.accept(
                new RexShuttle() {
                    @Override
                    public org.apache.calcite.rex.RexNode visitSubQuery(RexSubQuery subQuery) {
                        collect(subQuery.rel.accept(new ExpandTableScanShuttle()), sources);
                        return super.visitSubQuery(subQuery);
                    }
                });
        for (RelNode input : node.getInputs()) {
            collect(input, sources);
        }
    }
}

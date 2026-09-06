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

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.Internal;

import java.util.List;

/** Captured lineage with explicit completeness, independent of job execution success. */
@Internal
public final class LineageGraphObservation implements LineageGraph {
    private final LineageGraph graph;
    private final String tableStatus;
    private final String columnStatus;
    private final List<String> issues;

    public LineageGraphObservation(
            LineageGraph graph, String tableStatus, String columnStatus, List<String> issues) {
        this.graph = graph;
        this.tableStatus = tableStatus;
        this.columnStatus = columnStatus;
        this.issues = List.copyOf(issues);
    }

    public String getTableStatus() {
        return tableStatus;
    }

    public String getColumnStatus() {
        return columnStatus;
    }

    public List<String> getIssues() {
        return issues;
    }

    @Override
    public List<SourceLineageVertex> sources() {
        return graph.sources();
    }

    @Override
    public List<LineageVertex> sinks() {
        return graph.sinks();
    }

    @Override
    public List<LineageEdge> relations() {
        return graph.relations();
    }

    @Override
    public List<ColumnLineageRelation> columnRelations() {
        return graph.columnRelations();
    }
}

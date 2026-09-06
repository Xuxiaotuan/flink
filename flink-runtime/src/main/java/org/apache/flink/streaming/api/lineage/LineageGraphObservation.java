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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Captured lineage with explicit completeness, independent of job execution success. */
@Internal
public final class LineageGraphObservation implements LineageGraph {
    private final LineageGraph graph;
    private final String tableStatus;
    private final String columnStatus;
    private final List<String> issues;
    private final Map<String, Map<String, String>> columnStatuses;

    public LineageGraphObservation(
            LineageGraph graph, String tableStatus, String columnStatus, List<String> issues) {
        this(graph, tableStatus, columnStatus, issues, Collections.emptyMap());
    }

    public LineageGraphObservation(
            LineageGraph graph,
            String tableStatus,
            String columnStatus,
            List<String> issues,
            Map<String, Map<String, String>> columnStatuses) {
        this.graph = graph;
        this.tableStatus = tableStatus;
        this.columnStatus = columnStatus;
        this.issues = List.copyOf(issues);
        Map<String, Map<String, String>> copy = new LinkedHashMap<>();
        columnStatuses.forEach((namespace, statuses) -> copy.put(namespace, Map.copyOf(statuses)));
        this.columnStatuses = Collections.unmodifiableMap(copy);
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

    /** Column completeness per namespace and output dataset, covering every writer. */
    public Map<String, Map<String, String>> getColumnStatuses() {
        return columnStatuses;
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

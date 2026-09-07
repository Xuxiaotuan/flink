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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

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
    private final Map<String, Map<String, String>> tableStatuses;

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
        this(graph, tableStatus, columnStatus, issues, columnStatuses, Collections.emptyMap());
    }

    public LineageGraphObservation(
            LineageGraph graph,
            String tableStatus,
            String columnStatus,
            List<String> issues,
            Map<String, Map<String, String>> columnStatuses,
            Map<String, Map<String, String>> tableStatuses) {
        this.graph = graph;
        this.tableStatus = tableStatus;
        this.columnStatus = columnStatus;
        this.issues = List.copyOf(issues);
        Map<String, Map<String, String>> copy = new LinkedHashMap<>();
        columnStatuses.forEach((namespace, statuses) -> copy.put(namespace, Map.copyOf(statuses)));
        this.columnStatuses = Collections.unmodifiableMap(copy);
        Map<String, Map<String, String>> tableCopy = new LinkedHashMap<>();
        tableStatuses.forEach(
                (namespace, statuses) -> tableCopy.put(namespace, Map.copyOf(statuses)));
        this.tableStatuses = Collections.unmodifiableMap(tableCopy);
    }

    /** Verified table coverage per native output identity, covering every writer. */
    @JsonIgnore
    public Map<String, Map<String, String>> getTableStatuses() {
        return tableStatuses;
    }

    @JsonIgnore
    public String getTableStatus() {
        return tableStatus;
    }

    @JsonIgnore
    public String getColumnStatus() {
        return columnStatus;
    }

    @JsonIgnore
    public List<String> getIssues() {
        return issues;
    }

    /** Column completeness per namespace and output dataset, covering every writer. */
    @JsonIgnore
    public Map<String, Map<String, String>> getColumnStatuses() {
        return columnStatuses;
    }

    @Override
    @JsonProperty("sources")
    public List<SourceLineageVertex> sources() {
        return graph.sources();
    }

    @Override
    @JsonProperty("sinks")
    public List<LineageVertex> sinks() {
        return graph.sinks();
    }

    @Override
    @JsonProperty("lineageEdges")
    public List<LineageEdge> relations() {
        return graph.relations();
    }

    @Override
    @JsonProperty("columnLineageRelations")
    public List<ColumnLineageRelation> columnRelations() {
        return graph.columnRelations();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.listener.CatalogContext;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Frozen logical source metadata for a scan removed by the optimizer, not a runtime read. */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PlannerPrunedSource implements TableLineageDataset {

    private final PlannerLineageDataset dataset;
    private final String namespace;
    private final ResolvedCatalogTable table;

    @JsonCreator
    public PlannerPrunedSource(
            @JsonProperty("dataset") PlannerLineageDataset dataset,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("table") ResolvedCatalogTable table) {
        if (dataset == null || namespace == null || namespace.trim().isEmpty() || table == null) {
            throw new TableLineageExtractionException(
                    "Pruned source snapshot must contain identity, namespace and source schema; recompile the plan.");
        }
        this.dataset = dataset;
        this.namespace = namespace;
        this.table = table;
    }

    @JsonProperty("dataset")
    public PlannerLineageDataset getDataset() {
        return dataset;
    }

    @Override
    public String name() {
        return dataset.asSerializableString();
    }

    @Override
    @JsonProperty("namespace")
    public String namespace() {
        return namespace;
    }

    @Override
    @JsonProperty("table")
    public ResolvedCatalogTable table() {
        return table;
    }

    @Override
    public CatalogContext catalogContext() {
        // Do not look up today's catalog when restoring a frozen logical dependency.
        return CatalogContext.createContext(dataset.getQualifiedName().get(0), null);
    }

    @Override
    public ObjectPath objectPath() {
        return new ObjectPath(dataset.getQualifiedName().get(1), dataset.getQualifiedName().get(2));
    }

    @Override
    public List<String> fieldNames() {
        return table.getResolvedSchema().getColumnNames();
    }

    @Override
    public Map<String, LineageDatasetFacet> facets() {
        return Collections.emptyMap();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PlannerPrunedSource)) {
            return false;
        }
        final PlannerPrunedSource that = (PlannerPrunedSource) other;
        return dataset.equals(that.dataset)
                && namespace.equals(that.namespace)
                && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataset, namespace, table);
    }
}

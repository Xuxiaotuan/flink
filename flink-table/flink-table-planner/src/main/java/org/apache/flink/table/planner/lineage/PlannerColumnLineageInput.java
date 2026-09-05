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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** Input field reference in the planner-private column lineage model. */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PlannerColumnLineageInput {

    private static final String FIELD_NAME_DATASET = "dataset";
    private static final String FIELD_NAME_FIELD = "field";
    private static final String FIELD_NAME_DEPENDENCY_TYPE = "dependencyType";

    private final PlannerLineageDataset dataset;
    private final String fieldName;
    private final PlannerColumnLineageDependencyType dependencyType;

    @JsonCreator
    public PlannerColumnLineageInput(
            @JsonProperty(FIELD_NAME_DATASET) PlannerLineageDataset dataset,
            @JsonProperty(FIELD_NAME_FIELD) String fieldName,
            @JsonProperty(FIELD_NAME_DEPENDENCY_TYPE)
                    PlannerColumnLineageDependencyType dependencyType) {
        this.dataset = Objects.requireNonNull(dataset, "dataset");
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new TableLineageExtractionException("Input field name must not be blank.");
        }
        this.fieldName = fieldName;
        this.dependencyType = Objects.requireNonNull(dependencyType, "dependencyType");
    }

    @JsonProperty(FIELD_NAME_DATASET)
    public PlannerLineageDataset getDataset() {
        return dataset;
    }

    @JsonProperty(FIELD_NAME_FIELD)
    public String getFieldName() {
        return fieldName;
    }

    @JsonProperty(FIELD_NAME_DEPENDENCY_TYPE)
    public PlannerColumnLineageDependencyType getDependencyType() {
        return dependencyType;
    }

    PlannerColumnLineageInput asIndirect() {
        if (dependencyType == PlannerColumnLineageDependencyType.INDIRECT) {
            return this;
        }
        return new PlannerColumnLineageInput(
                dataset, fieldName, PlannerColumnLineageDependencyType.INDIRECT);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PlannerColumnLineageInput)) {
            return false;
        }
        final PlannerColumnLineageInput that = (PlannerColumnLineageInput) object;
        return dataset.equals(that.dataset)
                && fieldName.equals(that.fieldName)
                && dependencyType == that.dependencyType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataset, fieldName, dependencyType);
    }
}

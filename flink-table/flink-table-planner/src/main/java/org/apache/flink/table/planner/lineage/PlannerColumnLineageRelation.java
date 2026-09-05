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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Lineage of one output field before it is mapped to a runtime lineage dataset. */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PlannerColumnLineageRelation {

    private static final String FIELD_NAME_OUTPUT_FIELD = "outputField";
    private static final String FIELD_NAME_INPUTS = "inputs";
    private static final String FIELD_NAME_ORIGIN = "origin";
    private static final String FIELD_NAME_TRANSFORMATIONS = "transformations";

    private final String outputField;
    private final List<PlannerColumnLineageInput> inputs;
    private final PlannerColumnLineageOrigin origin;
    private final List<PlannerColumnLineageTransformation> transformations;

    @JsonCreator
    public PlannerColumnLineageRelation(
            @JsonProperty(FIELD_NAME_OUTPUT_FIELD) String outputField,
            @JsonProperty(FIELD_NAME_INPUTS) List<PlannerColumnLineageInput> inputs,
            @JsonProperty(FIELD_NAME_ORIGIN) PlannerColumnLineageOrigin origin,
            @JsonProperty(FIELD_NAME_TRANSFORMATIONS)
                    List<PlannerColumnLineageTransformation> transformations) {
        if (outputField == null || outputField.trim().isEmpty()) {
            throw new TableLineageExtractionException("Output field name must not be blank.");
        }
        if (inputs == null) {
            throw new TableLineageExtractionException("Column lineage inputs must not be null.");
        }
        if (transformations == null) {
            throw new TableLineageExtractionException(
                    "Column lineage transformations must not be null.");
        }
        this.outputField = outputField;
        this.inputs = Collections.unmodifiableList(new ArrayList<>(inputs));
        this.origin = Objects.requireNonNull(origin, "origin");
        this.transformations = Collections.unmodifiableList(new ArrayList<>(transformations));
        final boolean hasDirectInput =
                inputs.stream()
                        .anyMatch(
                                input ->
                                        input.getDependencyType()
                                                == PlannerColumnLineageDependencyType.DIRECT);
        if (origin != PlannerColumnLineageOrigin.INPUT_FIELDS && hasDirectInput) {
            throw new TableLineageExtractionException(
                    "Constant and system output fields must not contain direct input fields.");
        }
        if (origin == PlannerColumnLineageOrigin.INPUT_FIELDS && !hasDirectInput) {
            throw new TableLineageExtractionException(
                    "Input-derived output field must contain at least one direct input field.");
        }
    }

    @JsonProperty(FIELD_NAME_OUTPUT_FIELD)
    public String getOutputField() {
        return outputField;
    }

    @JsonProperty(FIELD_NAME_INPUTS)
    public List<PlannerColumnLineageInput> getInputs() {
        return inputs;
    }

    @JsonProperty(FIELD_NAME_ORIGIN)
    public PlannerColumnLineageOrigin getOrigin() {
        return origin;
    }

    @JsonProperty(FIELD_NAME_TRANSFORMATIONS)
    public List<PlannerColumnLineageTransformation> getTransformations() {
        return transformations;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PlannerColumnLineageRelation)) {
            return false;
        }
        final PlannerColumnLineageRelation that = (PlannerColumnLineageRelation) object;
        return outputField.equals(that.outputField)
                && inputs.equals(that.inputs)
                && origin == that.origin
                && transformations.equals(that.transformations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputField, inputs, origin, transformations);
    }
}

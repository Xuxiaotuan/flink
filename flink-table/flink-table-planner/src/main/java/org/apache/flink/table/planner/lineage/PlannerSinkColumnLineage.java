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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Planner-private column lineage isolated to one sink. */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PlannerSinkColumnLineage {

    private static final String FIELD_NAME_SINK_KEY = "sinkKey";
    private static final String FIELD_NAME_EXPECTED_OUTPUT_FIELDS = "expectedOutputFields";
    private static final String FIELD_NAME_EXPECTED_SOURCES = "expectedSources";
    private static final String FIELD_NAME_RELATIONS = "relations";
    private static final String FIELD_NAME_PRUNED_SOURCES = "prunedSources";

    private final String sinkKey;
    private final List<String> expectedOutputFields;
    private final List<PlannerLineageDataset> expectedSources;
    private final List<PlannerColumnLineageRelation> relations;
    @Nullable private final List<PlannerPrunedSource> prunedSources;

    public PlannerSinkColumnLineage(
            String sinkKey,
            List<String> expectedOutputFields,
            List<PlannerLineageDataset> expectedSources,
            List<PlannerColumnLineageRelation> relations) {
        this(sinkKey, expectedOutputFields, expectedSources, relations, null);
    }

    @JsonCreator
    public PlannerSinkColumnLineage(
            @JsonProperty(FIELD_NAME_SINK_KEY) String sinkKey,
            @JsonProperty(FIELD_NAME_EXPECTED_OUTPUT_FIELDS) List<String> expectedOutputFields,
            @JsonProperty(FIELD_NAME_EXPECTED_SOURCES) List<PlannerLineageDataset> expectedSources,
            @JsonProperty(FIELD_NAME_RELATIONS) List<PlannerColumnLineageRelation> relations,
            @Nullable @JsonProperty(FIELD_NAME_PRUNED_SOURCES)
                    List<PlannerPrunedSource> prunedSources) {
        if (sinkKey == null || sinkKey.trim().isEmpty()) {
            throw new TableLineageExtractionException("Sink key must not be blank.");
        }
        if (expectedOutputFields == null) {
            throw new TableLineageExtractionException("Expected output fields must not be null.");
        }
        if (expectedSources == null) {
            throw new TableLineageExtractionException("Expected sources must not be null.");
        }
        if (relations == null) {
            throw new TableLineageExtractionException("Column lineage relations must not be null.");
        }

        final Set<String> expectedFields = new HashSet<>();
        for (String expectedOutputField : expectedOutputFields) {
            if (expectedOutputField == null || expectedOutputField.trim().isEmpty()) {
                throw new TableLineageExtractionException(
                        "Expected output field name must not be blank.");
            }
            if (!expectedFields.add(expectedOutputField)) {
                throw new TableLineageExtractionException(
                        "Sink '"
                                + sinkKey
                                + "' has duplicate expected output field '"
                                + expectedOutputField
                                + "'.");
            }
        }

        final Set<PlannerLineageDataset> sourceIdentities = new HashSet<>();
        for (PlannerLineageDataset expectedSource : expectedSources) {
            if (expectedSource == null) {
                throw new TableLineageExtractionException(
                        "Expected source identity must not be null.");
            }
            if (!sourceIdentities.add(expectedSource)) {
                throw new TableLineageExtractionException(
                        "Sink '"
                                + sinkKey
                                + "' has duplicate expected source identity '"
                                + expectedSource.asSerializableString()
                                + "'.");
            }
        }

        final Set<String> relationOutputFields = new HashSet<>();
        for (PlannerColumnLineageRelation relation : relations) {
            if (relation == null) {
                throw new TableLineageExtractionException(
                        "Column lineage relation must not be null.");
            }
            if (!expectedFields.contains(relation.getOutputField())) {
                throw new TableLineageExtractionException(
                        "Sink '"
                                + sinkKey
                                + "' has lineage for '"
                                + relation.getOutputField()
                                + "', which is not an expected output.");
            }
            if (!relationOutputFields.add(relation.getOutputField())) {
                throw new TableLineageExtractionException(
                        "Sink '"
                                + sinkKey
                                + "' has duplicate output field lineage for '"
                                + relation.getOutputField()
                                + "'.");
            }
            for (PlannerColumnLineageInput input : relation.getInputs()) {
                if (!sourceIdentities.contains(input.getDataset())) {
                    throw new TableLineageExtractionException(
                            "Sink '"
                                    + sinkKey
                                    + "' has input source '"
                                    + input.getDataset().asSerializableString()
                                    + "' which is not an expected source identity.");
                }
            }
        }
        for (String expectedOutputField : expectedOutputFields) {
            if (!relationOutputFields.contains(expectedOutputField)) {
                throw new TableLineageExtractionException(
                        "Sink '"
                                + sinkKey
                                + "' has expected output '"
                                + expectedOutputField
                                + "' with no lineage relation.");
            }
        }
        this.sinkKey = sinkKey;
        this.expectedOutputFields =
                Collections.unmodifiableList(new ArrayList<>(expectedOutputFields));
        this.expectedSources = Collections.unmodifiableList(new ArrayList<>(expectedSources));
        this.relations = Collections.unmodifiableList(new ArrayList<>(relations));
        this.prunedSources =
                prunedSources == null
                        ? null
                        : Collections.unmodifiableList(new ArrayList<>(prunedSources));
    }

    @JsonProperty(FIELD_NAME_SINK_KEY)
    public String getSinkKey() {
        return sinkKey;
    }

    @JsonProperty(FIELD_NAME_EXPECTED_OUTPUT_FIELDS)
    public List<String> getExpectedOutputFields() {
        return expectedOutputFields;
    }

    @JsonProperty(FIELD_NAME_EXPECTED_SOURCES)
    public List<PlannerLineageDataset> getExpectedSources() {
        return expectedSources;
    }

    @JsonProperty(FIELD_NAME_RELATIONS)
    public List<PlannerColumnLineageRelation> getRelations() {
        return relations;
    }

    /**
     * Null identifies a plan without optimizer pruning evidence; empty proves no source was pruned.
     */
    @Nullable
    @JsonProperty(FIELD_NAME_PRUNED_SOURCES)
    public List<PlannerPrunedSource> getPrunedSources() {
        return prunedSources;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PlannerSinkColumnLineage)) {
            return false;
        }
        final PlannerSinkColumnLineage that = (PlannerSinkColumnLineage) object;
        return sinkKey.equals(that.sinkKey)
                && expectedOutputFields.equals(that.expectedOutputFields)
                && expectedSources.equals(that.expectedSources)
                && relations.equals(that.relations)
                && Objects.equals(prunedSources, that.prunedSources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sinkKey, expectedOutputFields, expectedSources, relations, prunedSources);
    }
}

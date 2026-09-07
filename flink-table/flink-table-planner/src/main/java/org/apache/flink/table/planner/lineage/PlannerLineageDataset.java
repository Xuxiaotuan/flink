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
import org.apache.flink.table.catalog.ObjectIdentifier;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Stable logical dataset reference retained until physical lineage identities are available. */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PlannerLineageDataset {

    private static final String FIELD_NAME_QUALIFIED_NAME = "qualifiedName";

    private final List<String> qualifiedName;

    @JsonCreator
    public PlannerLineageDataset(
            @JsonProperty(FIELD_NAME_QUALIFIED_NAME) List<String> qualifiedName) {
        if (qualifiedName == null
                || qualifiedName.size() != 3
                || qualifiedName.stream().anyMatch(name -> name == null || name.trim().isEmpty())) {
            throw new TableLineageExtractionException(
                    "Source table identity must contain catalog, database, and object names.");
        }
        this.qualifiedName = Collections.unmodifiableList(new ArrayList<>(qualifiedName));
    }

    @JsonProperty(FIELD_NAME_QUALIFIED_NAME)
    public List<String> getQualifiedName() {
        return qualifiedName;
    }

    public String asSummaryString() {
        return String.join(".", qualifiedName);
    }

    public String asSerializableString() {
        return ObjectIdentifier.of(qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2))
                .asSerializableString();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof PlannerLineageDataset)) {
            return false;
        }
        final PlannerLineageDataset that = (PlannerLineageDataset) object;
        return qualifiedName.equals(that.qualifiedName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifiedName);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }
}

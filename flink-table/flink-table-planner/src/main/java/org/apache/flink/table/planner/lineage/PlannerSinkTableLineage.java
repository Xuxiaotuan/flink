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

import java.util.List;
import java.util.Objects;

/** Verified logical table dependencies, independent of column expression support. */
@Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public final class PlannerSinkTableLineage {
    private final String sinkKey;
    private final List<PlannerLineageDataset> expectedSources;
    private final List<PlannerPrunedSource> prunedSources;

    @JsonCreator
    public PlannerSinkTableLineage(
            @JsonProperty("sinkKey") String sinkKey,
            @JsonProperty("expectedSources") List<PlannerLineageDataset> expectedSources,
            @JsonProperty("prunedSources") List<PlannerPrunedSource> prunedSources) {
        this.sinkKey = Objects.requireNonNull(sinkKey);
        this.expectedSources = List.copyOf(expectedSources);
        this.prunedSources = List.copyOf(prunedSources);
    }

    @JsonProperty("sinkKey")
    public String getSinkKey() {
        return sinkKey;
    }

    /** Version of this optional observation extension, independent of the execution plan. */
    @JsonProperty("formatVersion")
    public int getFormatVersion() {
        return 1;
    }

    @JsonProperty("expectedSources")
    public List<PlannerLineageDataset> getExpectedSources() {
        return expectedSources;
    }

    @JsonProperty("prunedSources")
    public List<PlannerPrunedSource> getPrunedSources() {
        return prunedSources;
    }
}

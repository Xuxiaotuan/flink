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

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.Internal;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** Complete column lineage carried by a sink transformation. */
@Internal
public final class TransformationColumnLineage {

    private final List<String> expectedOutputFields;
    private final List<ColumnLineageRelation> relations;
    private final List<SourceLineageVertex> prunedSources;

    public TransformationColumnLineage(
            List<String> expectedOutputFields, List<ColumnLineageRelation> relations) {
        this(expectedOutputFields, relations, Collections.emptyList());
    }

    public TransformationColumnLineage(
            List<String> expectedOutputFields,
            List<ColumnLineageRelation> relations,
            List<SourceLineageVertex> prunedSources) {
        this.expectedOutputFields = List.copyOf(requireNonNull(expectedOutputFields));
        this.relations = List.copyOf(requireNonNull(relations));
        this.prunedSources = List.copyOf(requireNonNull(prunedSources));
    }

    public List<String> getExpectedOutputFields() {
        return expectedOutputFields;
    }

    public List<ColumnLineageRelation> getRelations() {
        return relations;
    }

    /** Logical dependencies removed by optimization; these do not add source transformations. */
    public List<SourceLineageVertex> getPrunedSources() {
        return prunedSources;
    }
}

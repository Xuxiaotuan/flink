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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Default implementation for {@link ColumnLineageRelation}. */
@Internal
public final class DefaultColumnLineageRelation implements ColumnLineageRelation {
    @JsonProperty private final LineageDataset outputDataset;
    @JsonProperty private final String outputField;
    @JsonProperty private final List<ColumnLineageInput> inputs;
    @JsonProperty private final ColumnLineageOrigin origin;
    @JsonProperty private final @Nullable String transformation;

    public DefaultColumnLineageRelation(
            LineageDataset outputDataset,
            String outputField,
            List<ColumnLineageInput> inputs,
            ColumnLineageOrigin origin,
            @Nullable String transformation) {
        this.outputDataset = Objects.requireNonNull(outputDataset);
        this.outputField = outputField;
        this.inputs = List.copyOf(inputs);
        this.origin = origin;
        this.transformation = transformation;
    }

    @Override
    public LineageDataset outputDataset() {
        return outputDataset;
    }

    @Override
    public String outputField() {
        return outputField;
    }

    @Override
    public List<ColumnLineageInput> inputs() {
        return inputs;
    }

    @Override
    public ColumnLineageOrigin origin() {
        return origin;
    }

    @Override
    public Optional<String> transformation() {
        return Optional.ofNullable(transformation);
    }
}

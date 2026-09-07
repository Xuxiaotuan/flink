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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Isolates invalid optional lineage metadata from executable plan deserialization. */
@Internal
public final class OptionalColumnLineageDeserializer
        extends JsonDeserializer<PlannerSinkColumnLineage> {
    @Override
    public PlannerSinkColumnLineage deserialize(JsonParser parser, DeserializationContext context)
            throws IOException {
        // Malformed plan JSON remains an execution error; only the optional subtree is isolated.
        JsonNode tree = parser.getCodec().readTree(parser);
        JsonNode version = tree.get("formatVersion");
        if (version != null
                && (!version.isIntegralNumber()
                        || !version.canConvertToInt()
                        || version.intValue() != 1)) {
            LoggerFactory.getLogger(OptionalColumnLineageDeserializer.class)
                    .warn(
                            "Unsupported optional column lineage version; restoring execution without it.");
            return null;
        }
        try {
            return context.readTreeAsValue(tree, PlannerSinkColumnLineage.class);
        } catch (IOException error) {
            LoggerFactory.getLogger(OptionalColumnLineageDeserializer.class)
                    .warn(
                            "Invalid optional column lineage; restoring the execution plan without lineage.");
            LoggerFactory.getLogger(OptionalColumnLineageDeserializer.class)
                    .debug("Invalid optional column lineage details.", error);
            return null;
        } catch (RuntimeException error) {
            LoggerFactory.getLogger(OptionalColumnLineageDeserializer.class)
                    .warn("Unexpected column lineage restore failure; execution continues.", error);
            return null;
        }
    }
}

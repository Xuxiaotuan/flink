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

/** Invalid optional table metadata cannot prevent executable plan restoration. */
@Internal
public final class OptionalTableLineageDeserializer
        extends JsonDeserializer<PlannerSinkTableLineage> {
    @Override
    public PlannerSinkTableLineage deserialize(JsonParser parser, DeserializationContext context)
            throws IOException {
        JsonNode tree = parser.getCodec().readTree(parser);
        JsonNode version = tree.get("formatVersion");
        if (version != null
                && (!version.isIntegralNumber()
                        || !version.canConvertToInt()
                        || version.intValue() != 1)) {
            LoggerFactory.getLogger(OptionalTableLineageDeserializer.class)
                    .warn(
                            "Unsupported optional table lineage version; restoring execution without it.");
            return null;
        }
        try {
            return context.readTreeAsValue(tree, PlannerSinkTableLineage.class);
        } catch (IOException error) {
            LoggerFactory.getLogger(OptionalTableLineageDeserializer.class)
                    .warn("Invalid optional table lineage; restoring execution without it.");
            LoggerFactory.getLogger(OptionalTableLineageDeserializer.class)
                    .debug("Invalid optional table lineage details.", error);
            return null;
        } catch (RuntimeException error) {
            LoggerFactory.getLogger(OptionalTableLineageDeserializer.class)
                    .warn("Unexpected table lineage restore failure; execution continues.", error);
            return null;
        }
    }
}

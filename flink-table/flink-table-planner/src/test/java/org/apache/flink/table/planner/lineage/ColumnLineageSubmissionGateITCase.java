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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies that invalid column lineage prevents Table jobs from reaching submission. */
class ColumnLineageSubmissionGateITCase {

    private static final String INSERT_SQL =
            "INSERT INTO LineageSink SELECT `value` + 1 FROM LineageSource";
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();
    private static final List<JobStatusChangedEvent> STATUS_CHANGED_EVENTS =
            new CopyOnWriteArrayList<>();

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(createConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @BeforeEach
    void clearEvents() {
        STATUS_CHANGED_EVENTS.clear();
    }

    @Test
    void testMissingCompiledPlanRelationPreventsJobSubmission(
            @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final CompiledPlan compiledPlan = environment.compilePlanSql(INSERT_SQL);
        final JsonNode planJson = OBJECT_MAPPER.readTree(compiledPlan.asJsonString());

        assertThat(removeColumnLineageRelations(planJson)).isEqualTo(1);
        final int jobCountBefore = miniCluster.listJobs().get().size();

        assertThatThrownBy(
                        () ->
                                environment.loadPlan(
                                        PlanReference.fromJsonString(
                                                OBJECT_MAPPER.writeValueAsString(planJson))))
                .hasStackTraceContaining("LineageSink")
                .hasStackTraceContaining("result")
                .hasStackTraceContaining("with no lineage relation");

        assertThat(miniCluster.listJobs().get()).hasSize(jobCountBefore);
        assertThat(STATUS_CHANGED_EVENTS).noneMatch(JobCreatedEvent.class::isInstance);
    }

    @Test
    void testMissingEntireColumnLineagePreventsJobSubmissionDuringExecute(
            @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final CompiledPlan compiledPlan = environment.compilePlanSql(INSERT_SQL);
        final JsonNode planJson = OBJECT_MAPPER.readTree(compiledPlan.asJsonString());

        assertThat(removeColumnLineage(planJson)).isEqualTo(1);
        final CompiledPlan legacyPlan =
                environment.loadPlan(
                        PlanReference.fromJsonString(OBJECT_MAPPER.writeValueAsString(planJson)));
        final int jobCountBefore = miniCluster.listJobs().get().size();

        assertThatThrownBy(legacyPlan::execute)
                .isInstanceOf(TableLineageExtractionException.class)
                .hasMessageContaining("LineageSink")
                .hasMessageContaining("field '<unknown>'")
                .hasMessageContaining("compiled plan does not contain complete column lineage");

        assertThat(miniCluster.listJobs().get()).hasSize(jobCountBefore);
        assertThat(STATUS_CHANGED_EVENTS).noneMatch(JobCreatedEvent.class::isInstance);
    }

    private static TableEnvironmentImpl createEnvironment() {
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(createConfiguration())
                        .build();
        final TableEnvironmentImpl environment =
                (TableEnvironmentImpl) TableEnvironmentImpl.create(settings);
        environment.createTemporaryTable(
                "LineageSource",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("value", DataTypes.BIGINT()).build())
                        .option("bounded", "true")
                        .build());
        environment.createTemporaryTable(
                "LineageSink",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("result", DataTypes.BIGINT()).build())
                        .build());
        return environment;
    }

    private static int removeColumnLineageRelations(JsonNode node) {
        int removed = 0;
        if (node.isObject() && node.has("columnLineage")) {
            final ObjectNode columnLineage = (ObjectNode) node.get("columnLineage");
            columnLineage.putArray("relations");
            removed++;
        }
        for (JsonNode child : node) {
            removed += removeColumnLineageRelations(child);
        }
        return removed;
    }

    private static int removeColumnLineage(JsonNode node) {
        int removed = 0;
        if (node.isObject()) {
            removed += ((ObjectNode) node).remove("columnLineage") == null ? 0 : 1;
        }
        for (JsonNode child : node) {
            removed += removeColumnLineage(child);
        }
        return removed;
    }

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(
                JOB_STATUS_CHANGED_LISTENERS,
                Collections.singletonList(TestingJobStatusChangedListenerFactory.class.getName()));
        return configuration;
    }

    /** Listener factory used to prove that no job-created event escapes the lineage gate. */
    public static class TestingJobStatusChangedListenerFactory
            implements JobStatusChangedListenerFactory {

        @Override
        public JobStatusChangedListener createListener(Context context) {
            return STATUS_CHANGED_EVENTS::add;
        }
    }
}

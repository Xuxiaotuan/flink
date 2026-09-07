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
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies that lineage failures cannot veto otherwise valid Table jobs. */
class ColumnLineageSubmissionGateITCase {

    @RegisterExtension
    private final LoggerAuditingExtension lineageLogs =
            new LoggerAuditingExtension("org.apache.flink.table.planner", Level.INFO);

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
    void invalidLineageDoesNotPreventPlanRestore() throws Exception {
        assertPlanExecutesWithMissingLineage(true);
        assertExpectedObservationLogs();
    }

    @Test
    void unknownLineageVersionDoesNotPreventExecution() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final JsonNode plan =
                OBJECT_MAPPER.readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        for (String field : List.of("columnLineage", "tableLineage")) {
            final List<JsonNode> metadata = plan.findValues(field);
            assertThat(metadata).hasSize(1);
            ((ObjectNode) metadata.get(0)).put("formatVersion", 999);
        }
        environment
                .loadPlan(PlanReference.fromJsonString(plan.toString()))
                .execute()
                .await(30, TimeUnit.SECONDS);
        assertThat(TestValuesTableFactory.getResults("LineageSink")).containsExactly(Row.of(8L));
        assertThat(STATUS_CHANGED_EVENTS).anyMatch(JobCreatedEvent.class::isInstance);
        STATUS_CHANGED_EVENTS.stream()
                .filter(JobCreatedEvent.class::isInstance)
                .map(JobCreatedEvent.class::cast)
                .forEach(
                        event -> {
                            final org.apache.flink.streaming.api.lineage.LineageGraphObservation
                                    observation =
                                            (org.apache.flink.streaming.api.lineage
                                                            .LineageGraphObservation)
                                                    event.lineageGraph();
                            assertThat(observation.getTableStatus()).isNotEqualTo("COMPLETE");
                            assertThat(observation.getColumnStatus()).isEqualTo("UNAVAILABLE");
                            assertThat(observation.columnRelations()).isEmpty();
                        });
        assertExpectedObservationLogs();
    }

    @Test
    void malformedExecutablePlanStillFails() {
        assertThatThrownBy(
                        () ->
                                createEnvironment()
                                        .loadPlan(PlanReference.fromJsonString("{\"nodes\":[")))
                .isInstanceOf(org.apache.flink.table.api.TableException.class);
        assertThat(STATUS_CHANGED_EVENTS).noneMatch(JobCreatedEvent.class::isInstance);
    }

    @Test
    void supportedLineageIsStillPublished() throws Exception {
        createEnvironment().executeSql(INSERT_SQL).await(30, TimeUnit.SECONDS);
        assertThat(TestValuesTableFactory.getResults("LineageSink")).containsExactly(Row.of(8L));
        assertThat(STATUS_CHANGED_EVENTS).anyMatch(JobCreatedEvent.class::isInstance);
        STATUS_CHANGED_EVENTS.stream()
                .filter(JobCreatedEvent.class::isInstance)
                .map(JobCreatedEvent.class::cast)
                .forEach(event -> assertThat(event.lineageGraph()).isNotNull());
    }

    @Test
    void invalidSqlStillFails() {
        assertThatThrownBy(
                        () ->
                                createEnvironment()
                                        .executeSql(
                                                "INSERT INTO LineageSink SELECT missing_column FROM LineageSource"))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class);
        assertThat(STATUS_CHANGED_EVENTS).noneMatch(JobCreatedEvent.class::isInstance);
    }

    @Test
    void legacyPlanWithoutLineageStillExecutes() throws Exception {
        assertPlanExecutesWithMissingLineage(false);
    }

    private void assertPlanExecutesWithMissingLineage(boolean invalid) throws Exception {
        final TableEnvironmentImpl environment = createEnvironment();
        final JsonNode planJson =
                OBJECT_MAPPER.readTree(environment.compilePlanSql(INSERT_SQL).asJsonString());
        assertThat(invalid ? removeColumnLineageRelations(planJson) : removeColumnLineage(planJson))
                .isEqualTo(1);
        environment
                .loadPlan(PlanReference.fromJsonString(OBJECT_MAPPER.writeValueAsString(planJson)))
                .execute()
                .await(30, TimeUnit.SECONDS);
        assertThat(TestValuesTableFactory.getResults("LineageSink")).containsExactly(Row.of(8L));
        assertThat(STATUS_CHANGED_EVENTS).anyMatch(JobCreatedEvent.class::isInstance);
        STATUS_CHANGED_EVENTS.stream()
                .filter(JobCreatedEvent.class::isInstance)
                .map(JobCreatedEvent.class::cast)
                .forEach(
                        event -> {
                            org.apache.flink.streaming.api.lineage.LineageGraphObservation
                                    observation =
                                            (org.apache.flink.streaming.api.lineage
                                                            .LineageGraphObservation)
                                                    event.lineageGraph();
                            assertThat(observation.getColumnStatus()).isEqualTo("UNAVAILABLE");
                            assertThat(observation.getIssues()).isNotEmpty();
                            assertThat(observation.sources()).hasSize(1);
                            assertThat(observation.sinks()).hasSize(1);
                            assertThat(observation.columnRelations()).isEmpty();
                        });
    }

    @Test
    void setOperationLineageDoesNotPreventSqlExecution() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment(true);
        environment
                .executeSql(
                        "INSERT INTO LineageSink SELECT `value` FROM LineageSource INTERSECT SELECT `value` FROM LineageSource")
                .await(30, TimeUnit.SECONDS);
        assertThat(TestValuesTableFactory.getResults("LineageSink")).containsExactly(Row.of(7L));
        assertThat(STATUS_CHANGED_EVENTS).anyMatch(JobCreatedEvent.class::isInstance);
        STATUS_CHANGED_EVENTS.stream()
                .filter(JobCreatedEvent.class::isInstance)
                .map(JobCreatedEvent.class::cast)
                .map(
                        event ->
                                (org.apache.flink.streaming.api.lineage.LineageGraphObservation)
                                        event.lineageGraph())
                .forEach(
                        observation -> {
                            assertThat(observation.getColumnStatus()).isEqualTo("COMPLETE");
                            assertThat(observation.columnRelations()).isNotEmpty();
                        });
    }

    @Test
    void unsupportedScalarSubqueryLineageDoesNotPreventSqlExecution() throws Exception {
        final TableEnvironmentImpl environment = createEnvironment(true);
        environment.createTemporaryTable(
                "OtherSource",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("value", DataTypes.BIGINT()).build())
                        .option(
                                "data-id",
                                TestValuesTableFactory.registerData(
                                        Collections.singletonList(Row.of(7L))))
                        .option("bounded", "true")
                        .build());
        environment
                .executeSql(
                        "INSERT INTO LineageSink SELECT l.`value` FROM LineageSource l "
                                + "WHERE l.`value` >= (SELECT MIN(r.`value`) FROM OtherSource r)")
                .await(30, TimeUnit.SECONDS);
        assertThat(TestValuesTableFactory.getResults("LineageSink")).containsExactly(Row.of(7L));
        assertThat(STATUS_CHANGED_EVENTS).anyMatch(JobCreatedEvent.class::isInstance);
        STATUS_CHANGED_EVENTS.stream()
                .filter(JobCreatedEvent.class::isInstance)
                .map(JobCreatedEvent.class::cast)
                .map(
                        event ->
                                (org.apache.flink.streaming.api.lineage.LineageGraphObservation)
                                        event.lineageGraph())
                .forEach(
                        observation -> {
                            assertThat(observation.getColumnStatus()).isEqualTo("UNAVAILABLE");
                            assertThat(observation.columnRelations()).isEmpty();
                        });
        assertExpectedObservationLogs();
    }

    private void assertExpectedObservationLogs() {
        assertThat(lineageLogs.getEvents())
                .isNotEmpty()
                .allSatisfy(
                        event -> {
                            assertThat(event.getThrown()).isNull();
                            assertThat(event.getMessage().getFormattedMessage())
                                    .doesNotContainIgnoringCase("exception");
                        });
    }

    private static TableEnvironmentImpl createEnvironment() {
        return createEnvironment(false);
    }

    private static TableEnvironmentImpl createEnvironment(boolean batch) {
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(createConfiguration())
                        .build();
        settings.getConfiguration()
                .set(
                        org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE,
                        batch
                                ? org.apache.flink.api.common.RuntimeExecutionMode.BATCH
                                : org.apache.flink.api.common.RuntimeExecutionMode.STREAMING);
        final TableEnvironmentImpl environment =
                (TableEnvironmentImpl) TableEnvironmentImpl.create(settings);
        environment.createTemporaryTable(
                "LineageSource",
                TableDescriptor.forConnector("values")
                        .schema(Schema.newBuilder().column("value", DataTypes.BIGINT()).build())
                        .option(
                                "data-id",
                                TestValuesTableFactory.registerData(
                                        Collections.singletonList(Row.of(7L))))
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

    /** Listener factory used to check lineage availability on actual job-created events. */
    public static class TestingJobStatusChangedListenerFactory
            implements JobStatusChangedListenerFactory {

        @Override
        public JobStatusChangedListener createListener(Context context) {
            return STATUS_CHANGED_EVENTS::add;
        }
    }
}

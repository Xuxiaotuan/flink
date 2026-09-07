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

package org.apache.flink.client.deployment.application.executors;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.application.EmbeddedJobClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.core.execution.SubmissionIdentity;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class EmbeddedSubmissionIdentityTest {
    @Test
    void submittedSnapshotsAndDelayedCreatedEventsRetainTheirIdentity() throws Exception {
        RecordingFactory.EVENTS.clear();
        Configuration configuration = new Configuration();
        configuration.set(
                DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS,
                List.of(RecordingFactory.class.getName()));
        StreamGraph graph = graph(configuration);
        List<ExecutionPlan> submitted = new ArrayList<>();
        List<CompletableFuture<Acknowledge>> acknowledgements = new ArrayList<>();
        TestingDispatcherGateway gateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                plan -> {
                                    submitted.add(plan);
                                    CompletableFuture<Acknowledge> acknowledgement =
                                            new CompletableFuture<>();
                                    acknowledgements.add(acknowledgement);
                                    return acknowledgement;
                                })
                        .setRequestJobStatusFunction(
                                id -> CompletableFuture.completedFuture(JobStatus.RUNNING))
                        .build();
        EmbeddedExecutor executor =
                new EmbeddedExecutor(
                        new ArrayList<>(),
                        gateway,
                        configuration,
                        (id, loader) ->
                                new EmbeddedJobClient(
                                        id,
                                        gateway,
                                        new ManuallyTriggeredScheduledExecutor(),
                                        Duration.ofSeconds(30),
                                        loader));
        CompletableFuture<JobClient> first =
                executor.execute(graph, configuration, getClass().getClassLoader());
        String firstId = graph.getJobConfiguration().getString(SubmissionIdentity.CONFIG_KEY, null);
        CompletableFuture<JobClient> second =
                executor.execute(graph, configuration, getClass().getClassLoader());
        String secondId =
                graph.getJobConfiguration().getString(SubmissionIdentity.CONFIG_KEY, null);
        assertThat(firstId).isNotNull();
        assertThat(secondId).isNotNull().isNotEqualTo(firstId);
        assertThat(submitted).hasSize(2);
        assertThat(submitted.get(0)).isNotSameAs(graph);
        assertThat(
                        submitted
                                .get(0)
                                .getJobConfiguration()
                                .getString(SubmissionIdentity.CONFIG_KEY, null))
                .isEqualTo(firstId);
        assertThat(
                        submitted
                                .get(1)
                                .getJobConfiguration()
                                .getString(SubmissionIdentity.CONFIG_KEY, null))
                .isEqualTo(secondId);
        acknowledgements.get(0).complete(Acknowledge.get());
        first.get(30, TimeUnit.SECONDS);
        acknowledgements.get(1).complete(Acknowledge.get());
        second.get(30, TimeUnit.SECONDS);
        assertThat(RecordingFactory.EVENTS)
                .extracting(event -> ((SubmissionIdentity) event).submissionId())
                .containsExactly(firstId, secondId);
    }

    @Test
    void reconnectToRecoveredOrTerminalJobDoesNotRotateIdentity() throws Exception {
        for (boolean suspended : List.of(false, true)) {
            Configuration configuration = new Configuration();
            StreamGraph graph = graph(configuration);
            configuration.set(
                    PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, graph.getJobID().toHexString());
            graph.getJobConfiguration()
                    .setString(SubmissionIdentity.CONFIG_KEY, "persisted-submission");
            List<JobID> recovered = new ArrayList<>();
            TestingDispatcherGateway gateway =
                    TestingDispatcherGateway.newBuilder()
                            .setSubmitFunction(
                                    plan -> {
                                        throw new AssertionError("reconnect must not submit");
                                    })
                            .setRecoverJobFunction(
                                    id -> {
                                        recovered.add(id);
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    })
                            .build();
            EmbeddedExecutor executor =
                    new EmbeddedExecutor(
                            new ArrayList<>(),
                            suspended ? List.of(graph.getJobID()) : List.of(),
                            suspended ? List.of() : List.of(graph.getJobID()),
                            gateway,
                            configuration,
                            (id, loader) ->
                                    new EmbeddedJobClient(
                                            id,
                                            gateway,
                                            new ManuallyTriggeredScheduledExecutor(),
                                            Duration.ofSeconds(30),
                                            loader));
            executor.execute(graph, configuration, getClass().getClassLoader())
                    .get(30, TimeUnit.SECONDS);
            assertThat(graph.getJobConfiguration().getString(SubmissionIdentity.CONFIG_KEY, null))
                    .isEqualTo("persisted-submission");
            assertThat(recovered).hasSize(suspended ? 1 : 0);
        }
    }

    private static StreamGraph graph(Configuration configuration) {
        StreamGraph graph =
                new StreamGraph(
                        configuration,
                        new ExecutionConfig(),
                        new CheckpointConfig(),
                        SavepointRestoreSettings.none());
        graph.setJobId(new JobID(1, 2));
        return graph;
    }

    public static class RecordingFactory implements JobStatusChangedListenerFactory {
        static final List<JobStatusChangedEvent> EVENTS = new CopyOnWriteArrayList<>();

        @Override
        public JobStatusChangedListener createListener(Context context) {
            return EVENTS::add;
        }
    }
}

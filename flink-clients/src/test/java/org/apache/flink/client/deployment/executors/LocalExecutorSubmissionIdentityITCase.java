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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobExecutionStatusEvent;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.core.execution.SubmissionIdentity;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests submission identity across real client and JobManager listener boundaries. */
class LocalExecutorSubmissionIdentityITCase {

    private final List<MiniCluster> clusters = new ArrayList<>();

    @BeforeEach
    void clearEvents() {
        RecordingListenerFactory.EVENTS.clear();
    }

    @AfterEach
    void closeClusters() throws Exception {
        for (MiniCluster cluster : clusters) {
            cluster.closeAsync().get(60, TimeUnit.SECONDS);
        }
    }

    @Test
    void testReusedStreamGraphHasDistinctSubmissionsAndMatchingClientAndJobManagerEvents()
            throws Exception {
        final Configuration configuration = configuration();
        final StreamGraph graph = streamGraph();
        final JobID fixedJobId = graph.getJobID();
        final LocalExecutor executor =
                LocalExecutor.createWithFactory(configuration, this::createCluster);

        final String firstId = executeAndCheckEvents(executor, graph, configuration);
        final StreamGraph persistedGraph = InstantiationUtil.clone(graph);
        assertThat(persistedGraph.getJobID()).isEqualTo(fixedJobId);
        assertThat(
                        persistedGraph
                                .getJobConfiguration()
                                .getString(SubmissionIdentity.CONFIG_KEY, null))
                .isEqualTo(firstId);

        final String secondId = executeAndCheckEvents(executor, graph, configuration);
        assertThat(graph.getJobID()).isEqualTo(fixedJobId);
        assertThat(secondId).isNotEqualTo(firstId);
        assertThat(
                        persistedGraph
                                .getJobConfiguration()
                                .getString(SubmissionIdentity.CONFIG_KEY, null))
                .isEqualTo(firstId);
    }

    @Test
    void testReusedJobGraphHasDistinctIdentitiesInJobManagerEvents() throws Exception {
        final JobGraph graph = streamGraph().getJobGraph();
        final JobID fixedJobId = graph.getJobID();
        String previousId = null;
        for (int submission = 0; submission < 2; submission++) {
            // Fresh clusters allow independent successful submissions with the same JobID.
            final MiniCluster cluster =
                    createCluster(
                            new MiniClusterConfiguration.Builder()
                                    .setConfiguration(configuration())
                                    .setNumTaskManagers(1)
                                    .setNumSlotsPerTaskManager(1)
                                    .build());
            cluster.start();
            cluster.submitJob(graph).get(60, TimeUnit.SECONDS);
            assertThat(cluster.requestJobResult(fixedJobId).get(60, TimeUnit.SECONDS).isSuccess())
                    .isTrue();
            final JobStatusChangedEvent event = nextEvent();
            assertThat(event).isInstanceOf(JobExecutionStatusEvent.class);
            assertThat(event.jobId()).isEqualTo(fixedJobId);
            final String identity =
                    graph.getJobConfiguration().getString(SubmissionIdentity.CONFIG_KEY, null);
            assertThat(identity).isNotBlank().isNotEqualTo(previousId);
            assertThat(((SubmissionIdentity) event).submissionId()).isEqualTo(identity);
            previousId = identity;
            cluster.closeAsync().get(60, TimeUnit.SECONDS);
        }
    }

    private String executeAndCheckEvents(
            LocalExecutor executor, StreamGraph graph, Configuration configuration)
            throws Exception {
        final JobClient client =
                executor.execute(graph, configuration, getClass().getClassLoader())
                        .get(60, TimeUnit.SECONDS);
        assertThat(client.getJobExecutionResult().get(60, TimeUnit.SECONDS).getJobID())
                .isEqualTo(graph.getJobID());
        final String identity =
                graph.getJobConfiguration().getString(SubmissionIdentity.CONFIG_KEY, null);
        assertThat(identity).isNotBlank();
        final List<JobStatusChangedEvent> events = List.of(nextEvent(), nextEvent());
        assertThat(events).filteredOn(event -> event instanceof JobCreatedEvent).hasSize(1);
        assertThat(events).filteredOn(event -> event instanceof JobExecutionStatusEvent).hasSize(1);
        assertThat(events)
                .allSatisfy(
                        event -> {
                            assertThat(event.jobId()).isEqualTo(graph.getJobID());
                            assertThat(event).isInstanceOf(SubmissionIdentity.class);
                            assertThat(((SubmissionIdentity) event).submissionId())
                                    .isEqualTo(identity);
                        });
        return identity;
    }

    private static JobStatusChangedEvent nextEvent() throws InterruptedException {
        final JobStatusChangedEvent event =
                RecordingListenerFactory.EVENTS.poll(60, TimeUnit.SECONDS);
        assertThat(event).as("real client or JobManager listener event").isNotNull();
        return event;
    }

    private MiniCluster createCluster(MiniClusterConfiguration configuration) {
        final MiniCluster cluster = new MiniCluster(configuration);
        clusters.add(cluster);
        return cluster;
    }

    private static Configuration configuration() {
        return new Configuration()
                .set(DeploymentOptions.ATTACHED, true)
                .set(
                        DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS,
                        Collections.singletonList(RecordingListenerFactory.class.getName()));
    }

    private static StreamGraph streamGraph() {
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.fromData(1, 2, 3).sinkTo(new DiscardingSink<>());
        final StreamGraph graph = environment.getStreamGraph();
        graph.setJobId(new JobID());
        return graph;
    }

    /** Public factory loaded through the configured production listener mechanism. */
    public static class RecordingListenerFactory implements JobStatusChangedListenerFactory {
        private static final BlockingQueue<JobStatusChangedEvent> EVENTS =
                new LinkedBlockingQueue<>();

        @Override
        public JobStatusChangedListener createListener(Context context) {
            return event -> {
                if (event instanceof JobCreatedEvent
                        || (event instanceof JobExecutionStatusEvent
                                && ((JobExecutionStatusEvent) event).newStatus()
                                        == JobStatus.FINISHED)) {
                    EVENTS.add(event);
                }
            };
        }
    }
}

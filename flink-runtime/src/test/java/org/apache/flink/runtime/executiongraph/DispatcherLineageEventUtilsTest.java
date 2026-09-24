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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.JobStatusChangedEvent;
import org.apache.flink.streaming.api.lineage.DefaultLineageGraph;
import org.apache.flink.streaming.api.lineage.LineageGraph;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DispatcherLineageEventUtilsTest {

    @Test
    void createsDispatcherEventThroughTheUserClassLoader() {
        final List<JobStatusChangedEvent> events = new ArrayList<>();
        final LineageGraph graph = DefaultLineageGraph.builder().build();

        DispatcherLineageEventUtils.notifyJobCreated(
                List.of(events::add),
                new JobID(),
                "job",
                graph,
                RuntimeExecutionMode.STREAMING,
                "submission",
                getClass().getClassLoader(),
                TestingJobCreatedEvent.class.getName());

        assertThat(events).singleElement().isInstanceOf(TestingJobCreatedEvent.class);
        assertThat(((TestingJobCreatedEvent) events.get(0)).lineageGraph()).isSameAs(graph);
    }

    public static final class TestingJobCreatedEvent implements JobStatusChangedEvent {
        private final JobID jobId;
        private final String jobName;
        private final LineageGraph lineageGraph;

        public TestingJobCreatedEvent(
                JobID jobId,
                String jobName,
                LineageGraph lineageGraph,
                RuntimeExecutionMode executionMode,
                String submissionId) {
            this.jobId = jobId;
            this.jobName = jobName;
            this.lineageGraph = lineageGraph;
        }

        @Override
        public JobID jobId() {
            return jobId;
        }

        @Override
        public String jobName() {
            return jobName;
        }

        public LineageGraph lineageGraph() {
            return lineageGraph;
        }
    }
}

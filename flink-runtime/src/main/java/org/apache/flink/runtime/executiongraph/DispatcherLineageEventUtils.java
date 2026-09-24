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
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.streaming.api.lineage.LineageGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.util.List;

/** Creates the streaming job-created event without a runtime-to-streaming module dependency. */
final class DispatcherLineageEventUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DispatcherLineageEventUtils.class);
    private static final String DEFAULT_EVENT_CLASS =
            "org.apache.flink.streaming.runtime.execution.DefaultJobCreatedEvent";

    private DispatcherLineageEventUtils() {}

    static void notifyJobCreated(
            List<JobStatusChangedListener> listeners,
            JobID jobId,
            String jobName,
            LineageGraph lineageGraph,
            RuntimeExecutionMode executionMode,
            @Nullable String submissionId,
            ClassLoader classLoader) {
        notifyJobCreated(
                listeners,
                jobId,
                jobName,
                lineageGraph,
                executionMode,
                submissionId,
                classLoader,
                DEFAULT_EVENT_CLASS);
    }

    static void notifyJobCreated(
            List<JobStatusChangedListener> listeners,
            JobID jobId,
            String jobName,
            LineageGraph lineageGraph,
            RuntimeExecutionMode executionMode,
            @Nullable String submissionId,
            ClassLoader classLoader,
            String eventClassName) {
        try {
            Class<?> eventClass;
            try {
                eventClass = Class.forName(eventClassName, true, classLoader);
            } catch (ClassNotFoundException userClassLoaderFailure) {
                // The user-code classloader may intentionally hide Flink runtime classes. Fall
                // back to the Dispatcher loader so the built-in event contract remains usable.
                LOG.debug(
                        "Could not load Dispatcher lineage event with user classloader; falling back to Flink classloader.",
                        userClassLoaderFailure);
                eventClass =
                        Class.forName(
                                eventClassName,
                                true,
                                DispatcherLineageEventUtils.class.getClassLoader());
            }
            final Constructor<?> constructor =
                    eventClass.getConstructor(
                            JobID.class,
                            String.class,
                            LineageGraph.class,
                            RuntimeExecutionMode.class,
                            String.class);
            final Object event =
                    constructor.newInstance(
                            jobId, jobName, lineageGraph, executionMode, submissionId);
            if (!(event instanceof JobStatusChangedEvent)) {
                throw new IllegalArgumentException(
                        "Job-created event does not implement JobStatusChangedEvent: "
                                + eventClassName);
            }
            final JobStatusChangedEvent statusEvent = (JobStatusChangedEvent) event;
            for (JobStatusChangedListener listener : listeners) {
                try {
                    listener.onEvent(statusEvent);
                } catch (Throwable listenerFailure) {
                    LOG.warn(
                            "Error while notifying Dispatcher job-created listener",
                            listenerFailure);
                }
            }
        } catch (Throwable eventFailure) {
            LOG.warn(
                    "Could not create Dispatcher job-created lineage event; execution continues.",
                    eventFailure);
        }
    }
}

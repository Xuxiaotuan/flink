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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerConfiguration;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

/** Configuration class for the {@link ResourceManagerRuntimeServices} class. */
public class ResourceManagerRuntimeServicesConfiguration {

    private final Duration jobTimeout;

    private final SlotManagerConfiguration slotManagerConfiguration;

    public ResourceManagerRuntimeServicesConfiguration(
            Duration jobTimeout, SlotManagerConfiguration slotManagerConfiguration) {
        this.jobTimeout = Preconditions.checkNotNull(jobTimeout);
        this.slotManagerConfiguration = Preconditions.checkNotNull(slotManagerConfiguration);
    }

    public Duration getJobTimeout() {
        return jobTimeout;
    }

    public SlotManagerConfiguration getSlotManagerConfiguration() {
        return slotManagerConfiguration;
    }

    // ---------------------------- Static methods ----------------------------------

    public static ResourceManagerRuntimeServicesConfiguration fromConfiguration(
            Configuration configuration, WorkerResourceSpecFactory defaultWorkerResourceSpecFactory)
            throws ConfigurationException {

        final Duration jobTimeout;
        try {
            jobTimeout = configuration.get(ResourceManagerOptions.JOB_TIMEOUT);
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(
                    "Could not parse the resource manager's job timeout "
                            + "value "
                            + ResourceManagerOptions.JOB_TIMEOUT
                            + '.',
                    e);
        }

        final WorkerResourceSpec defaultWorkerResourceSpec =
                defaultWorkerResourceSpecFactory.createDefaultWorkerResourceSpec(configuration);
        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(
                        configuration, defaultWorkerResourceSpec);

        return new ResourceManagerRuntimeServicesConfiguration(
                jobTimeout, slotManagerConfiguration);
    }
}

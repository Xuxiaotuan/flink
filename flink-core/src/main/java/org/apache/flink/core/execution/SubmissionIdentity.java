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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * Internal identity shared by client and runtime observations of one submission.
 *
 * <p>A client submission call assigns a fresh ID before taking the submitted plan snapshot. Transport
 * retries and recovery use that snapshot's configuration without rotating the ID. The ID does not
 * replace JobID or change the dispatcher's duplicate-job rules.
 */
@Internal
public interface SubmissionIdentity extends JobStatusChangedEvent {
    String CONFIG_KEY = "internal.lineage.submission-id";

    /** Returns null for legacy events that do not identify a submission. */
    @Nullable
    String submissionId();
}

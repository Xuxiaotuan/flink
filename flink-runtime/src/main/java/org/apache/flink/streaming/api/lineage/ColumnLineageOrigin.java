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

package org.apache.flink.streaming.api.lineage;

import org.apache.flink.annotation.PublicEvolving;

/** Describes the direct origin of an output field's value. */
@PublicEvolving
public enum ColumnLineageOrigin {
    /**
     * The output value originates from one or more {@link ColumnLineageDependencyType#DIRECT}
     * inputs.
     */
    INPUT_FIELDS,

    /** The output value is a constant. It may have indirect control dependencies. */
    CONSTANT,

    /** The output value is produced by the system. It may have indirect control dependencies. */
    SYSTEM
}

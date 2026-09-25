---
title: Data Lineage
weight: 12
type: docs
aliases:
  - /internals/data_lineage.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Native Lineage Support
As organisations look to govern their data ecosystems; understanding data lineage, where data is coming from and going to, becomes critical. As Apache Flink is widely used for data ingestion and ETL in Streaming Data Lakes, we need 
an end to end lineage solution for scenarios including but not limited to:
  - `Data Quality Assurance`: Identifying and rectifying data inconsistencies by tracing data errors back to their origin within the data pipeline.
  - `Data Governance`： Establishing clear data ownership and accountability by documenting data origins and transformations.
  - `Regulatory Compliance`: Ensuring adherence to data privacy and compliance regulations by tracking data flow and transformations throughout its lifecycle.
  - `Data Optimization`: Identifying redundant data processing steps and optimizing data flows to improve efficiency.

Apache Flink provides a native lineage support by providing an internal lineage data model and [Job Status Listener]({{< ref "docs/deployment/advanced/job_status_listener" >}}) for
developer to integrate lineage metadata into external lineage system, for example [OpenLineage](https://openlineage.io). When a job is created in Flink runtime, the JobCreatedEvent 
contains the Lineage Graph metadata that will be sent to Job Status Listeners.

# Lineage Data Model
Flink native lineage interfaces are defined in two layers. The first layer is the generic interface for all Flink jobs and connector, and the second layer defines
the extended interfaces for Table and DataStream independently. The interface and class relationships are defined in the diagram below.

{{< img src="/fig/lineage_interfaces.png" alt="Lineage Data Model" width="80%">}}

By default, Table related lineage interfaces or classes are used in Flink Table environment, thus Flink users doesn't need to touch these interfaces. The Flink community will gradually support all
of the common connectors, such as Kafka, JDBC, Cassandra, Hive. If you have a customized connector defined, you need to have customized source/sink implementations of the LineageVertexProvider interface.
Within a LineageVertex, a list of Lineage Datasets are defined as metadata for Flink source/sink. 


```java
@PublicEvolving
public interface LineageVertexProvider {
  LineageVertex getLineageVertex();
}
```

For the interface details, please refer to [FLIP-314](https://cwiki.apache.org/confluence/display/FLINK/FLIP-314%3A+Support+Customized+Job+Lineage+Listener).

## Complete SQL column lineage in this development branch

This branch captures logical table and column dependencies before optimization
and carries them through direct translation and compiled-plan restore. If the
optimizer removes a scan, such as in `WHERE 1=0`, its dependency remains in the
lineage graph without restoring a source transformation. The graph represents
SQL dependencies, not an audit of tables physically read by the job.

Compiled sink metadata includes `prunedSources`: an empty list certifies that
no logical source was removed; entries freeze the identities, namespaces and
resolved table definitions of removed scans. Runtime sources must match the
remaining expected identities exactly. Missing runtime sources, conflicting
pruning metadata and unresolved input fields remain errors before submission.
Older plans without `prunedSources` must be recompiled; manually adding an empty
list is not a supported migration.

Freezing removed scans requires `table.plan.compile.catalog-objects=ALL`.
Snapshots include schema and connector options, even for temporary tables, so
compiled plans must be protected as sensitive artifacts. `SCHEMA` and
`IDENTIFIER` policies are not silently overridden. Source, input-format and
legacy source-function providers can supply snapshots without running a job;
providers requiring a DataStream/transformation execution to establish identity
and ambiguous definitions of the same pruned table are rejected.

The lineage extraction gate does not guarantee that an external listener has
successfully delivered the event. Delivery semantics are the listener's
responsibility and require separate validation.

## SQL support boundary and verification

This development branch validates column lineage for projection and aliases,
expressions and functions, joins, aggregations and grouping, unions, windows,
CTEs and covered subqueries, and StatementSet writes. A StatementSet with
multiple named sinks keeps each sink's output fields and input dependencies
independent. Restored compiled plans preserve the same column lineage in the
final `JobCreatedEvent`.

Lineage is optional metadata and must not change Flink execution semantics. If
lineage metadata is missing, malformed, uses an unknown format version, or
cannot be derived for a planner shape, the job may still execute; the emitted
`LineageGraphObservation` reports `UNAVAILABLE` or `PARTIAL` and records the
issue. Invalid SQL and invalid executable plans still fail normally. This
branch does not claim complete column lineage for arbitrary DataStream code or
for every SQL construct, connector-specific schema, `IN`/`EXISTS` query, UDTF,
CEP, or `MATCH_RECOGNIZE` form.

The focused Flink 2.4 verification command is:

```bash
JAVA_HOME=/path/to/jdk-17 \
  ./mvnw -s tools/ci/google-mirror-settings.xml \
  -pl flink-table/flink-table-planner \
  -Dflink.markBundledAsOptional=false \
  -DskipITs -Dcheckstyle.skip -Drat.skip=true \
  -Dtest=ColumnLineagePropagationTest,ColumnLineageSubmissionGateITCase test
```

The current branch passed 51 tests (41 propagation and 10 submission-gate
tests). This verifies Planner extraction, compiled-plan restore, StatementSet
fan-out, final `JobCreatedEvent` visibility, execution-status lineage metadata,
and execution/lineage failure isolation. The submission-gate suite runs through
the MiniCluster Dispatcher: the planner writes a versioned runtime-neutral
lineage payload into the submitted `JobGraph`, the Dispatcher decodes it, and
the existing `DefaultJobCreatedEvent` is delivered exactly once with the full
`columnRelations()` graph. Client-side duplicate notifications are disabled so
the Dispatcher is the single owner of job-created lineage events.

The same transport has also been exercised through a separately deployed Flink
2.4 Session Cluster. A network SQL submission reached `FINISHED`, wrote the
expected output, and delivered START/RUNNING/COMPLETE OpenLineage events to an
HTTP collector. The COMPLETE event contained table lineage and a
`columnLineage` facet for the output fields. The transport additionally carries
connector options, table kind, comment, and field names; the OpenLineage Flink
2 adapter reconstructs a lightweight `CatalogBaseTable` from that payload when
the planner-only table object is not visible to the listener, so connector
visitors can keep using their normal option-based identification logic.

The external acceptance used JDK 17 in the runtime image and the local
OpenLineage all-in-one jar. The normal Gradle dependency-resolution path could
not be used in this environment because Maven Central returned HTTP 403 for
Flink shaded artifacts; the changed adapter was compiled directly against the
local Flink 2.4 runtime and then exercised by the deployed listener. This does
not imply that every Flink SQL operator or every external listener delivery
path is supported.

{{< top >}}

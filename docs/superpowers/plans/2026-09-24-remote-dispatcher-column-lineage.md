# Remote Dispatcher Column Lineage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Planner-produced table and column lineage survive client-to-Dispatcher submission and be delivered to the configured Job Status Listener as a complete `JobCreatedEvent` in remote Session Cluster deployments.

**Architecture:** Keep planner-only catalog objects on the client. At stream-graph generation, convert the lineage graph into a versioned, runtime-neutral transport payload stored in the job configuration. On the Dispatcher/JobMaster side, decode that payload into runtime lineage implementations and emit one Dispatcher-owned job-created event before execution status events. Preserve the existing client/local path during migration and make malformed or unknown payloads non-blocking with an explicit unavailable observation.

**Tech Stack:** Flink 2.4 modules, Java 17, shaded Jackson, `JobGraph`/`StreamGraph` job configuration, Job Status Listener SPI, MiniCluster and remote Session Cluster tests.

**Spec:** `docs/content/docs/internals/data_lineage.md`

## Global Constraints

- The transport format must be versioned and runtime-neutral; it must not contain `CatalogContext`, `CatalogBaseTable`, planner classes, anonymous connector implementations, or user class instances.
- Lineage extraction and transport failures must never veto valid Flink execution.
- Existing local/client listener behavior and existing table/column lineage interfaces must remain source-compatible unless a compatibility adapter is added and tested.
- Full column relations, source/sink dataset identities, field names, dependency types, origins, transformations, and supported dataset facets must be preserved.
- Unknown format versions, malformed payloads, missing payloads, and payloads that fail validation must produce `UNAVAILABLE`/issues and continue execution.
- A passing MiniCluster test alone is insufficient; at least one actual remote Session Cluster submission must exercise the Dispatcher-owned listener path.

## Review Focus

- Module boundary: the Dispatcher must emit a job-created event without introducing a runtime/streaming-java Maven cycle; test the compatibility event type seen by existing listeners.
- Payload fidelity: a two-sink StatementSet must retain distinct output datasets and column relations after transport; test both table and column identities.
- Compiled plan restore: batch compiled plans must carry and restore the same transport payload; test final event contents after restore.
- Failure isolation: corrupt, unknown-version, and unsupported payloads must not prevent the job from running and must expose an explicit lineage issue.
- Lifecycle duplication: remote submission must emit exactly one Dispatcher-owned job-created event, not one client event plus one Dispatcher event.

### Task 1: Define the runtime-neutral transport contract

**Files:**
- Create: `flink-runtime/src/main/java/org/apache/flink/streaming/api/lineage/LineageGraphTransport.java`
- Create: `flink-runtime/src/main/java/org/apache/flink/streaming/api/lineage/LineageGraphTransportFormat.java`
- Create: `flink-runtime/src/test/java/org/apache/flink/streaming/api/lineage/LineageGraphTransportTest.java`
- Modify: `flink-core/src/main/java/org/apache/flink/core/execution/DefaultJobExecutionStatusEvent.java`

**Interfaces:**
- Consumes: `LineageGraph`, `LineageDataset`, `ColumnLineageRelation`, `LineageEdge`.
- Produces: `serialize(LineageGraph): String`, `deserialize(String): LineageGraph`, and a stable configuration key plus format-version constant.

- [x] **Step 1: Write transport round-trip tests**

  Build a graph containing two sources, two sinks, schema facets, one direct input, one indirect input, all three origins where applicable, and a transformation string. Assert that deserialization preserves dataset name/namespace, field names, dependency types, origins, transformations, edge count, and facet names.

- [x] **Step 2: Write failure tests**

  Assert that missing payload, malformed JSON, unknown `formatVersion`, duplicate output relations, and references to unknown datasets throw a typed transport exception without invoking planner code.

- [x] **Step 3: Implement explicit DTO serialization**

  Serialize only stable scalar fields, dataset identities, field names, boundedness, relation references, and facet payloads. Do not serialize planner catalog fields or Java implementation class names. Use the shaded Jackson mapper already used by Flink runtime.

- [x] **Step 4: Implement runtime reconstruction**

  Reconstruct generic runtime datasets, source/sink vertices, edges, inputs, and relations, then run the existing `DefaultLineageGraph` validation before returning the graph.

- [x] **Step 5: Run the transport tests**

  Run:

  ```bash
  JAVA_HOME=/Users/xujiawei/Library/Java/JavaVirtualMachines/ms-17.0.14/Contents/Home \
  PATH=/Users/xujiawei/Library/Java/JavaVirtualMachines/ms-17.0.14/Contents/Home/bin:$PATH \
  ./mvnw -s tools/ci/google-mirror-settings.xml -pl flink-runtime \
    -DskipITs -Dcheckstyle.skip -Drat.skip=true \
    -Dspotless.check.skip=true -Dtest=LineageGraphTransportTest test
  ```

  Expected: all transport round-trip and rejection tests pass.

### Task 2: Carry the payload through StreamGraph and compiled plans

**Files:**
- Modify: `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamGraphGenerator.java`
- Modify: `flink-runtime/src/main/java/org/apache/flink/streaming/api/graph/StreamGraph.java`
- Modify: compiled-plan serialization code that currently writes `tableLineage` and `columnLineage` metadata.
- Test: `flink-table/flink-table-planner/src/test/java/org/apache/flink/table/planner/lineage/ColumnLineageSubmissionGateITCase.java`

**Interfaces:**
- Consumes: `LineageGraphTransport.serialize`.
- Produces: a job-configuration payload available from both direct submission and compiled-plan restore.

- [x] **Step 1: Add a payload-presence assertion to direct submission**

  Assert that the generated `StreamGraph` job configuration contains the transport key and the expected format version before submission.

- [x] **Step 2: Write compiled-plan round-trip test**

  Compile the existing batch plan, inspect the serialized plan for the payload, restore it, and assert the restored `JobCreatedEvent` graph has the same column relation identities as the original graph.

- [x] **Step 3: Store the payload after lineage observation**

  Serialize the observed graph after Planner extraction. If serialization fails, store an unavailable status and issue while leaving the executable graph unchanged.

- [x] **Step 4: Preserve legacy plans**

  Keep plans without the new payload executable. Mark their remote lineage observation unavailable rather than attempting to infer missing relations.

- [x] **Step 5: Run direct and restore tests**

  Run `ColumnLineagePropagationTest` and `ColumnLineageSubmissionGateITCase`; expected counts are 41 and at least 10 respectively, with the new payload assertions passing.

### Task 3: Move job-created event delivery to a Dispatcher-compatible API

**Files:**
- Modify: `flink-core/src/main/java/org/apache/flink/core/execution/JobStatusChangedEvent.java`
- Create or modify the runtime/core job-created event interface and implementation in the module that can be used by Dispatcher without a Maven cycle.
- Modify: `flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/execution/JobCreatedEvent.java`
- Modify: `flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/execution/DefaultJobCreatedEvent.java`
- Modify: `flink-clients/src/main/java/org/apache/flink/client/deployment/executors/AbstractSessionClusterExecutor.java`
- Modify: `flink-clients/src/main/java/org/apache/flink/client/deployment/executors/LocalExecutor.java`
- Test: existing listener compatibility tests plus a new event compatibility test.

**Interfaces:**
- Consumes: decoded runtime `LineageGraph`, `JobID`, job name, execution mode, and submission identity.
- Produces: one event type that Dispatcher can construct and existing listener implementations can recognize through a compatibility adapter.

- [x] **Step 1: Add a failing compatibility test**

  Register a listener compiled against the existing streaming `JobCreatedEvent` contract and assert that a Dispatcher-created event is recognized as a job-created event with a non-empty graph.

- [x] **Step 2: Choose the dependency-safe event location**

  Place the canonical event contract in a lower-level module that can see the runtime lineage interfaces, and retain the existing streaming package as a source-compatible adapter. Do not add a `flink-runtime` dependency on `flink-streaming-java`.

- [x] **Step 3: Keep local/client behavior compatible**

  Route local execution through the same canonical event factory while preserving submission identity and execution mode.

- [x] **Step 4: Run module compile and compatibility tests**

  Build `flink-core`, `flink-runtime`, `flink-streaming-java`, and `flink-clients` with the focused event tests. Expected: no Maven cycle and existing listener tests remain green.

### Task 4: Emit exactly one complete event from the Dispatcher

**Files:**
- Modify: `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/DefaultExecutionGraphBuilder.java`
- Modify: `flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/DefaultExecutionGraph.java`
- Modify: `flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java` or the selected JobMaster runner factory.
- Test: runtime Dispatcher/JobMaster tests for single event and malformed payload handling.

**Interfaces:**
- Consumes: job-configuration transport payload and the Dispatcher-created listener list.
- Produces: one complete job-created event before execution-state events; subsequent status events retain existing lineage status metadata.

- [x] **Step 1: Write the single-event test**

  Submit a remote execution plan with a listener factory and assert exactly one job-created event, followed by status events, with the full decoded graph.

- [x] **Step 2: Decode at the Dispatcher boundary**

  Read the payload using the system/runtime classloader, reconstruct the graph, and create the event before scheduling. Do not rely on the transient `StreamGraph.lineageGraph` field.

- [x] **Step 3: Fail closed for lineage only**

  On decode or validation failure, emit an unavailable observation with an issue and continue normal job scheduling.

- [x] **Step 4: Remove duplicate remote client notification**

  Gate the client-side `notifyJobStatusListeners` call for remote Session Cluster execution so the Dispatcher is the sole owner of the remote job-created event. Keep LocalExecutor behavior covered separately.

- [x] **Step 5: Run runtime tests**

  Run the focused Dispatcher/JobMaster tests and inspect event order and count in the test listener.

### Task 5: Prove the real remote Session Cluster path

**Files:**
- Create: `flink-table/flink-table-planner/src/test/java/org/apache/flink/table/planner/lineage/RemoteSessionClusterColumnLineageITCase.java`
- Modify: test fixture configuration only as needed to start a session cluster and install the listener factory.
- Modify: `docs/content/docs/internals/data_lineage.md`

**Interfaces:**
- Consumes: Tasks 1–4 transport and event path.
- Produces: end-to-end evidence for remote Dispatcher-owned table and column lineage.

- [ ] **Step 1: Add remote StatementSet coverage**

  Submit a two-sink StatementSet through a real Session Cluster client and assert the single received event contains both sink datasets and their independent output/input field relations.

- [ ] **Step 2: Add remote batch compiled-plan coverage**

  Submit a restored batch compiled plan remotely and assert the same column relation identities and execution result.

- [ ] **Step 3: Add remote failure-isolation coverage**

  Submit an unknown-version or malformed lineage payload and assert the job still completes while the event reports unavailable lineage.

- [ ] **Step 4: Run the end-to-end suite**

  Run the new remote tests with JDK 17 and the Google mirror settings. Expected: remote Dispatcher event count is exactly one for each successful job, and all table/column assertions pass.

- [x] **Step 5: Update the documentation with the verified MiniCluster boundary and explicit remote gaps**

  Replace the current evidence-incomplete wording with the exact deployment modes and test command that passed. Keep unsupported SQL forms explicitly listed.

## Acceptance Gate

The implementation is complete only when Tasks 1–5 pass, the Maven graph has no new cycle, the remote Session Cluster test observes one Dispatcher-owned event containing full `columnRelations()`, and the existing 51 focused Planner tests remain green. Until then, the status remains `evidence_incomplete` for remote Dispatcher field lineage.

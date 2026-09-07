# Table Planner

This module connects Table/SQL API and runtime. It is responsible for translating and optimizing a table program into a Flink pipeline. 
For user documentation, check the [table documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/overview/).

This README contains some development info for table planner contributors.

## Immutables for rules

Calcite's `@ImmutableBeans.Property` has been removed in Calcite 1.28.0.

Since Flink 1.17, planner rules use [Immutables](http://immutables.github.io/) to generate immutable
classes for rule configs based on the provided interface. A config should be annotated with `@Value.Immutable`.
In case the config is a nested class, the enclosing one should be annotated with `@Value.Enclosing`.

Once a new rule config is written and annotated, compile the module to generate the immutable class
for that config. Generated code will be placed in `target/generated-sources/annotations`. The config
can then be instantiated with the help of the generated immutable class like `Immutable<EnclosingClassName>.<ConfigClassName>`.

In case of issues, please double-check if a required generated class is present.
As an example have a look at `org.apache.flink.table.planner.plan.rules.logical.EventTimeTemporalJoinRewriteRule`.
See also `org.apache.calcite.plan.RelRule` for detailed explanation from Calcite.

## Json Plan unit tests

Unit tests verifying the JSON plan changes (e.g. Java tests in `org.apache.flink.table.planner.plan.nodes.exec.stream`) 
can regenerate all the files setting the environment variable `PLAN_TEST_FORCE_OVERWRITE=true`.

## Optional lineage metadata in compiled plans (fork POC)

`DynamicTableSinkSpec.columnLineage` and `tableLineage` are independent,
optional observation extensions. Each newly written extension uses
`"formatVersion": 1`; this is not an ExecNode or state serializer version.
The extensions are excluded from sink execution identity and do not change
checkpoint/savepoint formats.

Restoration accepts version 1 and the existing unversioned POC layout.
Absent metadata remains absent. Unknown versions (including 0), malformed
version values, or invalid extension contents discard only that extension.
Executable plan parsing and validation remain unchanged: a malformed plan
or invalid execution configuration still fails normally. Readers must not
interpret missing metadata as a verified source-free query.

Observation status is carried separately through job execution events, not
inserted into the existing lineage graph JSON. Graph serialization retains
`lineageEdges`, `columnLineageRelations`, `sources`, and `sinks` (empty optional
column relations may be omitted). Dataset names use escaped catalog identifiers
so names containing dots or backticks remain unambiguous.

Per-output completeness covers every writer of that dataset. An independently
verified output may retain exact table/column dependencies when another output
is unavailable; unknown writer correspondence cannot be guessed. COMPLETE
describes supported logical planner dependencies, not UDF internals, physical
reads, or reliable delivery to an external collector.

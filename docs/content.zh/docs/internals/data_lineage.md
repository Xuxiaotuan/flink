---
title: 数据血缘
weight: 12
type: docs
aliases:
  - /zh/internals/data_lineage.html
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

# 原生血缘支持
数据血缘在数据生态系统中变得越来越重要。随着 Apache Flink 被广泛用于流数据湖中的数据提取和 ETL，我们需要一个端到端的沿袭解决方案，用于包括但不限于以下场景:
  - `数据质量保证`: 通过将数据错误追溯到数据管道内的来源来识别和纠正数据不一致.
  - `数据治理`： 通过记录数据来源和转换来建立明确的数据所有权和责任制.
  - `数据合规`: 通过在整个生命周期中跟踪数据流和转换，确保遵守数据隐私和合规性法规.
  - `数据优化`: 识别冗余的数据处理步骤并优化数据流以提高效率.

Apache Flink 为满足社区需求提供了原生的沿袭支持，它提供了一个内部沿袭数据模型和 [作业状态监听器]({{< ref "docs/deployment/advanced/job_status_listener" >}}) 以便开发人员将血缘元数据集成到外部系统中，例如 [OpenLineage](https://openlineage.io). 
在 Flink 运行时创建作业时，包含沿袭图元数据的 JobCreatedEvent 将被发送到这个作业状态监听器里.

# 血统数据模型
Flink 原生的 Lineage 接口分为两层定义，第一层是所有 Flink 作业和 Connector 的通用接口，第二层则单独定义了 Table 和 DataStream 的扩展接口，接口和类的关系定义如下图所示。

{{< img src="/fig/lineage_interfaces.png" alt="Lineage Data Model" width="80%">}}

默认情况下，Table 相关的 lineage 接口或类主要在 Flink Table Runtime 中使用，因此 Flink 用户不需要接触这些接口。Flink 社区将逐步支持所有
常见的连接器，例如 Kafka、JDBC、Cassandra、Hive 等。如果您定义了自定义连接器，则需要自定义 source/sink 实现 LineageVertexProvider 接口。
在 LineageVertex 中，定义了一个 Lineage Dataset 列表作为 Flink source/sink 的元数据。


```java
@PublicEvolving
public interface LineageVertexProvider {
  LineageVertex getLineageVertex();
}
```

接口详细信息请参考 [FLIP-314](https://cwiki.apache.org/confluence/display/FLINK/FLIP-314%3A+Support+Customized+Job+Lineage+Listener).

## 本开发分支的完整 SQL 字段血缘

本分支在优化前提取逻辑表和字段依赖，并将其传递到直接执行和编译计划恢复流程。
当优化器删除扫描节点（例如 `WHERE 1=0`）时，血缘图仍保留该逻辑依赖，
但不会把 Source Transformation 加回执行拓扑。因此，图表达的是 SQL 依赖，
不能将每个输入表都解释为作业实际读取过的表。

编译计划的 Sink 血缘元数据新增 `prunedSources`：空列表表示没有逻辑来源被裁剪；
非空列表保存被裁剪来源的身份、namespace 和已解析表定义快照。
运行时来源必须与扣除裁剪来源后的预期集合完全一致。
真正缺失的运行时来源、冲突的裁剪信息以及不存在的输入字段仍会在提交前报错。
缺少 `prunedSources` 的旧计划必须重新编译，不支持手工补空列表绕过检查。

来源快照要求 `table.plan.compile.catalog-objects=ALL`，包含 schema 和连接器选项，
临时表也不例外，因此编译计划应按敏感文件保护。
不会静默覆盖 `SCHEMA` 或 `IDENTIFIER` 序列化策略。
目前可从 Source、InputFormat 和已有的 SourceFunction Provider 获取快照而不运行作业；
需要执行 DataStream／Transformation 才能确定身份的 Provider，以及同一个被裁剪表的歧义定义，
仍会明确报错。

上述门禁保证的是血缘提取完整性，不保证外部监听器已经成功送达事件；
事件送达语义需要由监听器单独实现和验收。

{{< top >}}

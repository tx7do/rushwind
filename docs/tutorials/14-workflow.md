# 14 · 工作流

> 本章目标：掌握 workflow 域的形状——契约的生命周期面与实用面、memory 参考引擎与 Argo 适配器的分工；学会提交、观察相位、取消，以及 worker 的运行至终态循环。

前置：[第 02 章 · 引导装配](02-bootstrap-and-config.md)。

## 契约：生命周期面 + 实用面

workflow 域的核心 crate（`rushwind-workflow`）只有两个 trait。**生命周期面**来自上游 go-wind 的移植——`Client::close`、`Worker::stop`、`Worker::is_running`；**实用面**是框架调用者真正需要的四件事——提交、读相位、取消、运行至终态：

```rust
use rushwind_workflow::{WorkflowClient, WorkflowSpec};

let spec = WorkflowSpec::new(manifest_json).named("my-flow");
let handle = WorkflowClient::submit(&client, &spec).await?;
let status = WorkflowClient::status(&client, &handle).await?;
println!("{:?}", status.phase);
WorkflowClient::cancel(&client, &handle).await?;
```

两条设计铁律：

- **manifest 对契约完全不透明**。契约传输 `serde_json::Value`，解释它是引擎（和外部系统）的职责——契约不定义工作流 DSL。
- **相位是各家 agrees on 的最小集**：`Running` / `Succeeded` / `Failed` / `Cancelled` / `Unknown`——不发明各家没有的相位。

## 引擎：参考 + 外部

`rushwind-workflow-memory` 是语义基线：in-process 相位存储，`complete()` 钩驱动相位——引擎自身没有执行语义（运行不透明 manifest 是外部系统的职责），这正是它的诚实之处。新外部适配器先跟它对齐语义，再接真系统：

```rust
let engine = MemoryWorkflowEngine::new();
let handle = WorkflowClient::submit(&engine, &spec).await?;
engine.complete(&handle, Phase::Succeeded)?;   // 测试钩：驱动相位
```

`rushwind-workflow-argo` 适配 Argo Server 的 REST API（手写 reqwest，无 SDK）：submit 是 POST manifest 到 `/api/v1/workflows/{namespace}`，status 读 `status.phase` 映射契约相位，cancel 走 terminate 端点，404 = `NotFound`：

```rust
let engine = rushwind_workflow_argo::ArgoWorkflowEngine::new(
    rushwind_workflow_argo::ArgoOptions {
        server: "https://argo.example:2746".into(),
        namespace: "rushwind".into(),
        ..Default::default()
    },
)?;
```

一个 manifest 要么带名字（`WorkflowSpec::named` 或清单里自己的 `generateName`），要么引擎拒绝——不发明它无法兑现的名字。

## worker：运行至终态

`WorkflowWorker::run` 提交并轮询到终态相位；`stop` 翻转后所有在途 run 以 `Cancelled` 收场；`is_running` 反映存活计数：

```rust
let worker = rushwind_workflow_argo::ArgoWorkflowWorker::new(engine_ref);
let outcome = rushwind_workflow::WorkflowWorker::run(&worker, &spec).await?;
assert!(matches!(outcome.phase, Phase::Succeeded | Phase::Failed | Phase::Cancelled));
```

相位由**部署侧**驱动（memory 引擎的测试钩 / Argo 的真实执行）——worker 只负责轮询与停止，不执行 manifest。

## 动手练习

1. 用 memory 引擎提交一个工作流，`complete` 到 `Succeeded`，观察 `status` 的相位变化。
2. `stop` 一个在途 run，确认 `run` 以 `WorkflowError::Cancelled` 收场且 `is_running` 变 false。
3. 起 Argo Server（`kubectl port-forward`），用 `rushwind-workflow-argo` 提交一个 hello-world 工作流并轮询到终态。

---

[系列目录](README.md) | 上一章：[13 · 测试之道](13-testing.md)

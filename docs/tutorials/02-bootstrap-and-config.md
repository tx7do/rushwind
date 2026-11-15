# 02 · 装配器与配置

> 本章目标：学会两条装配路径——代码里的 `App::builder` 与配置驱动的 `Bootstrap`；掌握配置域的 `Source` 契约、八种来源与回退链。

前置：[01 · 生命周期与第一个服务](01-hello-rushwind.md)。

## 两条装配路径

第 01 章的 `App::builder` 是**代码装配**：引擎、路由、钩子都是 Rust 表达式。当这些决定想推迟到部署时（同一份二进制，测试环境配内存存储、生产环境配数据库），改用 `Bootstrap`（`rushwind-bootstrap`）——一份 YAML/JSON 文档装配出存储引擎、HTTP 端点与路由，全部挂在同一个生命周期下。

## 一份 YAML 装配整个应用

*取自 `examples/bootstrap-demo`，可直接 `cargo run -p bootstrap-demo`：*

```rust
use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use rushwind_bootstrap::{Bootstrap, BootstrapError, RouteInput, RouteSurface};
use rushwind_storage::{ColumnKind, Repository, Schema};
use rushwind_storage_memory::MemoryRepo;
use rushwind_transport::StopSignal;

const CONFIG: &str = r#"
app:
  name: bootstrap-demo
  version: v0.1.0
  stop_timeout_secs: 5
storage:
  engine: memory
  settings: {}
storage_endpoints:
  - nest: /widgets
    api: crud
servers:
  - kind: http
    bind: 127.0.0.1:0
    route_packs:
      - name: health
      - name: wired
"#;

fn schema() -> Schema {
    Schema::builder("widgets", "id")
        .column("name", ColumnKind::Text)
        .build()
        .expect("schema is valid")
}

fn assemble() -> Result<Bootstrap, BootstrapError> {
    let bootstrap = Bootstrap::from_yaml_str(CONFIG)?.storage_factory("memory", |_settings| {
        Box::pin(async move {
            let repo = MemoryRepo::new(schema())
                .map_err(|e| BootstrapError::Failed(e.to_string()))?;
            Ok(Arc::new(repo) as Arc<dyn Repository>)
        })
    });
    Ok(bootstrap
        .route_pack("health", |_input| {
            Ok(RouteSurface::new(
                Router::new().route("/health", get(|| async { "ok" })),
            ))
        })
        .route_pack("wired", |input: RouteInput| {
            let repository = input.repository.clone();
            Ok(RouteSurface::new(Router::new().route(
                "/wired",
                get(move || {
                    let repository = repository.clone();
                    async move { format!("repo={}", repository.is_some()) }
                }),
            )))
        }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrapped = assemble()?.build().await?;
    for endpoint in &bootstrapped.endpoints {
        println!("[demo] serving on {endpoint}");
    }
    let result = bootstrapped.app.run(StopSignal::new()).await;
    println!("[demo] lifecycle finished: {result:?}");
    Ok(())
}
```

四个要点：

- **YAML 决定"用什么"，代码决定"怎么造"**。`storage.engine: memory` 选中引擎名，但 schema 是应用知识而非配置，所以由 `storage_factory("memory", ...)` 闭包在装配期构造。
- **`storage_endpoints` 零代码挂出 CRUD**。`nest: /widgets` + `api: crud` 直接在 HTTP 面上挂出该资源的增删改查（底层是 `rushwind-storage-axum` 的 `CrudApi`，见[第 03 章](03-storage-contract.md)）。
- **`route_packs` 是具名路由包**。配置里声明名字，代码里用 `route_pack("名字", ...)` 提供内容；`RouteInput` 携带装配好的仓储句柄，`RouteSurface::new(Router)` 返回路由面。
- **文件配置等价**。`Bootstrap::from_yaml_path` 从磁盘读同一份结构。

认证/鉴权也能从配置接入：注册 `authn_factory`/`authz_factory` 工厂后，`authn.<instance>.engine` 这样的配置键会把认证器/授权器装配到 HTTP 面上（装配器内部的规则见[第 06 章](06-auth.md)的层序部分）。

## 配置域：`Source` 契约

`rushwind-config` 定义了一份极小的契约——**按键取原始字节**：

```rust
pub trait Source: Send + Sync {
    fn load<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ConfigError>>;
    fn watch<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Box<dyn SignalStream>, ConfigError>> { /* 默认：NotWatchable */ }
    fn watch_value<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Box<dyn ValueStream>, ConfigError>> { /* 同上 */ }
}
```

三种能力、八种实现：

| 实现 | load | 推送监听 | 说明 |
|:---|:---|:---|:---|
| `config-file` | 读整个文件 | ✓（监听父目录） | 文件缺失是 `Err`，不是 `Ok(None)` |
| `config-env` | 读环境变量 | ✗ | 未设置 = `Ok(None)`，专为回退链设计 |
| `config-consul` | Consul KV | ✓（阻塞查询） | 缺 key（404）= `Ok(None)`；live |
| `config-etcd` | etcd KV | ✓（原生 watch） | 缺 key = `Ok(None)`；live |
| `config-http` | HTTP GET | 轮询（默认 30s） | 404 = `Ok(None)`，其余非 2xx = `Err` |
| `config-embedded` | 编译期嵌入 | ✗ | 链接期事实 |
| `config-apollo` | Apollo Server REST | 长轮询 | canonical key-sorted JSON；404 = `Ok(None)` |
| `config-nacos` | Nacos v1 cs/configs REST | 轮询（默认 30s） | 原文直传；404 = `Ok(None)`；tenant/accessToken |
| `config-kubernetes` | K8s ConfigMap REST | 轮询（默认 30s） | `data` map 规范化 key-sorted JSON；404 = `Ok(None)`；namespace/token |

每个引擎都有 `from_settings(serde_json::Value)`，对应 bootstrap 工厂的 settings 线格式：

```rust
// consul：{ "addr": ..., "path": ... }（两者必填）
let source = rushwind_config_consul::ConsulSource::from_settings(
    serde_json::json!({ "addr": "http://127.0.0.1:8500", "path": "myapp" }),
)?;

// etcd：{ "endpoints": [...], "key": 可选默认键 }
let source = rushwind_config_etcd::EtcdSource::from_settings(
    serde_json::json!({ "endpoints": ["http://127.0.0.1:2379"] }),
).await?;

// env：前缀 + 默认键
let source = rushwind_config_env::EnvSource::new(
    rushwind_config_env::EnvOptions::new().with_prefix("MYAPP_"),
);

// http：{ "url": ..., "poll_interval_ms": ..., "timeout_ms": ... }
```

## 回退链：`FallbackSource`

```rust
let source = FallbackSource::new(vec![
    Arc::new(consul_source),   // 远端优先
    Arc::new(file_source),     // 本地文件兜底
    Arc::new(env_source),      // 环境变量最后
])?;
let bytes = source.load("db.url").await?;
```

按顺序尝试，第一个 `Ok(Some(bytes))` 胜出。**缺 key 的 `Ok(None)` 是回退的行进信号，不是错误**；只有当所有来源都失败/缺席时，才汇总报错（join 起来的失败详情优先于笼统的 `Unresolved`）。

踩坑：`watch_value`（推送值流）会被回退链合并——哪个子来源先推来值就用哪个；但 `watch`（信号流）不合并。文件引擎监听的是**父目录**而不是文件本身（编辑器保存走临时文件重命名），且首个内容不会在监听开始时立刻推送。

## 踩坑清单

- **"缺席"语义因引擎而异**。env/consul/etcd/http 的缺席是 `Ok(None)`（回退信号），file 的缺席是 `Err`。把文件来源放进回退链时，缺失文件会直接终止回退而不是跳到下一来源——文件是"必须存在的兜底"时的正确选择。
- **`watch` 与 `watch_value` 是两种能力**。子来源没实现时默认返回 `ConfigError::NotWatchable`；回退链靠它跳过不具备能力的子来源。
- **`from_settings` 的键是 snake_case 字段**。写 YAML/JSON 装配文档时按各引擎 `Settings` 结构的字段名来。
- **配置域与脚本域有一座桥**。`rushwind-script-config` 能把任意配置 `Source` 适配成脚本 `ScriptSource`，让脚本热更跟着配置监听走（见[第 11 章](11-script-engines.md)）。

## 动手练习

1. 把 bootstrap-demo 的 `CONFIG` 拆成 `demo.yaml`，改用 `Bootstrap::from_yaml_path`，跑通后再改动文件重启观察差异。
2. 写一个三来源回退链（http → file → env），用 `cargo run` 起一个最小 HTTP 服务当配置源，验证 `Ok(None)` 的回退行为。
3. 在 `route_pack` 里用 `RouteInput` 把配置中的 `storage.settings` 打印出来，体会"settings 是配置、schema 是代码"的分界。

---
[系列目录](README.md) | 上一章：[01 · 生命周期与第一个服务](01-hello-rushwind.md) | 下一章：[03 · 存储契约](03-storage-contract.md)

# HTTP 边缘：错误信封、请求中间件栈与认证鉴权桥

本文是 HTTP 边缘域（`rushwind-http`）的权威设计文档：补齐 [security-authn-authz.md](./security-authn-authz.md) 留给"HTTP 族"的执法空位——那份文档认定会话传输的唯一挂载点是门链，而 axum 路由的逐路由执法是应用装配；本文就是那层装配的框架级供给。

## 定位

`rushwind-transport-axum` 只负责把一个 [`Router`](../crates/rushwind-transport-axum/src/lib.rs) 接进生命周期（绑定、排水、停机）；业务路由周围的一切——统一错误响应、请求中间件栈、认证鉴权——都在本 crate。与 `rushwind-storage-axum`（存储线的 HTTP 端点）、`rushwind-health` 的探针 handler 同属"各域自带 axum 边缘"的模式，本 crate 是通用业务路由的那块边缘。

单 crate、无契约/引擎分体：axum 就是唯一的 HTTP 引擎，为假想的第二框架抽象中间件 trait 属于投机泛化。

## 错误信封

`HttpError` 信封有四个字段（不设服务端 `Cause`/`StackTrace`——异常链与调用栈属于日志，不上线）：

| 字段 | 载体 | 语义 |
|:---|:---|:---|
| `code` | `Code` 枚举 | 对齐 gRPC 码名（`UNAUTHENTICATED`/`ALREADY_EXISTS`/`RESOURCE_EXHAUSTED`/`DEADLINE_EXCEEDED`…），`status()` 按 gRPC→HTTP 状态映射表映射 400/401/403/404/409/429/500/501/503/504 |
| `reason` | 稳定字符串 | 前端 i18n 替换键——服务端刻意不做 i18n，渲染分工在前端 |
| `message` | 人读文本 | 日志与调试用 |
| `details` | `Option<Box<Value>>` | 结构化上下文；装箱让错误保持 ~72 字节，clippy `result_large_err` 之下 |

`IntoResponse` 把任何 `Result<T, HttpError>` 渲染成同一个 JSON 形状：

```json
{"code": "NOT_FOUND", "reason": "USER_NOT_FOUND", "message": "no such user", "details": {}}
```

两个源分类学内置 `From` 转换：

- `AuthnError` → 按其状态锚点：401 族原样 401，配置失败族 500；reason 一律取分类学的稳定 code（`AUTHN_MISSING_BEARER_TOKEN` 等）。
- `StorageError` → 按变体：`NotFound`→404、`InvalidQuery`→400/`INVALID_QUERY`、`Conflict`→409、`Unsupported`→501/`UNSUPPORTED`、`Timeout`→504/`DEADLINE_EXCEEDED`、`Backend`→500/`BACKEND_FAILURE`。

## 请求中间件栈

每个中间件是一个 `with_*` 自由函数（包 `Router` 返回 `Router`，与 `CrudApi::router()` 的装配风格一致），`HttpEdge` 把它们按固定次序串好：

| 请求序 | 包装器 | 语义 |
|:---|:---|:---|
| 1 | `with_request_id` | 回显入站 `x-request-id`，缺失则 OS CSPRNG 铸 32 hex（与 session 引擎同形）；插 `RequestId` 扩展 + 响应头。最外层——recovery 层在 panic 上下文里要读它 |
| 2 | `with_recovery` | axum 无内建 panic 隔离（panic 直接断连接）；把请求身份（request_id/method/path）挂成 `rushwind-panic` 上下文后 `catch_unwind`，回 `500/INTERNAL_PANIC` 信封。报告由进程级 hook 单行输出（target `rushwind.panic`，带请求上下文），本层不再自打日志——无双报 |
| 3 | `with_logging` | 每请求一个 `rushwind.http` span（与 storage-observe 的 `rushwind.storage` 同约定），记 method/path/request_id/status/latency；导出交 subscriber |
| 4 | `with_cors` | `CorsOptions`（origin 列表 + credentials + methods/headers/expose/max_age）→ tower-http 层；`*` + credentials 走逐请求镜像（浏览器拒绝 `*` 与 `ACAC: true` 并存） |
| 5 | `with_timeout` | 预算到点丢弃内层 future，回 `504/DEADLINE_EXCEEDED`——客户端见信封，handler 直接消失 |

`HttpEdge::new()` 默认开 recovery + request_id + logging，CORS/timeout 显式开启；`wrap()` 内部按**逆序 layer**（axum 后 layer 者在外层），与设置次序无关。

**刻意不做**：`ratelimit`/`circuitbreaker` 桥（两个契约域都在，但逐路由的键控与阈值是策略决策，属应用装配；桥是平凡的后补件）；`codec`/`crypto`/`metadata`/`validate`（DTO 层，应用形状）；`retry`（客户端侧，见 `rushwind-retry`）。

## 认证鉴权桥

security 契约明说 HTTP 逐路由执法是应用的中间件，本 crate 把这块写好：

- **`with_authn(router, authenticator)`**——收集请求头对（`Authenticator::authenticate` 的精确载体），走认证两半组合，成功则把 `AuthClaims` 与 `AuthContext{subject, claims}` 插入请求扩展；失败按 `From<AuthnError>` 回信封。
- **`Authenticated` / `OptionalAuthenticated`**——handler 参数提取器：前者缺失即 `401/AUTHN_UNAUTHENTICATED`，后者永不拒绝。
- **`with_authorization(_for/_claim)(router, engine, action, resource[, project 或 claims 键])`**——一个包装子树一个权限点：读认证扩展里的 claims（缺失是 401 不是 403——authz 没理由替 authn 挡枪），求值 `Engine::is_authorized`；放行 / `403/PERMISSION_DENIED`；引擎错误按分类学锚点回 403、稳定 code 作 reason——引擎故障拒绝而非放行。

这就是动态 RBAC 的形状：策略存 DB，启动与变更时 `set_policies` 热装（authz 引擎的内部可变性让重置对在途流量不可见），每个受保护路由组带自己的权限点。

**白名单是装配不是中间件**：豁免 login/captcha/refresh 这类公共端点，做法是"受保护子树挂层、公共子树不挂、`merge` 汇合"——axum 的 layer 只裹已加入的路由，装配次序即语义。

## 域挂载

- **`mount_health`**（feature `health`）：`/healthz` 常量 200（能应 HTTP 即活），`/readyz` 跑聚合器（down 即 503）。独立成 feature 只因 health 的 HTTP 检查器拖 reqwest。
- **`mount_metrics`**（feature `metrics`）：`/metrics` 出 Prometheus 文本格式。feature 只因 prometheus 客户端树重。

## 测试

全部走 `tower::ServiceExt::oneshot`，无监听套接字：信封形状与状态映射钉死、panic/超时/CORS 预检各一钉、request-id 回显/铸造、authn 三种拒绝 + 提取器、authz 无 claims/放行/拒绝/引擎错误、`HttpEdge` 装配次序与退出门。feature 门控测试文件以 `#![cfg(feature = ...)]` 置顶，无 feature 时为空编译。

## 设计决策清单

| 主题 | 决策 |
|:---|:---|
| 中间件形态 | `Router→Router` 自由函数 + `HttpEdge` 装配；axum layer 只裹先加入的路由，装配次序即语义 |
| 白名单 | 子树拆分 + merge，路径匹配消失在装配里 |
| 错误载体 | `HttpError: IntoResponse`，`Result<T, HttpError>` 即渲染 |
| 引擎错误拒绝码 | 锚 403，reason 取分类学 code |
| CORS | tower-http `cors` feature 承载预检语义，策略面收敛为 `CorsOptions` |
| panic 恢复 | `catch_unwind`（panic=unwind 前提下）；请求身份挂 `rushwind-panic` 上下文，报告由进程 hook 单行输出，本层不自打日志 |
| 请求上下文 | 请求扩展（`RequestId`/`AuthClaims`/`AuthContext`）+ 提取器，无贯穿式上下文参数 |

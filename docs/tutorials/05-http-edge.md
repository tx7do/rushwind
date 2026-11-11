# 05 · HTTP 服务栈

> 本章目标：掌握 `HttpEdge` 中间件栈、统一错误信封、CORS 与超时；理解"层只包早期路由"的装配规则与公开/保护子树的拆分。

前置：[01 · 生命周期与第一个服务](01-hello-rushwind.md)。

## 统一错误信封

HTTP 面上所有处理器返回同一种错误——`HttpError`（`rushwind-http`），线上渲染为四字段 JSON：

```json
{ "code": 404, "reason": "NOT_FOUND", "message": "widget 404 not found" }
```

```rust
pub type HttpResult<T> = Result<T, HttpError>;

fn handler() -> HttpResult<Json<Widget>> {
    let w = find(404).ok_or_else(|| HttpError::not_found("widget.missing", "widget 404 not found"))?;
    Ok(Json(w))
}
```

十档 `Code`（`BadRequest`…`DeadlineExceeded`）各有便捷构造器与固定状态码；`reason` 是稳定的机器可读码，`message` 给人看。两个现成的 `From` 桥让域错误直通信封：`From<StorageError>`（`NotFound`→404、`Conflict`→409、`Timeout`→504……）与 `From<AuthnError>`（认证失败 401、配置错误 500）。`details` 字段是 `Option<Box<Value>>`——特意装箱，把错误值压在小尺寸内以通过 `result_large_err` 检查。

## 中间件栈：两条路

**路线 A：`HttpEdge` 一站式。** 默认栈 recovery → request-id → logging，请求时序固定（后加的层包住先加的，builder 顺序无关）：

```rust
let app = HttpEdge::new()
    .with_cors(CorsOptions::new().with_allow_origin("https://app.example.com").with_allow_credentials(true))
    .with_timeout(Duration::from_secs(3))
    .wrap(router);
```

**路线 B：自由函数逐个叠。** `with_recovery`、`with_request_id[_generator]`、`with_logging`、`with_timeout`、`with_cors[_compat]` 都是 `Router -> Router`，适合只要其中一两层的场合。

超时层超时后回 `504/DEADLINE_EXCEEDED`；恢复层用 `catch_unwind`，panic 细节只进日志，线上 `500/INTERNAL_PANIC`。

## CORS 两版

`with_cors` 基于 tower-http；`with_cors_compat` 是手写层，行为与老客户端兼容。选项统一走 `CorsOptions`：

```rust
CorsOptions::new()
    .with_allow_origin("https://app.example.com")
    .with_allow_credentials(true)
    .with_allow_method("PATCH")
    .with_expose_header("x-request-id")
```

默认放行方法 `GET HEAD POST PUT PATCH DELETE OPTIONS`、默认放行头 `content-type, authorization, x-request-id`。两个特例：`*` 源 + 凭据的组合会按请求镜像 Origin（浏览器拒绝 `*` 与 `ACAC: true` 并存）；`without_allow_methods`/`without_allow_headers` 还原"从未配置"状态——连头都不发，而不是发空列表。

## 装配铁律：层只包早期路由

axum 的 `Router::layer` **只包裹在此之前加入的路由**。所以：

```rust
// ✅ 正确：先装配路由，最后包层
let protected = Router::new()
    .route("/widgets", post(create))
    .route("/widgets/:id", get(show))
    .layer(middleware::from_fn(...));

let public = Router::new().route("/health", get(health));

let app = public.merge(protected);   // 白名单 = 子树拆分 + merge
```

```rust
// ❌ 错误：先包层再加路由，后加的路由裸奔
let app = Router::new().layer(guard);
let app = app.route("/late", get(...));   // 不受 guard 保护
```

公开/保护路由的白名单是**装配动作**（拆子树、分别包层、`merge`），不是中间件里的路径白名单列表。认证桥 `with_authn`、鉴权桥 `with_authorization`（[第 06 章](06-auth.md)）同样遵守这条铁律。

## 健康与指标挂载

`HttpEdge` 有两个特性门挂载点（`features = ["health", "metrics"]`）：

```rust
let router = mount_health(router, health.clone());      // GET /healthz（liveness）、/readyz（readiness）
let router = mount_metrics(router, metrics.clone());    // GET /metrics（Prometheus 文本）
```

## 生成式路由面：`gen-http` 与 `http-binding`

proto 优先的服务可以走另一条路：在 `.proto` 里用 `google.api.http` 注解声明路由，`rushwind-gen-http` 从 descriptor set 生成路由表、表单绑定计划、错误状态表和每个服务的 `Handlers` trait（`async fn get(&self, ctx: RequestContext, req: Input) -> Result<Output, StatusError>`），`rushwind-http-binding` 提供运行时：表单绑定、Content-Type 编解码协商（Json/Form/Proto）、protojson 响应与四字段错误信封、绑定层（挂在最外层，编解码失败先于 401 回 `400/CODEC`）。

```rust
let cfg = CodegenConfig {
    proto_module_path: "admin_proto::proto",
    pool_expr: "admin_proto::pool()",
    auth_free: &[("admin.v1.AdminService", "Health")],   // 免认证操作清单
};
let source = rushwind_gen_http::generate_from_bytes(&descriptor_set_bytes, &cfg)?;
```

生成器是 fail-closed 的：正则约束路径、通配段、具名 body、路由遮蔽（同方法下 `{var}` 先注册遮蔽后注册的字面路径）都会让**构建失败**而不是上线一条语义漂移的路由。descriptor set 必须用 `protoc` 生成——protox 会丢自定义注解字节。

## 踩坑清单

- **`HttpEdge::wrap` 之后再 merge 路由，新路由不在栈里**。与"层只包早期路由"同源：先把所有子树 merge 完，最后交给 `wrap`。
- **鉴权桥依赖认证桥注入的 claims**。两者同用时，鉴权层先 `layer`、认证层后 `layer`（认证在外圈）——见[第 06 章](06-auth.md)。
- **authz 面上"没有 claims"是 401，不是 403**；引擎故障拒绝时回 403 带稳定 reason。别把两类问题混进同一个告警。
- **axum-only**：这层栈建立在 axum 语义上，别试图换 web 框架；要别的协议请用传输矩阵（[第 12 章](12-transports.md)）。

## 动手练习

1. 构造一棵公开子树（`/health`）+ 保护子树（`/admin`），先只给保护子树叠 `with_authn`，验证 `/health` 无凭据可访问、`/admin` 返回 401。
2. 写一个返回 `StorageError::Conflict` 的处理器，确认信封是 `409/CONFLICT`。
3. 用 `CorsOptions` 配出"允许凭据 + 具名源"，用 `curl -H "Origin: ..."` 验证预检与回显。

---
[系列目录](README.md) | 上一章：[04 · 查询语法](04-query-syntax.md) | 下一章：[06 · 认证与鉴权](06-auth.md)

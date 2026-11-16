# 03 · 存储契约

> 本章目标：掌握 `Repository` 契约的全部面——schema、租户上下文、查询、三种分页、upsert 与"错误是值"的哲学；理解"引擎只是构造细节"。

前置：无（可紧跟[第 02 章](02-bootstrap-and-config.md)）。

## 一套代码，多个引擎

`rushwind-storage` 定义契约：`Schema`、`Record`、`ListQuery`、`Repository`。`rushwind-storage-memory`（内存引用引擎）、`rushwind-storage-seaorm`（SQL 系）、`rushwind-storage-mongodb`、`rushwind-storage-elasticsearch` 等都是这份契约的实现。**引擎选择是构造函数里的一行，下游所有代码都是契约**：

```rust
// 内存引擎：测试与原型的参照实现
let repo = Arc::new(MemoryRepo::new(schema())?);

// SQLite（经 SeaORM）：同一套下游代码
let repo = {
    let repo = SeaRepo::sqlite_memory(schema()).await?;
    repo.migrate_create().await?;      // 建表
    Arc::new(repo)
};
```

*本节代码均取自 `examples/storage-basics`，可 `cargo run -p storage-basics` 观察两个引擎输出一致。*

## Schema：应用知识

```rust
fn schema() -> Schema {
    Schema::builder("widgets", "id")     // 表名 + 主键
        .column("name", ColumnKind::Text)
        .column("age", ColumnKind::Int)
        .column("score", ColumnKind::Real)
        .column("owner_id", ColumnKind::Int)
        .build()
        .expect("schema is valid")
}
```

## 租户上下文：`QueryCtx` 与 `Viewer`

契约的每个方法第一个参数都是 `QueryCtx`——它携带调用者身份，**行级隔离由引擎强制**，不是应用层的 if：

```rust
let all = QueryCtx::all_access();          // 全量访问（服务内部/管理面）
let outsider = QueryCtx::new(Viewer::own(2));  // 只能看/改 owner_id=2 的行

// viewer 2 只能看到自己的行
let mine = repo.list(outsider.clone(), &ListQuery::page(1, 10)).await?;
// 越权 get 直接 NotFound，跟"行不存在"无法区分
let hidden = repo.get(outsider, Value::Int(1)).await?;   // None
```

## 查询：`ListQuery` 的四个旋钮

```rust
let hot = ListQuery {
    filter: Some(FilterExpr::any([                       // OR 组（详见第 04 章）
        FilterExpr::cond("name", Op::Ilike, [Value::Text("a%".into())]),
        FilterExpr::cond("score", Op::Gt, [Value::Real(2.0)]),
    ])),
    sort: Sort::by("age", SortDir::Desc),
    ..ListQuery::page(1, 10)                             // 页码分页
};
let page = repo.list(all.clone(), &hot).await?;
println!("{} rows, total {}", page.items.len(), page.total);
```

三种分页策略：

```rust
Paging::Page   { page, size }        // 页码 + 页大小
Paging::Offset { offset, limit }     // 偏移量（后台批处理友好）
Paging::Token  { token, limit }      // 游标（keyset），响应里给 next_token
```

游标流式取全量的标准姿势——循环到 `next_token` 为 `None`：

```rust
let mut token = String::new();
loop {
    let stream = ListQuery { paging: Paging::Token { token: token.clone(), limit: 2 }, ..Default::default() };
    let page = repo.list(all.clone(), &stream).await?;
    // ...消费 page.items...
    match page.next_token { Some(next) => token = next, None => break }
}
```

投影用 `FieldMask`：

```rust
let masked = ListQuery { mask: Some(FieldMask::of(["id", "name"])), ..ListQuery::page(1, 1) };
```

## 写入与"错误是值"

```rust
// create：主键由引擎回填
repo.create(all.clone(), Record::new().set("name", "alpha").set("age", 5)).await?;

// upsert：同一扇门完成插入与更新
repo.upsert(all.clone(), zebra.clone()).await?;      // 不存在 → 插入
repo.upsert(all.clone(), zebra.set("age", 2)).await?; // 存在 → 更新

// get 未命中返回 Ok(None)——缺失不是错误
let missing: Option<Record> = repo.get(all.clone(), Value::Int(404)).await?;

// 查询非法才是错误：未知列 → StorageError::InvalidQuery
let bad = repo.list(all.clone(), &ListQuery::default()
        .filtered(FilterExpr::cond("nope", Op::Eq, [Value::Int(1)]))).await.unwrap_err();
```

区分"没找到"（`Ok(None)`/空页）与"查得不对"（`Err(InvalidQuery)`），调用方就不必靠字符串匹配错误文本。

## HTTP CRUD：一行挂载

`rushwind-storage-axum` 的 `CrudApi` 把契约直接变成 REST 面，第 02 章 bootstrap 配置里的 `storage_endpoints` 底层就是它：

```rust
use rushwind_storage_axum::CrudApi;

let viewer: ViewerFn = Arc::new(|headers: &HeaderMap| {
    match headers.get("x-owner-id") {
        Some(v) => Viewer::own(v.to_str().unwrap_or("0").parse().unwrap_or(0)),
        None => Viewer::all(),
    }
});

let router = CrudApi::new(repo.clone())
    .with_viewer(viewer)          // 从请求头推导租户身份
    // .with_auditor(auditor)     // 可选：审计钩子
    .router();
```

## 踩坑清单

- **schema 是应用知识，不进配置**。bootstrap 装配时它由 `storage_factory` 闭包捕获——同一份 YAML 在测试装配内存引擎、在生产装配 SeaORM 引擎时，schema 只在一处定义。
- **软删除需要表里先有墓碑列**。给存储叠软删除装饰器前，schema 里要有整型的 `deleted_at` 列（见[第 08 章](08-cache.md)）。
- **`list` 的 `total` 与 `items` 是分开的**：`total` 是过滤后的全量计数，`items` 才是本页内容——做分页 UI 时别拿 `items.len()` 当总数。
- **迁移**：`rushwind-storage-seaorm-migration` 提供由实体派生的 schema 迁移；原型期用 `SeaRepo` 的 `migrate_create()` 直接建表即可。
- **JSON 列的 kind 是 `ColumnKind::Json`**：载荷是 `Value::Json`（规范文本）或 `Value::Array`（类型化元素），标量也合法——`accepts` 对 JSON 列全放行。过滤走 `JSON_CONTAINS`/`ARRAY_CONTAINS`（JSON 列专属）与 `json_path` 点路径（缺失读 NULL）；各引擎的方言面与诚实拒绝见[第 04 章](04-query-syntax.md)，可跑示例在 `examples/storage-json-filter`（SQLite 的 REGEXP 拒绝是教义不是缺陷）。

## 动手练习

1. 把 `storage-basics` 的 `scenario` 对着第三个引擎跑一遍（比如 MongoDB），确认零改动。
2. 用 `Viewer::own` 写一个"每个租户只能列出自己行"的集成测试。
3. 用游标分页写一个导出函数，把全表按主键序流式写出。
4. 给 `meta` JSON 列写一个 `ARRAY_CONTAINS` 命中与一个 `json_path` EQ 精确的查询，对着内存与 SQLite 双引擎跑 `examples/storage-json-filter`，确认结果一致。

---
[系列目录](README.md) | 上一章：[02 · 装配器与配置](02-bootstrap-and-config.md) | 下一章：[04 · 查询语法](04-query-syntax.md)

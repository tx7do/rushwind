# 04 · 查询语法

> 本章目标：掌握 29 操作符分类学与 `FilterExpr` 过滤树；学会用 protojson 在线格式上表达同样的查询——与 Django ORM 的 field lookups 同族的表达力，落在类型化契约上。

前置：[03 · 存储契约](03-storage-contract.md)。

## 过滤树：`FilterExpr`

过滤条件是一棵两层可无限嵌套的树：组（AND/OR）+ 叶子（`field op values`）：

```rust
use rushwind_storage::{FilterExpr, Op, Value};

// name ILIKE 'a%' OR score > 2.0
let expr = FilterExpr::any([
    FilterExpr::cond("name", Op::Ilike, [Value::Text("a%".into())]),
    FilterExpr::cond("score", Op::Gt, [Value::Real(2.0)]),
]);

// 默认构造（FilterExpr::all）是 AND 组
let and_of = FilterExpr::all([
    FilterExpr::cond("age", Op::Gte, [Value::Int(18)]),
    expr,     // 组可以嵌组
]);
```

## 29 操作符分类学

契约操作符（`Op`）与线格式枚举（`Operator`，见 `proto/rushwind/storage/v1/query.proto`）共同构成 29 个成员的分类学，按族记忆：

| 族 | 操作符 |
|:---|:---|
| 比较 | `Eq` `Neq` `Gt` `Gte` `Lt` `Lte` |
| 字符串 | `Like` `NotLike` `Ilike` `Contains` `StartsWith` `EndsWith` |
| 集合 | `In` `NotIn` |
| 空值 | `IsNull` `IsNotNull` |
| 区间 | `Between` |
| Django 派生便捷族 | `Exact` `Icontains` `IstartsWith` `IendsWith` `Iexact` |
| 线格式独有、无关系型等价 | `Regexp` `Iregexp` `JsonContains` `ArrayContains` `Exists` `Search` |

## 线格式如何落到契约

`rushwind-storage-proto` 的 `wire` 模块把 29 个线格式操作符映射到契约，规则只有四条：

1. **同名直映**：比较、字符串、集合、空值、区间各族原样落到 `Op`。
2. **别名折叠**：`EXACT` → `Eq`。
3. **Django 派生族推导成 ILIKE 模式**——每个引擎都已实现大小写不敏感的 LIKE，所以：
   - `ICONTAINS` → `ILIKE '%{v}%'`
   - `ISTARTS_WITH` → `ILIKE '{v}%'`
   - `IENDS_WITH` → `ILIKE '%{v}'`
4. **尾巴在边界显式拒绝**：`REGEXP`/`IREGEXP`/`JSON_CONTAINS`/`ARRAY_CONTAINS`/`EXISTS`/`SEARCH` 返回 `StorageError::Unsupported`——绝不静默丢弃。`date_part`/`json_path` 字段同理。`IEXACT` 已是自有契约算子（2026-11-12 全矩阵落地：SQL 走 `LOWER(col) = LOWER(?)`、MongoDB 锚定折叠正则、ES/OpenSearch `term.case_insensitive`、ClickHouse `lowerUTF8`）；regex 族的落地前置是契约侧正则求值故事、JSON 族的落地前置是 `Value` 协议加 JSON/Array 种类。

测试 `operator_names_cover_the_taxonomy` 保证 29 个成员每一个都有映射或显式报错，永不 panic。

## protojson 形态

查询以 protojson 文档进站，字段名 lowerCamelCase 与 snake_case 都接受，枚举值是 proto 成员名。

叶子过滤文档（`filter_expr_from_json`）：

```json
{
  "type": "OR",
  "conditions": [
    { "field": "name", "op": "ICONTAINS", "value": "a" },
    { "field": "score", "op": "GT", "value": 2.0 }
  ],
  "groups": [
    { "type": "AND",
      "conditions": [
        { "field": "age", "op": "GTE", "value": 18 },
        { "field": "unit", "op": "IN", "values": [10, 20] }
      ]
    }
  ]
}
```

完整列表请求文档（`list_query_from_json`）：分页策略、过滤树、排序、投影一体化——

```json
{
  "paginationType": { "pageBased": { "page": 2, "pageSize": 10 } },
  "filter": { "filterExpr": { "type": "AND", "conditions": [ { "field": "name", "op": "ICONTAINS", "value": "a" } ] } },
  "sorting": [ { "field": "age", "direction": "DESC" } ]
}
```

分页四选一：`pageBased`（页码）/`offsetBased`（偏移）/`tokenBased`（游标）/`noPaging`（取全量，上限受契约的 `MAX_LIMIT` 保护）。省略 `paginationType` 时用契约默认分页。

## 对照表：Django lookup → 本框架写法

| Django 写法 | 契约 `Op` | 线格式 `op` |
|:---|:---|:---|
| `filter(age__gte=18)` | `Op::Gte` | `GTE` |
| `filter(name__icontains="a")` | `Op::Ilike`（推导） | `ICONTAINS` |
| `filter(name__istartswith="A")` | `Op::Ilike`（推导） | `ISTARTS_WITH` |
| `filter(id__in=[1,2])` | `Op::In` | `IN` |
| `filter(deleted__isnull=True)` | `Op::IsNull` | `IS_NULL` |
| `filter(score__range=(1,2))` | `Op::Between` | `BETWEEN` |
| `filter(name__regex=...)` | ✗ 边界拒绝 | `REGEXP` → `Unsupported` |

## 踩坑清单

- **组类型省略 = AND**。线格式里 `type` 未指定按 `AND` 处理；`OPERATOR_UNSPECIFIED` 则直接 `InvalidQuery`。
- **复合值不支持**。`value` 只接受 JSON 标量（bool/number/string/null）；数组与对象值在边界报 `Unsupported`。
- **数字的精度**：protojson 的 `google.protobuf.Value` 数值是 f64 载体，整数列会安全收整（判断在 i64 范围内才转 `Value::Int`），超大整数请用字符串传递的方案与引擎的列类型协商。
- **模式推导不改语义**：`ICONTAINS` 推导成 `ILIKE` 意味着用户输入里的 `%`/`_` 会被当作通配符——接受终端用户输入前先转义，或改用 `Eq` 族。

## 动手练习

1. 用 `FilterExpr` 写出 `(a=1 AND b=2) OR (c CONTAINS 'x' AND d IS NULL)`，跑在内存引擎上。
2. 把同一棵树写成 protojson 文档，用 `filter_expr_from_json` 解析，断言两边 `Debug` 输出一致。
3. 故意发一个 `op: "REGEXP"` 的请求，确认拿到的是带稳定 reason 的 `Unsupported`，而不是 500。

---
[系列目录](README.md) | 上一章：[03 · 存储契约](03-storage-contract.md) | 下一章：[05 · HTTP 服务栈](05-http-edge.md)

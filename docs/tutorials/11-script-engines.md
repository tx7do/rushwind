# 11 · 脚本引擎

> 本章目标：掌握能力探测式的脚本契约，理解五种引擎（CEL/Lua/JavaScript/Starlark/WASM）的能力边界与各自脾气，学会按"表达式、规则、完整程序"选型。

前置：[02 · 装配器与配置](02-bootstrap-and-config.md)（配置源桥接部分）。

## 契约：生命周期 + 正交能力

`rushwind-script` 不假设所有引擎生而平等。`ScriptEngine` 只约定生命周期（`init`/`close`/错误查看），其余能力——加载、执行、全局访问、宿主函数、模块、监听热更、沙箱、配额、同步执行——都是**独立 trait**，引擎用探针暴露自己有什么：

```rust
let engine = new_script_engine("cel")?;
engine.init().await?;

// 能力探测：拿到 Option，按需 downgrade
if let Some(loader) = engine.clone().as_loader() {
    loader.load_string("policy", "1 + 2").await?;
}
let value = engine.as_executor().unwrap().execute_string("policy", "1 + 2").await?;
assert_eq!(value, ScriptValue::Int(3));
```

`ScriptValue` 是数据-only 的枚举（`Null/Bool/Int/UInt/Float/String/Bytes/Array/Map`）——可调用值跨界一律变 `Null`。错误三态：`Failed`（引擎内失败）、`CapabilityNotSupported`（能力未实现）、`QuotaExceeded`。

工厂注册表把引擎类型名接到构造器，业务代码永远只见 `SharedEngine`（`Arc<dyn FullEngine>`）：

```rust
rushwind_script_lua::register();                       // 装配期：注册引擎工厂
let engine = rushwind_script::new_script_engine("lua")?; // 运行期：按名构造
```

## 五引擎能力矩阵

| 能力 | CEL | Lua | JavaScript | Starlark | WASM |
|:---|:-:|:-:|:-:|:-:|:-:|
| 加载/执行 | ✓ | ✓ | ✓ | ✓ | ✓（仅 `_start`） |
| 全局变量 | ✓ | ✓ | ✓ | ✓ | ✗ |
| 宿主函数 | 经 `call_function` | ✓ | ✗ | ✗ | ✗ |
| 模块 | ✓ | ✗（恒拒绝） | ✓ | ✓ | ✗ |
| 热更监听 | ✓ | ✓ | ✓ | ✓ | ✗ |
| 沙箱库开关 | ✗ | ✓（`set_open_libs`） | ✗ | ✗（固定标准库） | ✗ |
| 指令配额 | ✗ | ✓（debug 钩子） | 事后（无中断） | ✗ | ✗ |

## 各引擎的脾气

**CEL** —— 表达式引擎，适合规则/谓词。加载即编译，执行对全部已加载表达式求值并**返回最后一个结果**；全局变量经 `register_global` 注入。坑：宿主函数不能在表达式里调用（只在宿主侧 `call_function`）；上游解析器对某些畸形输入（包括空表达式）会 panic——已被 `catch_unwind` 包成 `Failed`，但别依赖。

**Lua** —— 能力最全：沙箱（安全构造只用 `StdLib::ALL_SAFE`，永远没有 `debug`/`ffi`）、指令配额（debug 钩子中止）。坑：**`execute`/`execute_string` 丢弃结果**（返回 `Null`），取值必须 `call_function`：

```rust
engine.clone().as_loader().unwrap().load_string("s", "function add(a, b) return a + b end").await?;
let args = [ScriptValue::Int(1), ScriptValue::Int(2)];
let sum = engine.call_function("add", &args).await?;   // Int(3)
```

**JavaScript** —— boa 引擎 + actor 架构（worker 线程独占 `!Send` 的解释器，命令走信道）。坑：**宿主函数注册恒失败**（boa 的闭包包装要 unsafe，工作区禁 unsafe）；`execute` 返回**结果数组**（与 CEL/Lua 的"末值/Null"都不同）；无中途中断，配额是事后检查。

**Starlark** —— 确定性构建语言。语义是**每次执行把累积的全部脚本按序重跑**（后脚本可见前脚本的全局），副作用脚本会重复生效；整数全局被钳到 32 位；宿主函数不可注册。

**WASM** —— wasmi 字节码引擎：加载 + 执行 `last` 模块的 `_start` 导出（没有 `_start` 就静默执行返回 `Null`），外加两个真能力面——**globals**（wasm 标量注册进 `env` 导入模块、注册表为唯一事实来源）与 **call_function**（按名调用导出函数，参数按签名严格编组、范围检查绝不截断）；`register_function` 维持拒绝（异步 HostFunction 对同步解释器的诚实桥需要 reactor，行内 poll 会死锁）。坑：源契约是 `String`，而编译器产物通常不是合法 UTF-8——手工编码的字节段才能走得通这条路径。

## 与配置域的桥

`rushwind-script-config` 把任意配置 `Source` 适配成脚本 `ScriptSource`——脚本热更于是可以蹭配置域的监听能力（etcd 原生 watch、consul 阻塞查询、文件系统通知）：

```rust
let source = rushwind_script_config::ConfigSource::new(Arc::new(etcd_source));
engine.as_loader().unwrap().set_source(Some(Arc::new(source)));
```

映射规则：配置的"缺席"（`Ok(None)`）⇒ 脚本的 `Failed`（未找到）；`NotWatchable` ⇒ `CapabilityNotSupported`；载荷必须是合法 UTF-8。

## 选型速查

- 校验请求、算折扣、判断开关 → **CEL**（表达式，快，无宿主函数需求时最省心）
- 可信脚本要调宿主能力、要沙箱配额 → **Lua**
- 已有 JS 策略代码要复用、只算数据 → **JavaScript**（不能回调和注册宿主函数）
- 类构建配置的确定性求值 → **Starlark**
- 把 WASM 模块当插件跑 → **WASM**（`_start` 约定）

## 踩坑清单

- **统一用 `factory()` 构造**（各引擎 crate 提供），而不是裸 `new()`——CEL 等引擎的热更监听任务需要工厂里的弱引用接线。
- **`execute` 语义五家三样**：CEL 末值、Lua 丢弃、JS 数组、Starlark 丢弃、WASM 看 `_start`——跨引擎抽象业务时以 `call_function`/全局读取为准。
- **Lua 的墙上时钟配额是事后检查**：真死循环只有指令配额能拦。
- **Starlark 重跑副作用**：别往 Starlark 脚本里写带副作用的逻辑，把它当纯函数集。

## 动手练习

1. 用 CEL 写一个"订单金额 > 1000 且会员等级 ≥ 3 打九折"的规则引擎，全局注入 `amount`/`level`，单测覆盖边界。
2. 同一业务用 Lua 表达（`function discount(amount, level) ... end` + `call_function`），对比两版的主观可维护性。
3. 把第 02 章的 etcd 配置源接到 Lua 引擎的 `ScriptSource` 上，改 etcd 里的脚本键值，观察热更生效。

---
[系列目录](README.md) | 上一章：[10 · 可观测性](10-observability.md) | 下一章：[12 · 多协议传输](12-transports.md)

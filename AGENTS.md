# AGENTS.md

ZmqSharp 工程约束。

## 测试

- 测试框架使用 xUnit;断言使用 FluentAssertions。
- 测试项目:`ZmqSharp.Tests`,通过 `InternalsVisibleTo` 访问库内部实现。

## 代码风格

- 不使用 `!` 空断言运算符;需要非空断言的地方使用 `is {} varname` 模式匹配。
- 使用集合字面量(collection literals)。
- 锁使用 `System.Lock`,不使用 `object` 作为 lock 对象。
- 使用最新 C# 风格;API 设计不受阻塞时代或 C 风格设计库的影响。
- 全异步链路:禁止阻塞调用、`.Result`、`.Wait()` 及阻塞式 IO API。
- 开启 `TreatWarningsAsErrors`,并以 `.editorconfig` + `dotnet format` 保持风格一致。

## AOT

- 必须完整支持 Native AOT:不依赖运行时反射或动态代码生成;
  序列化/元数据需求使用 source generator 方案。
- 库启用 AOT 兼容检查(`IsAotCompatible`),用 `ZmqSharp.AotSmoke` 的 `PublishAot` 验证。

## 命名空间

- 按层组织:`ZmqSharp.Messages` / `ZmqSharp.Zmtp` / `ZmqSharp.Transports` /
  `ZmqSharp.Patterns`。

## 文档

- 设计文档放 `docs/impl/`,编号递增(0001、0002…),每篇标注状态
  (draft / accepted / superseded)。

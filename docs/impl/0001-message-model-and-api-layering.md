# 0001 - 消息模型与 API 分层

状态:draft
日期:2026-08-06

本文档固定 ZMTP 风格 .NET 库的基础设计:API 分层、消息模型、队列语义与
内存所有权规则。ZMTP 解析细节、pattern 层与路由在后续文档展开。

## 1. 目标

面向现代 .NET 运行时的 ZMTP / ZeroMQ 风格通信库。

非目标:

- 不做 RPC 框架。
- 不自动 retry。
- 不提供业务 correlation。
- 不隐藏业务级 timeout。

## 2. 分层模型

```text
Application
  |
  +-- ZSocketChannel   高层:队列 API(Channel),构造时接管低层回调
  +-- ZSocket          pattern 层(REQ/REP, DEALER/ROUTER, PUB/SUB)
  +-- ZMTP Session     单 peer 连接:greeting / handshake / traffic
  +-- Transport        TCP / IPC / inproc(可插拔)
```

消息面跨两层:

- 低层:borrowed `ZMessageView`,零分配,仅回调期间有效。
- 高层:owned `ZMessage`(sealed class),可逃逸,消费方负责 Dispose。

## 3. 关键决策

| # | 决策 | 理由 |
|---|------|------|
| D1 | 不使用 `System.IO.Pipelines` | 消息协议本质是流式的,帧长前置,按需租用缓冲即足够;Pipe 的保留/Advance 语义带来 borrowed 与连续性的冲突,且其模型对非纯流式应用不够 |
| D2 | 低层 struct 视图 + 高层 class 包装,单次 move | 内部热路径零分配、可内联;边界一次性转移所有权,无需引用计数 |
| D3 | multipart 保留,帧为一级公民 | 路由信封、topic、REQ/REP 分隔帧都依赖帧边界(RFC 23) |
| D4 | 连续性逐帧、消费方驱动 | 帧结构(协议语义)与内存布局(性能)正交;连续隐含物化 |
| D5 | 原子收发 | RFC 23:message 的全部帧或无一帧 |
| D6 | pooled/owned 按段;无 Detach | 标准池抽象无逃生口;永久所有权必须在分配前定 |
| D7 | 背压归 Channel | `capacity` = HWM;满则暂停接收泵 + 迟滞续流;drop 可配置 |
| D8 | 接收策略可扩展 | 默认固定选项;v2 增加应用层 Decide 钩子 |

## 4. 低层回调契约

```csharp
public readonly struct ZMessageView
{
    public int FrameCount { get; }
    public ReadOnlySequence<byte> Frame(int i);
    public bool TryGetContiguousFrame(int i, out ReadOnlyMemory<byte> mem);
}

public delegate bool ZMessageHandler(ZMessageView message, CancellationToken token);
// true  = 继续接收
// false = 暂停接收泵(背压),由外部 Resume() 恢复
```

规则:

- borrowed:数据仅在回调执行期间有效,禁止保存引用。
- 同步、串行调用,不并发。
- `false` 只表达「暂停」;丢消息是高层 Channel 的策略,不混入低层 bool。

## 5. 高层 Channel 桥

```csharp
public sealed class ZSocketChannel : IAsyncDisposable
{
    // BoundedChannelOptions.Capacity = HWM
    public ChannelReader<ZMessage> Inbound { get; }
    public void CompleteInbound(Exception? error = null);
}
```

- 构造时订阅低层回调;回调内按接收策略物化消息并 `writer.TryWrite`。
- 满:`TryWrite` 失败 → 回调返回 false(暂停),后台续流等待
  `writer.WaitToWriteAsync()`,以 `Count <= capacity / 2` 做低水位迟滞,
  避免在满边界抖动。
- drop 模式(面向 PUB 类丢消息语义)作为显式选项,默认不启用。
- 协议异常 → `writer.TryComplete(exception)`,消费者可见。
- 关闭/完成时排空 Channel 并 Dispose 所有未消费消息,防止池泄漏。
- 水位线、读写者并发全部由 Channel 决定,库不引入额外信号量。

## 6. 消息模型

```csharp
public enum ZBufferOrigin { Pooled, Owned }

public sealed class ZMessage : IDisposable
{
    public static ZMessage FromOwned(byte[] data);           // 发送侧:永久所有权,零拷贝
    public int FrameCount { get; }
    public ZBufferOrigin Origin { get; }                     // 按段
    public ReadOnlySequence<byte> Frame(int i);
    public bool TryGetContiguousFrame(int i, out ReadOnlyMemory<byte> mem);
    public byte[] ToOwnedArray();                            // 保留:拷贝到 GC 存储
    public bool TryTakeOwner(out IMemoryOwner<byte> owner);  // 单帧:转移归还责任
    public void Dispose();                                   // 幂等
}
```

不变量:

- 连续消息 = 每个 frame 单段;否则允许 frame 跨段。
- 帧结构(协议语义)与内存布局(性能)正交。
- view 是借用的、绝不 Dispose;class 是唯一的 Dispose 责任点。
- `Dispose` 幂等(`Interlocked` 守卫),归还所有 Pooled 段;Owned 段仅释放引用。
- 所有权按段记录,multipart 可混合 Pooled / Owned。
- 内部表示:view 用帧表(offset/length 数组 + owner 引用),
  `ReadOnlySequence` 按需构建,避免每帧一个链式堆节点。

所有权路径:

- Pooled:库从 `MemoryPool<byte>` 租,`Dispose` 归还。
- Owned:`FromOwned`(发送)或 `ToOwnedArray`(接收后保留),GC 管理,不碰池。
- `TryTakeOwner`:单帧延迟归还(转移归还责任),不是永久所有权。
- 无 `Detach()`:标准池抽象不支持;永久所有权必须在分配前定。

## 7. 接收管线(无 Pipe)

```text
Socket.ReceiveAsync -> 池化 buffer -> parser -> 策略决策 -> 交付(view 或 owned class)
```

- 连续帧:帧头已含 size,按帧长 rent 池化 buffer,读满(ReadExactly 语义),
  单段交付。单次拷贝,交付缓冲即最终缓冲。
- 分段帧:按固定块(建议 8KB)rent,链成 `ReadOnlySequence<byte>`。
- `ContiguousFrameLimit` 默认 85,000(LOH 阈值):帧不超过阈值走连续;
  超过走分段(不进 LOH);超过 1MB(`ArrayPool<byte>.Shared` 的 2^20 上限)
  强制分段,不尝试连续。
- 不使用 Pipe 的理由:其模型面向「保留未消费字节」的流式解析;消息协议由
  帧头可知长度,按需租用更简单。纯流式语义下,分段/连续只是性能参数。

## 8. 发送路径

- `SendAsync(ReadOnlyMemory<byte>)`:库内拷贝(对应 `zmq_msg_init_buffer` 语义),
  调用方数据不受约束。
- `SendAsync(ZMessage)`:所有权转移,发送完成后由库 Dispose
  (对应 `zmq_msg_init_data` 语义)。
- 单连接单 writer;消息为原子单元;任一帧写失败或对端断开 → 终止连接,
  Dispose 在途消息,不交付一半。

## 9. 连接与重连

- 生命周期:connect → greeting → handshake → traffic → disconnect。
- 重连产生新的 ZMTP 连接,重新 greeting / handshake。
- 意外断连视为临时错误;重连退避随机化,避免连接风暴(RFC 23 Error Handling)。
- ERROR command 致命:关闭连接,且不以相同凭证重连。

## 10. 接收策略与扩展点

```csharp
public enum ZReceivePolicy { Borrowed, Pooled }

public sealed class ZReceiveOptions
{
    public ZReceivePolicy Policy { get; init; } = ZReceivePolicy.Pooled;
    public int ContiguousFrameLimit { get; init; } = 85_000;   // 0 = 全分段

    // v2:应用层策略钩子,当前保留空位
    public Func<ZReceiveContext, ZReceiveAction>? Decide { get; init; }
}

public readonly struct ZReceiveContext
{
    public ReadOnlySequence<byte> FirstFrame { get; }
    public long BytesSeen { get; }
    public long NextFrameSize { get; }
    public int FramesSeen { get; }
}

public readonly struct ZReceiveAction
{
    public ZReceivePolicy Policy { get; init; }
    public bool Contiguous { get; init; }
}
```

- 决策点唯一:parser 读到帧头后、物化前。v1 的固定逻辑就是这个点上的分支,
  加钩子只改一处。
- 约束:`Borrowed` 与强制连续互斥(连续隐含物化)。
- 应用层优势:首帧/前缀 + 帧长可预测「缓存 → Owned」「巨大 → 分段流式」;
  ZMTP 层自身无法预测消息总大小。

## 11. 工程约束

权威列表见 AGENTS.md。要点:测试 xUnit + FluentAssertions;不用 `!` 空断言,
需要时用 `is {}` 模式匹配;集合字面量;`System.Lock`;全异步链路;
最新 C# 风格;完整 AOT 支持。

## 12. 项目结构与命名空间

- 项目拆分:v1 保持单库(`ZmqSharp`)+ 测试项目(`ZmqSharp.Tests`)+
  AOT smoke(`ZmqSharp.AotSmoke`),不拆多个库。
  理由:库零外部依赖,单程序集对 AOT/裁剪最简单;分层用命名空间与依赖方向约束。
  拆分触发条件:出现第二个传输实现(如 TLS/QUIC),或协议层出现第二个消费方。
  届时拆 `ZmqSharp`(核心:消息模型、session、parser、pattern)+
  `ZmqSharp.Transports`(实现 `IZTransport`),依赖方向 Transports -> Core。
- 命名空间按层:`ZmqSharp.Messages` / `ZmqSharp.Zmtp` / `ZmqSharp.Transports` /
  `ZmqSharp.Patterns`。

## 13. 后续

- pattern 层(routing、REQ/REP 状态机)另立文档。
- 开放问题:连接级心跳(PING/PONG,ZMTP 3.1)、超大帧(>1MB)的流式消费 API。

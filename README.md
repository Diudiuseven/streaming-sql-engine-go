# streaming-sql-engine-go

面向事件流的持续查询引擎：用 SQL 子集订阅不断到来的事件，按事件时间做窗口聚合，
持续增量地产出结果。纯 Go 实现，无外部依赖。

## 快速示例

```go
eng := streamsql.NewEngine(streamsql.Config{
    AllowedLateness: 5 * time.Second,
})
q, err := eng.Register("per-user-count", `
    SELECT user, COUNT(*) AS c, SUM(amount) AS total
    FROM events
    WHERE amount > 0
    GROUP BY TUMBLE(ts, '10s'), user
    ORDER BY total DESC
    LIMIT 10`)
// ...
eng.Submit(streamsql.Event{ID: "e1", Timestamp: 1000,
    Fields: map[string]any{"ts": int64(1000), "user": "alice", "amount": int64(42)}})
eng.Sync()
for b := range q.Outputs() { /* ... */ }
```

## 支持的 SQL 子集

```sql
SELECT <expr> [AS alias], ...
FROM <stream>
[WHERE <bool expr>]
[GROUP BY TUMBLE(<time_col>, <dur>) | HOP(<time_col>, <slide>, <size>) [, <key col>...]]
[ORDER BY <output column> [ASC|DESC], ...]
[LIMIT <n>]
```

- **投影**：列引用（支持 `s.x` 限定名）、字面量、算术 `+ - * /`、聚合表达式。
  不支持 `SELECT *`（输出 schema 必须显式且稳定）。
- **过滤**：`= != <> < <= > >=`、`AND OR NOT`、括号、布尔字面量 `TRUE/FALSE`。
- **窗口**：`TUMBLE(ts, '10s')` 滚动窗口；`HOP(ts, '5s', '30s')` 滑动窗口
  （slide ≤ size）。窗口长度可用 Go 时长字符串（`'10s'`、`'5m'`）或整数毫秒。
- **聚合**：`COUNT(*)`、`COUNT(col)`、`SUM`、`AVG`、`MIN`、`MAX`。
  聚合参数可以是任意标量表达式（如 `SUM(price * qty)`）。
- **伪列**：分组查询中可投影 `window_start` / `window_end`（窗口边界，毫秒）。
- **ORDER BY** 只能引用 SELECT 输出列名（大小写不敏感）；**LIMIT** 作用于每个输出批次。

### 明确不支持（报错而非静默忽略）

`JOIN`、`HAVING`、`UNION/INTERSECT/EXCEPT`、`DISTINCT`、`IN/LIKE/BETWEEN`、`CASE`、
子查询、`INSERT/UPDATE/DELETE/DDL`、无窗口的全局聚合、slide > size 的 HOP、
标量函数。所有拒绝都通过可区分的错误类型返回：

| 错误种类 | 含义 |
|---|---|
| `ErrKindParse` | 语句无法解析或语义不合法（如列未出现在 GROUP BY） |
| `ErrKindUnsupported` | 语法合法但超出支持子集 |
| `ErrKindLateEvent` | 事件超出允许迟到范围，未被统计 |
| `ErrKindDuplicate` | 事件 ID 重复，未重复统计 |
| `ErrKindBackpressure` | 某查询输入缓冲已满，该事件仅对此查询丢弃 |
| `ErrKindCheckpoint` | 检查点读写失败 |

用 `streamsql.IsKind(err, streamsql.ErrKindLateEvent)` 判断。

## 时间语义

- **事件时间**：分组查询以 `GROUP BY TUMBLE/HOP` 指定的时间列（int64 毫秒）为准；
  无窗口的投影查询以 `Event.Timestamp` 为准。
- **水位线**：`watermark = max(已见事件时间) - AllowedLateness`。
  窗口触发完全由事件时间推进驱动，与处理时刻（墙钟）无关。
- **乱序**：水位线之前到达的事件正常归位到对应窗口。
- **迟到**：`ts <= watermark` 的事件被拒绝，不计入任何结果，
  并在该查询的 `Errors()` 通道上给出 `ErrKindLateEvent` 结论（`Stats.LateDropped` 计数）。
- **重复**：相同 `Event.ID` 不会重复统计，重复到达会在 `Errors()` 上给出
  `ErrKindDuplicate` 结论（`Stats.Duplicates` 计数）。去重条目在水位线越过其事件时间后
  被清除（此后再次到达的重复会被判为迟到，结论仍可区分）。
- **缺失/非法时间列**：事件被拒绝并报告（`Stats.BadEvents`），不会被静默归入错误窗口。

## 输出模型与确定性

- 每处理一个事件，只有**发生变化的聚合分组**会产出一个更新批次（`Row.Final=false`）；
  窗口因水位线关闭时产出最终批次（`Row.Final=true`）并释放状态。
- 连续运行下，每个分组的最后一次更新与其最终行一致，且全量最终行与把同样事件
  一次性批处理计算的结果逐条一致（有测试保证）。
- **排序规则**：批次内先按 `ORDER BY` 项排序（nil 在升序时排最后），
  并列时按整行值的规范编码（`%#v`）字典序排序——这是稳定且有文档的取舍规则；
  未指定 `ORDER BY` 时同样按规范编码排序。相同事件序列 + 相同配置 ⇒ 输出逐条一致。

## 状态生命周期

窗口关闭（`window_end <= watermark`）后立即产出最终行并释放该窗口的全部分组状态；
去重表随水位线推进同步清除。长时间运行时内存不随已关闭窗口数量增长
（`TestWindowStateReleased` 验证 1000 个窗口后驻留状态有界）。

## 检查点与恢复

```go
err := eng.Checkpoint(dir)          // 原子写入：temp 文件 + fsync + rename
eng2, err := streamsql.RecoverEngine(dir, cfg)
off := eng2.Offset()                // 从 off+1 开始重放事件
```

- 检查点先通过屏障等待所有查询处理完已提交事件，再序列化全部查询状态
  （水位线、分组聚合、去重表、计数器）与引擎偏移量。
- 写入中断不会留下不可读的半成品：残留临时文件被忽略；主文件带 magic + CRC32 校验，
  损坏时返回 `ErrKindCheckpoint` 而非读出半截状态。
- 恢复后从 `Offset()+1` 重放：已提交结果不重复产出（已提交事件的重放会被去重状态拦截），
  未提交事件不跳过。

## 多查询与背压

- 一个引擎实例可注册多条查询，共享同一事件流；每条查询有独立的 goroutine、
  输入缓冲、水位线与进度。
- `Submit` 对每个查询非阻塞分发：某查询输入缓冲满时，该事件**仅对此查询**丢弃，
  并在其 `Errors()` 上报告 `ErrKindBackpressure`（`Stats.Backpressured` 计数），
  提交者与其他查询不受影响。
- 查询的输出通道若无人消费，只会拖慢该查询自身（最终触发上述背压），不会拖垮上游。
- `Sync()` 会阻塞直到所有查询处理完已提交事件；若有查询因输出无人消费而永久停滞，
  `Sync`/`Checkpoint` 也会一直等待（这是有意语义）。`Close()` 总是可返回。

## 配置项

| 字段 | 默认 | 说明 |
|---|---|---|
| `AllowedLateness` | 0 | 允许迟到时长；水位线 = 最大事件时间 − 该值 |
| `BufferSize` | 1024 | 每查询输入缓冲容量（超出→背压结论） |
| `OutputBufferSize` | 1024 | 每查询输出缓冲容量（满则该查询阻塞在产出上） |
| `ErrorBufferSize` | 256 | 每查询错误通道容量（满则结论计数入 `Stats.ErrorsDropped`） |

## 测试

```sh
go test -race ./...
```

覆盖：乱序归位、迟到拒绝、重复去重、窗口按事件时间触发、增量结果与批处理一致、
确定性重放、窗口状态释放、检查点恢复（不重复不跳过、中断写入）、多查询并发与背压。

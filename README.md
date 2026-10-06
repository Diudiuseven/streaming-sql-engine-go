# streaming-sql-engine-go

面向事件流的持续查询引擎：用 SQL 子集订阅不断到来的事件，按**事件时间**做窗口聚合，持续增量地产出结果。支持乱序与迟到事件、精确一次去重、检查点恢复、多查询共享流与背压。

## 快速示例

```go
e := engine.New(engine.Config{AllowedLateness: time.Minute})
sub, _ := e.Subscribe(`
    SELECT user, COUNT(*) AS c, SUM(amount) AS total
    FROM orders
    WHERE amount > 0
    GROUP BY user, TUMBLE(ts, '5m')
    ORDER BY total DESC
    LIMIT 10`)
e.Publish(event.Item{Event: &event.Event{Seq: 1, ID: "e1", EventTime: t0, Fields: map[string]any{
    "user": "alice", "amount": int64(42), "ts": t0,
}}})
e.Publish(event.Item{Watermark: &event.Watermark{Seq: 2, T: t0.Add(5 * time.Minute)}})
for u := range sub.Updates { /* 增量更新与窗口最终结果 */ }
```

## 支持的 SQL 子集

```
SELECT <列 | 聚合 | WINDOW_START() | WINDOW_END() | *> [AS 别名], ...
FROM <源>
[WHERE <比较表达式，支持 AND/OR/括号>]
[GROUP BY <列>, ..., TUMBLE(<时间列>, '<时长>') | HOP(<时间列>, '<时长>', '<滑动>')]
[ORDER BY <输出列> [ASC|DESC], ...]
[LIMIT <n>]
```

- 聚合函数：`COUNT(*)`、`COUNT(col)`、`SUM`、`MIN`、`MAX`、`AVG`（数值列为 `int64`/`float64`）。
- 窗口：`TUMBLE`（滚动）、`HOP`（滑动，size 必须是 slide 的整数倍），时长用 Go duration 字符串（如 `'5m'`、`'1h30m'`），窗口按 Unix 纪元对齐。
- `WHERE` 支持 `= != < <= > >=` 与 `AND`/`OR`/括号；未知列按 NULL 处理（比较结果为 false）。
- 语义约束（违反时报 `ErrUnsupported`，不会静默忽略子句）：
  - 聚合必须配合 `GROUP BY` 或窗口；`SELECT` 中的普通列必须出现在 `GROUP BY` 中；
  - `SELECT *` 仅用于纯投影查询；
  - `ORDER BY`/`LIMIT` 仅作用于窗口聚合查询（在窗口关闭时的最终输出上生效）；
  - `WINDOW_START()/WINDOW_END()` 仅在窗口查询中可用。
- 明确不支持（报 `ErrUnsupported`）：`JOIN`、`HAVING`、`UNION`、`DISTINCT`、`IN`、`BETWEEN`、`LIKE`、`CASE`、子查询、`INSERT/UPDATE/DELETE/DDL` 等。

### 可区分的错误

解析失败返回 `*sqlparse.Error{Kind: ErrParse}`；语法合法但超出子集返回 `Kind: ErrUnsupported`。两者可通过类型断言区分，引擎绝不静默降级。

## 时间语义

- **事件时间驱动**：窗口触发只由水印（`event.Watermark`）推进决定，与处理时刻无关。
- **乱序归位**：事件按事件时间分配到窗口（半开区间 `[start, end)`，纪元对齐），乱序到达不影响归属。
- **迟到处理**：水位线 `W` 与 `AllowedLateness = L` 定义迟到地平线 `W - L`。事件时间 **早于** 地平线的事件不被计入，产出 `UpdateLateEvent`（可区分的迟到结论）；等于或晚于地平线的迟到事件正常计入。
- **精确一次**：按 `Event.ID` 去重，重复事件产出 `UpdateDuplicateEvent`。去重表只保留地平线之后的条目，随水印推进裁剪，内存有界。
- **窗口关闭与状态释放**：当 `窗口End + AllowedLateness <= 水印` 时窗口关闭，产出最终行（`Update.Final = true`）后立即释放状态；已关闭窗口数量不会使内存无界增长（有测试保证）。

## 输出模型（增量物化）

- `UpdateRow`（`Final=false`）：某分组聚合值**发生变化**时的更新（upsert 语义，携带当前完整行）；未变化的分组不产出。
- `UpdateRow`（`Final=true`）：窗口关闭时的最终行，按 `ORDER BY` 排序并应用 `LIMIT`。
- `UpdateLateEvent` / `UpdateDuplicateEvent`：迟到/重复事件的可区分判定。
- 任意时刻每个（窗口， 分组）的最新更新即当前结果；连续运行的最终结果与同一事件序列一次性批处理的结果逐条一致（有测试保证）。

## 确定性与可复现

相同事件序列、相同水印推进与相同配置下输出逐条一致：

- 窗口按纪元对齐，与引擎启动时刻无关；
- `Publish` 的处理段被串行化，调用顺序即处理顺序；
- `ORDER BY` 并列时，按全部输出列（列名排序后）的类型感知全序做确定性决胜；跨类型按 `类型名:值` 的规范编码排序；NULL 最小；
- 窗口按起始时间升序关闭。

## 多查询与背压

- 多条查询共享同一事件流，各自维护独立状态与进度，互不阻塞。
- 每个订阅有界缓冲（`Config.SubBuffer`，默认 1024）。默认策略下，消费过慢的订阅被以 `ErrBackpressure` 终止（`Subscription.Err()` 可区分），不影响其他订阅与上游；`Config.BlockOnFull = true` 时改为阻塞投递（不丢数据但会拖慢上游）。

## 检查点与恢复

```go
store, _ := checkpoint.NewStore(dir)
e.Snapshot(store)                 // 原子提交：写临时文件 + fsync + rename
infos, lastSeq, _ := e2.Restore(store) // 新引擎恢复全部订阅状态与流位置
sub, _ := e2.Resume(infos[0].SubID)    // 重新挂接输出通道
```

- `Snapshot` 持久化所有订阅的聚合状态、去重表、水印与流位置（`LastSeq`）；编码字节本身确定性（键排序后序列化）。
- 提交是原子的：崩溃不会留下可读的半成品状态，残留的临时文件在 `Load` 时被清理。
- 恢复后不会重复产出已提交结果（水印与已发射缓存随状态恢复），也不会跳过事件——前提是源从 `lastSeq` 之后继续投递，且在 `Resume` 所有订阅后再恢复发布。

## 配置项

| 配置 | 含义 | 默认 |
|---|---|---|
| `Config.AllowedLateness` | 允许的最大迟到时长，决定迟到地平线 | 0 |
| `Config.SubBuffer` | 每订阅输出缓冲容量 | 1024 |
| `Config.BlockOnFull` | 缓冲满时阻塞投递（true）还是以 `ErrBackpressure` 终止该订阅（false） | false |

## 测试

```sh
go test ./...        # 全部单测（解析、窗口、聚合、乱序/迟到/重复、触发、恢复、并发、内存有界）
go test -race ./...  # 竞态检测
```

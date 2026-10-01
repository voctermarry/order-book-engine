## 用途

本项目是「限价订单簿撮合与执行分析平台」的代码仓库，用于逐步实现该方向的撮合、执行与风险分析能力。

当前实现了单标的订单事件回放：从标准输入读取 UTF-8 JSON Lines，按价格时间优先撮合，并逐行输出确定性结果。

## 环境与安装

- Python 3.11 及以上

```bash
python -m pip install -e .
```

## 测试

```bash
python -m pytest
```

## 命令行入口

安装后提供 `order-book-engine` 命令：

```bash
order-book-engine version    # 打印版本号
order-book-engine --help     # 打印用法
order-book-engine replay     # 从标准输入回放单证券 JSON Lines 订单事件
order-book-engine events     # 从标准输入读取一个多证券有序事件 JSON 文档
```

## replay 输入输出约定

- 从标准输入读取 UTF-8 JSON Lines，逐行向标准输出一个 JSON 对象，不读写任何文件。
- 空行忽略。相同输入的输出逐字节一致。
- 正常完成退出码为 0；标准输入读取或标准输出写入失败时退出码为 1，标准错误仅输出单行 `ERROR_IO`。

### 事件

ADD：

```json
{"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY", "order_type": "LIMIT", "quantity": 5, "price": 100, "time_in_force": "GTC"}
```

- `event_id`：全流唯一字符串。
- `order_id`：唯一字符串。
- `side`：`BUY` 或 `SELL`。
- `order_type`：`LIMIT`、`MARKET` 或 `ICEBERG`。
- `quantity`：正整数。
- `price`：LIMIT/ICEBERG 必填正整数；MARKET 不得带非空 `price`（可省略或为 `null`）。
- `display_quantity`：仅 ICEBERG 必填，正整数且不得超过 `quantity`，表示单次最大公开量；LIMIT/MARKET 带该字段按 `INVALID_SCHEMA` 拒绝。
- `account_id`：可选，只能是非空字符串，适用于 LIMIT、MARKET、ICEBERG 各类新增委托；为空、非字符串（`null`、数字、布尔等）按 `INVALID_SCHEMA` 拒绝。用于自成交防护，见下文。
- `time_in_force`：可选，取值 `GTC`、`IOC`、`FOK`。
  - LIMIT 省略时按 `GTC` 处理（余量入簿）；`GTC`、`IOC`、`FOK` 均要求有效 `price`。
  - ICEBERG 仅接受省略或显式 `GTC`（其余时效、`null` 及非字符串取值按 `INVALID_SCHEMA` 拒绝）；合法订单先以全部余量主动撮合，剩余部分仅将 `min(display_quantity, remaining)` 纳入盘口，返回 `FILLED`、`PARTIALLY_FILLED_RESTING` 或 `RESTING`。
  - MARKET 省略时保持「立即成交、余量取消」语义；可显式指定 `IOC` 或 `FOK`，不得为 `GTC`。
  - `IOC`：仅撮合事件到达时可成交的数量，余量一律取消、不入簿；完全成交为 `FILLED`，部分成交为 `PARTIALLY_FILLED_CANCELLED`，完全未成交为 `UNFILLED_CANCELLED`。
  - `FOK`：先依据事件到达前的可成交盘口判断全部数量能否在限价范围内成交。数量足够时一次性生成全部成交；数量不足时不产生任何成交、不改变盘口、不消耗成交编号，返回 `UNFILLED_CANCELLED`。预检按价格范围内 ICEBERG 的全部余量（含未公开储备）计入可成交量。失败的 FOK 仍是已处理订单，其 `event_id` 与 `order_id` 均被占用。
  - 非字符串或其他取值、MARKET 与 GTC 的组合均以 `INVALID_SCHEMA` 拒绝。

CANCEL：

```json
{"event_id": "e2", "type": "CANCEL", "order_id": "o1"}
```

按 `order_id` 撤销未成交余量；已成交、已撤销或不存在的订单返回 `UNKNOWN_ORDER`。撤销 ICEBERG 时同时移除当前公开片段与全部储备（盘口仅曾汇总公开量）。

REPLACE：

```json
{"event_id": "e3", "type": "REPLACE", "order_id": "o1", "quantity": 3, "price": 101}
```

用原 `order_id` 替换在簿的 GTC 普通限价单或冰山单：

- `event_id`：全流唯一字符串。
- `order_id`：目标订单，必须为 `RESTING`；不存在或已结束返回 `UNKNOWN_ORDER`。
- `quantity`：正整数，表示新剩余总量（不含此前成交量）。
- `price`：正整数。
- `display_quantity`：可选，仅当目标为 ICEBERG；正整数且不得超过新 `quantity`，省略时沿用原峰值。普通限价单携带该字段按 `INVALID_SCHEMA` 拒绝。
- `side` 与订单类型继承目标，事件不得携带 `side`、`order_type`、`time_in_force`、`account_id` 或其他未知字段，否则按 `INVALID_SCHEMA` 拒绝（布尔值不算整数）。
- 新委托继承目标订单的 `account_id`（目标未设置则新委托也不带），并据此参与自成交防护。

替换成功时先移除目标全部余量，再将新委托作为本事件到达的 GTC 委托处理：即使参数未变也失去原队列优先级，不会与旧状态成交，但可作为 taker 撮合其他订单。无成交且余量入簿时 `result` 为 `REPLACED`，部分成交后入簿为 `PARTIALLY_FILLED_RESTING`，全部成交为 `FILLED`。冰山余量仅展示 `min(display_quantity, remaining)`，补片仍排到同价队尾。替换沿用原 `order_id`，不触发 `DUPLICATE_ORDER_ID`，且该 id 不允许后续 ADD 重用；移除、撮合与余量入簿是不可分割的状态变更。目标未知的有效事件占用 `event_id`，结构错误不占用。

EXECUTION_REPORT：

```json
{"event_id": "e4", "type": "EXECUTION_REPORT", "order_id": "o1", "benchmark_price": 100}
```

按 `order_id` 查询某个已接受订单的累计执行情况，是相对 `benchmark_price` 的只读分析，不撮合、不改变订单簿、队列顺序或下一个成交编号：

- `event_id`：全流唯一字符串。
- `order_id`：目标订单，必须为字符串；任何已被接受的订单均可查询（包括在簿、已成交、已撤销的订单），不存在时返回 `UNKNOWN_ORDER` 且占用 `event_id`。
- `benchmark_price`：正整数基准价（布尔值不算整数）。
- 字段缺失、多出或标识非字符串时按 `INVALID_SCHEMA` 拒绝，不占用 `event_id`；重复事件返回 `DUPLICATE_EVENT_ID`。

查询成功时 `result` 为 `REPORTED`，`trades` 为空，`bids`/`asks` 为当前盘口，并在 `result` 之后附加 `execution_analysis`：

- `side`：`BUY` 或 `SELL`。
- `current_status`：`RESTING`、`FILLED` 或 `CANCELLED`。
- `open_quantity`：在簿订单的总余量（ICEBERG 含未公开储备）；其他状态为零。
- `filled_quantity`：累计成交数量。
- `executed_notional`：成交价乘数量的总和。
- `vwap`：无成交时为 `null`，否则为 `{"numerator": executed_notional, "denominator": filled_quantity}`。
- `slippage_notional`：买单为 `executed_notional − benchmark_price × filled_quantity`，卖单取相反数；负值表示相对基准改善。
- `trade_attribution`：按 `trade_id` 升序的成交归因，每项含 `trade_id`、`role`（`MAKER` 或 `TAKER`）、`counterparty_order_id`（对手订单）、`event_id`（产生该成交的事件）、`price`、`quantity`。maker 与 taker 成交均归集到本订单；REPLACE 前后与 ICEBERG 补片的成交都计入原 `order_id`。

ACCOUNT_REPORT：

```json
{"event_id": "e5", "type": "ACCOUNT_REPORT", "account_id": "acct-1", "mark_price": 100}
```

按 `account_id` 查询某个已知账户的累计持仓、资金结果与市值风险，是相对 `mark_price` 的只读分析，不撮合、不改变订单簿、队列顺序或下一个成交编号：

- `event_id`：非空字符串，全流唯一。
- `account_id`：非空字符串。账户只要此前存在一笔已接受且带相同 `account_id` 的 ADD 即视为已知，订单此后的状态（在簿、已成交、已撤销）不影响认定；仅出现在被拒绝事件中的账户不算已知。账户未知时返回 `UNKNOWN_ACCOUNT` 且占用 `event_id`。
- `mark_price`：正整数标记价（布尔值不算整数）。
- 字段缺失、多出或类型不符时按 `INVALID_SCHEMA` 拒绝，不占用 `event_id`；重复事件返回 `DUPLICATE_EVENT_ID`。

查询成功时 `result` 为 `REPORTED`，`trades` 为空，`bids`/`asks` 为当前盘口，并在 `result` 之后附加 `position_analysis`：

- `account_id`、`mark_price`：回显查询参数。
- `buy_quantity`、`sell_quantity`：该账户作为 maker 或 taker 的累计买入、卖出数量；REPLACE 与 ICEBERG 补片继承账户，未带 `account_id` 的订单不计入。
- `net_position`：`buy_quantity − sell_quantity`。
- `buy_notional`、`sell_notional`：买入、卖出成交额（成交价乘数量累加）。
- `buy_vwap`、`sell_vwap`：对应数量为零时为 `null`，否则为 `{"numerator": 成交额, "denominator": 数量}` 的精确分数。
- `turnover_notional`：`buy_notional + sell_notional`。
- `risk_exposure`：`|net_position| × mark_price`。
- `mark_to_market_pnl`：`sell_notional − buy_notional + net_position × mark_price`。

DAY_END_RECONCILIATION：

```json
{"event_id": "e6", "type": "DAY_END_RECONCILIATION", "expected_trades": [
  {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1", "price": 100, "quantity": 3}
], "expected_accounts": [
  {"account_id": "acct-1", "net_position": 3, "cash_balance": -300}
]}
```

将引擎累计结果与外部日终记录做全量对账，是只读查询：不撮合、不改变订单簿、订单状态、队列优先级、成交日志、账户集合或下一个成交编号：

- `event_id`：非空字符串，全流唯一。
- `expected_trades`：外部成交记录数组。每项恰好含 `trade_id`（正整数）、`maker_order_id` 与 `taker_order_id`（非空字符串）、`price` 与 `quantity`（正整数）；`trade_id` 在数组内不得重复。
- `expected_accounts`：外部账户记录数组。每项恰好含 `account_id`（非空字符串）、`net_position`（整数）与 `cash_balance`（整数，卖出成交额减买入成交额）；`account_id` 在数组内不得重复。
- 数组内重复标识、字段缺失或多出、成员类型不符、空标识或布尔值冒充整数，均按 `INVALID_SCHEMA` 拒绝且不占用 `event_id`；合法查询占用 `event_id`，重复事件返回 `DUPLICATE_EVENT_ID`。

引擎侧实际值：实际成交为全部历史成交（按 `trade_id` 对照，比较字段不含内部 `event_id`）；实际账户为已接受且带 `account_id` 的 ADD 所建立的集合，持仓与现金计入该账户作为 maker 或 taker 的全部历史成交（REPLACE 与冰山补片继承账户），`net_position` 为买入数量减卖出数量，`cash_balance` 为卖出成交额减买入成交额。

完全一致时 `result` 为 `RECONCILED`，否则为 `BREAKS_FOUND`；两种结果 `trades` 均为空、`bids`/`asks` 为查询时盘口，并在 `result` 之后附加 `reconciliation`：

- `trade_breaks`、`account_breaks`：差异数组，分别按 `trade_id`、`account_id` 升序排列，无差异时为空数组。
- 每项含 `identifier`、`expected`、`actual` 与 `reason`：仅外部存在时 `reason` 为 `MISSING_ACTUAL`，仅引擎存在时为 `MISSING_EXPECTED`，两侧都存在但字段不同时为 `FIELD_MISMATCH`；缺失一侧为 `null`，存在一侧保留完整对象。

### 撮合规则

- 买单匹配最低卖价，卖单匹配最高买价；同价位先到者优先（价格时间优先）。
- 限价单不得越过自身价格；成交价取被动单（maker）价格。
- GTC 限价单余量入簿；IOC 余量取消、绝不入簿；FOK 要么在事件前盘口上全部成交，要么完全不成交；市价单余量取消；撤单不产生成交。
- ICEBERG 被动成交只消耗当前公开片段；片段耗尽但仍有储备时，立即公开下一片 `min(display_quantity, remaining)`，并排到同价已有可见订单之后，因此同一主动单可在其他同价单之后再次遇到它。各片沿用同一 `maker_order_id`；`bids`/`asks` 只汇总当前公开片段。市价单、IOC 与普通限价单均可消耗补片；FOK 失败时不补片、不改变盘口。
- 成交编号从 1 开始连续递增（失败的 FOK 不消耗编号）。

### 自成交防护（account_id）

- 仅当主动单与被动单**都携带** `account_id` 且二者完全相同（大小写敏感的字符串相等）时才触发防护；任一方未携带时按既有规则正常撮合。
- 撮合仍严格按价格时间优先查找对手盘。主动单在价格时间顺序上命中的**首笔**同账户被动单即为触发点：不生成该笔成交，不改变该被动单的队列位置、剩余量或冰山当前公开片段，立即取消主动单的全部余量，且**不得越过**该被动单继续寻找后续流动性。
- 触发前已经完成的外部成交保留并占用各自的 `trade_id`：此前无成交时 `result` 为 `SELF_TRADE_PREVENTED`，此前有成交时为 `PARTIALLY_FILLED_SELF_TRADE_PREVENTED`（对 GTC 余量同样直接取消、不入簿）。两种结果都在输出中附加 `self_trade_prevention` 对象；其他任何结果都不得包含该对象。
- 被防护取消的主动单仍占用其 `event_id` 与 `order_id`：之后不能撤销、替换，也不能通过 ADD 重用该 `order_id`。
- FOK 预检按与真实撮合一致的价格时间顺序模拟（包含冰山补片排到同价队尾的次序，储备量仍可计入）：在凑足全部数量之前遇到同账户被动单时，原子返回 `SELF_TRADE_PREVENTED`，`cancelled_quantity` 为原始委托量，不成交、不改变盘口、不消耗 `trade_id`；若在同账户单之前即可全部成交则仍为 `FILLED`；只有可成交流动性确实不足时才返回 `UNFILLED_CANCELLED`（限价范围之外的同账户单不触发防护）。
- REPLACE 先移除旧委托，再让继承目标账户的新委托作为主动单应用上述规则；触发防护后不恢复已移除的旧委托。
- `account_id` 出现在非 ADD（CANCEL/REPLACE）事件中，或在 ADD 中为空、非字符串时，返回 `INVALID_SCHEMA`；结构拒绝不占用 `event_id`，引擎状态不变。

### 每个事件的输出

```json
{"input_line": "…", "event_id": "e1", "result": "FILLED", "trades": [
  {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1", "price": 100, "quantity": 5}
], "bids": [], "asks": []}
```

- `input_line`：去除行终止符后的原始输入文本。
- `event_id`：可取得时为字符串，否则为 `null`。
- `result`：
  - GTC 限价单/ICEBERG：`FILLED`、`RESTING`、`PARTIALLY_FILLED_RESTING`、`SELF_TRADE_PREVENTED`、`PARTIALLY_FILLED_SELF_TRADE_PREVENTED`
  - IOC 限价单/市价单：`FILLED`、`PARTIALLY_FILLED_CANCELLED`、`UNFILLED_CANCELLED`、`SELF_TRADE_PREVENTED`、`PARTIALLY_FILLED_SELF_TRADE_PREVENTED`
  - FOK 限价单/市价单：`FILLED`、`UNFILLED_CANCELLED`、`SELF_TRADE_PREVENTED`
  - 市价单：与 IOC 相同
  - 撤单成功：`CANCELLED`
  - 替换成功：`REPLACED`、`PARTIALLY_FILLED_RESTING`、`FILLED`、`SELF_TRADE_PREVENTED`、`PARTIALLY_FILLED_SELF_TRADE_PREVENTED`
  - 执行查询成功：`REPORTED`（附加 `execution_analysis`）
  - 账户查询成功：`REPORTED`（附加 `position_analysis`）
  - 日终对账查询成功：`RECONCILED` 或 `BREAKS_FOUND`（附加 `reconciliation`）
  - 拒绝：`REJECTED`（附加 `reason`）
- `self_trade_prevention`：仅在两种自成交防护结果下出现，序列化于 `result`/`reason` 之后、`trades` 之前，含 `maker_order_id`（触发的被动单）、`taker_order_id`（被取消的主动单）与 `cancelled_quantity`（取消量，等于触发时主动单的剩余量；FOK 预检触发时为原始委托量）。其他结果不得包含该字段。
- `execution_analysis`：仅在 EXECUTION_REPORT 的 `REPORTED` 结果下出现，序列化于 `result` 之后、`trades` 之前，字段见上文 EXECUTION_REPORT 一节。其他结果不得包含该字段。
- `position_analysis`：仅在 ACCOUNT_REPORT 的 `REPORTED` 结果下出现，序列化于 `result` 之后、`trades` 之前，字段见上文 ACCOUNT_REPORT 一节。其他结果不得包含该字段。
- `reconciliation`：仅在 DAY_END_RECONCILIATION 的 `RECONCILED`/`BREAKS_FOUND` 结果下出现，序列化于 `result` 之后、`trades` 之前，字段见上文 DAY_END_RECONCILIATION 一节。其他结果不得包含该字段。
- `trades`：按发生顺序排列；每笔含 `maker_order_id`、`taker_order_id`、`price`、`quantity` 与 `trade_id`。
- `bids` 按价格降序、`asks` 按价格升序，每档含整数 `price` 与汇总 `quantity`。

拒绝原因：`INVALID_JSON`、`INVALID_SCHEMA`（非对象、缺字段、字段类型或枚举错误、未知字段）、`DUPLICATE_EVENT_ID`、`DUPLICATE_ORDER_ID`、`UNKNOWN_ORDER`、`UNKNOWN_ACCOUNT`。拒绝对象带空 `trades` 和拒绝前盘口，不改变订单簿、成交编号或后续优先级。

## 现有公开接口

- 命令行程序 `order-book-engine`（`version`、`replay`、`events`）
- Python 包 `order_book_engine`，其 `__version__` 为当前版本号
- 多证券有序事件回放与快照（`replay_events`、`EventReplayer`、`export_snapshot`、`restore_replayer`、`canonical_json`、`SnapshotError`），见下文。

## 多证券有序事件回放

在不改变基线撮合规则、优先级、拒绝语义与成交记录的前提下，新增一个确定性的事件回放入口：调用方一次提交一个或多个证券的有序订单事件流，得到逐事件结果、最终盘口、成交明细和可继续回放的内存快照。事件仅覆盖基线已支持的 `ADD`、`CANCEL`、`REPLACE`（不重新定义任何订单类型的撮合规则；`EXECUTION_REPORT`/`ACCOUNT_REPORT`/`DAY_END_RECONCILIATION` 仍只属于基线 JSON Lines 入口）。

### 命令行

```bash
order-book-engine events < request.json > response.json
```

标准输入读取**一个** UTF-8 JSON 请求文档，标准输出写出**一个** UTF-8 JSON 响应文档；程序不读写任何文件，快照只通过文档收发。请求非法时退出码为 2 并输出 `{"error":{"code":...,"message":...}}`；IO 失败时退出码为 1 且标准错误输出单行 `ERROR_IO`。

### Python 入口

```python
from order_book_engine import replay_events
response = replay_events(events, config=None, snapshot=None, snapshot_after="last")
# {"results": [...], "snapshot": {...} | None}
```

### 请求事件

每个事件是一个 JSON 对象，信封字段：

- `event_id`：非空字符串，**全流唯一**（跨所有证券）。
- `symbol`：非空字符串，证券代码；不同证券分别维护序列与簿状态。
- `sequence`：正整数（布尔值不算整数）；同一证券严格按 `sequence` 递增处理，期望序列为该证券上一成功事件序列加 1。输入顺序即处理顺序，`timestamp`（可选，非负整数或非空字符串）相同或乱序都不得改变输入顺序。
- 基线订单字段可**内联**携带（`type` + 基线 ADD/CANCEL/REPLACE 字段），也可放在嵌套的 `event` 对象中（该对象必须重复相同的 `event_id` 与 `type`）。内联形式只允许信封字段与基线字段；嵌套形式只允许信封字段加 `event`，任何未知字段按 `INVALID_EVENT` 拒绝。

### 逐事件结果

每个结果都关联 `event_id`、`symbol`、`sequence`，并含：

- `status`：`ACCEPTED`（已接受）、`REJECTED`（业务拒绝）或 `DUPLICATE`（幂等重复）。
- `result`：仅 `ACCEPTED` 时出现，沿用基线结果码（`RESTING`、`FILLED`、`REPLACED`、`CANCELLED` 等）。
- `rejection_code`：仅 `REJECTED` 时出现，见下文错误码。
- `expected_sequence`：序列类拒绝时给出该证券当前期望序列。
- `trades`：该事件产生的成交，按发生顺序排列，字段与基线完全一致（`trade_id` 在**各证券内**从 1 连续递增）。
- `book_changes`：本次盘口变更，`bids` 降序、`asks` 升序；仅列出数量发生变化的档位，被移除的档位以 `"quantity": 0` 表示。
- `bids`/`asks`：该事件处理后的该证券完整盘口（已知证券的拒绝事件回显未变化盘口；未知证券的预分发拒绝为空盘口且不创建证券）。

### 提交语义与错误码

- 逐事件提交：先前成功事件不会因后续失败回滚；失败事件不留下订单、成交、计数器或盘口变更。
- `INVALID_EVENT`：信封/必填字段缺失、非法数值、未知事件类型、载荷与信封不一致等（对应基线结构错误，不占用 `event_id` 与序列）。
- `SEQUENCE_GAP`：当前证券序列出现空洞（大于期望值）；不占用该序列与 `event_id`，修正后的事件可立即提交。
- `OUT_OF_ORDER`：序列倒退（小于期望值）。
- `EVENT_ID_CONFLICT`：已见 `eventId` 但规范化内容不一致（或用于另一证券）；不再次撮合、不占用序列。
- `DUPLICATE`：已见 `eventId` 且规范化内容（键排序后的紧凑 JSON）完全一致；不再次撮合、无成交无盘口变更。重试投递携带旧序列时仍识别为重复。
- 撤单/改单不存在或已终结订单，继续沿用基线拒绝码（如 `UNKNOWN_ORDER`、`DUPLICATE_ORDER_ID`）；这类有效事件与基线一样占用其 `event_id` 并推进该证券序列。
- 相同初始状态、配置和事件流产生字段顺序稳定、数值表示一致、可逐字节比较的 JSON（规范化序列化：键排序、紧凑分隔、整数不丢精度、无浮点）。

### 快照与恢复

`replay_events` 默认在响应中附带处理完最后一个事件后的 `snapshot`；`snapshot_after=None` 可省略，`snapshot_after={"symbol": ..., "sequence": ...}` 可在指定的**已接受**事件之后导出。有状态的会话也可用 `EventReplayer`（`submit`/`book`）配合 `export_snapshot`/`restore_replayer` 增量处理。

快照是 JSON 对象：

- `format_version`：格式版本（当前 `event-replay/1`）。
- `engine_version`、`config`（撮合配置摘要）与 `config_digest`（配置的 SHA-256）。
- `content`：各证券完整状态——价格时间队列顺序（含每档订单 id 队列）、订单剩余量、冰山当前公开量 `visible` 与补量所需 `display_quantity`、各证券最后序列 `last_sequence`、已接受事件日志、累计成交 `trade_log`、生成后续成交标识所需的 `next_trade_id`、账户集合。
- `content_digest`：基于规范化内容（连同版本与配置）计算的 SHA-256。

恢复（`restore_replayer(snapshot, config=None)` 或在 `replay_events` 中传 `snapshot=`）先验证版本、配置与摘要，再做结构与内部一致性交叉校验，全部通过后才采纳状态：

- 摘要不符：`SNAPSHOT_CORRUPT`。
- 版本不支持：`SNAPSHOT_VERSION_UNSUPPORTED`。
- 配置或配置摘要不一致：`CONFIG_MISMATCH`。
- 任何恢复失败都抛出 `SnapshotError`（其 `code` 为上述代码）且不创建部分恢复状态；CLI 将其映射为退出码 2 的错误文档。
- 恢复成功后继续生成的逐笔成交标识、成交顺序和最终结果，与不中断的一次性回放完全一致（逐字节比较）。


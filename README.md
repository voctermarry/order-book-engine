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

IMPACT_REPORT：

```json
{"event_id": "e7", "type": "IMPACT_REPORT", "side": "BUY", "quantity": 10, "benchmark_price": 100}
```

按盘口估算一笔**匿名市价单**的可执行量、均价、滑点与盘口冲击，是只读的假设分析：严格按价格时间优先模拟成交，成交价取 maker 价格；不撮合、不改变盘口、队列顺序、订单与冰山状态、成交日志、账户集合或下一个成交编号，也不产生任何成交。

- `event_id`：全流唯一字符串。
- `side`：`BUY` 或 `SELL`，分别模拟吃卖盘、买盘。
- `quantity`：正整数（布尔值不算整数）。
- `benchmark_price`：正整数基准价（布尔值不算整数）。
- 字段缺失、多出或类型错误（含非字符串 `event_id`、非法 `side`、布尔/零/负/浮点/字符串/空值的 `quantity` 或 `benchmark_price`）按 `INVALID_SCHEMA` 拒绝，不占用 `event_id`；合法查询占用 `event_id`，重复事件返回 `DUPLICATE_EVENT_ID`。

模拟规则与真实市价单一致：买单从最低卖价、卖单从最高买价起逐档成交；冰山被动单先消耗当前公开片段，片段耗尽但仍有储备时按既有规则补一片 `min(display_quantity, remaining)` 并移至同价队尾，同一模拟单可在同价其他订单之后再次遇到它。模拟单匿名且无账户，自成交防护不适用；直至请求量满足或可成交流动性全部耗尽。

查询成功时 `result` 为 `REPORTED`，`trades` 为空，`bids`/`asks` 为未变化的查询时盘口，并在 `result` 之后附加 `impact_analysis`：

- `side`、`requested_quantity`、`benchmark_price`：回显查询参数。
- `executable_quantity`：可执行（成交）数量；`unfilled_quantity`：`requested_quantity − executable_quantity`。
- `executed_notional`：各档成交价乘数量之和。
- `best_price`：查询前最佳对手价（买单为最低卖价、卖单为最高买价）；无流动性时为 `null`。
- `vwap`：无可成交时为 `null`，否则为 `{"numerator": executed_notional, "denominator": executable_quantity}` 的精确分数。
- `slippage_notional`：买单为 `executed_notional − benchmark_price × executable_quantity`，卖单取相反数；负值表示相对基准改善。
- `impact_notional`：以**查询前最佳对手价**替代基准价按同一公式计算的盘口冲击成本；卖单同样取相反数。
- `price_breakdown`：按模拟成交顺序排列的明细，每次消耗一个被动可见片段产生一项（粒度与真实成交一致），每项含 `price` 与 `quantity`；同一价位的不同 maker 分别成项，同一冰山的各次补片也按其再次排到队尾后的实际成交次序分别成项。
- 无流动性时 `executable_quantity` 与 `executed_notional` 为 0、`unfilled_quantity` 等于请求量、`best_price` 与 `vwap` 为 `null`、两项成本均为 0、`price_breakdown` 为空数组；部分成交时两项成本只按实际可执行量计算。

`ACCOUNT_REPORT`/`DAY_END_RECONCILIATION` 只属于基线 JSON Lines（`replay`）入口；`events` 多证券事件流不接受这两种类型（按 `INVALID_EVENT` 拒绝且不占用 `event_id` 与序列），快照格式也不因其改变。`EXECUTION_REPORT` 与 `IMPACT_REPORT` 则两个入口都接受：多证券事件流中的语义分别见下文「单订单执行报告 EXECUTION_REPORT」与「盘口冲击估算 IMPACT_REPORT」两节。与之对称，跨证券的 `PORTFOLIO_REPORT`、按多个价格情景重估同一账户既有成交的 `PORTFOLIO_STRESS_REPORT`、一次核对全部证券的 `SESSION_RECONCILIATION`、按评估价补齐母单实施缺口的 `PLAN_TCA_REPORT` 与按已提交序列重建历史盘口队列的 `BOOK_RECONSTRUCTION_REPORT` 只属于多证券事件流（`events`/`replay_events`/`EventReplayer`），基线单证券 JSON Lines 入口不接受这些类型（按基线 `INVALID_SCHEMA` 拒绝）。

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
  - 盘口冲击估算成功：`REPORTED`（附加 `impact_analysis`）
  - 拒绝：`REJECTED`（附加 `reason`）
- `self_trade_prevention`：仅在两种自成交防护结果下出现，序列化于 `result`/`reason` 之后、`trades` 之前，含 `maker_order_id`（触发的被动单）、`taker_order_id`（被取消的主动单）与 `cancelled_quantity`（取消量，等于触发时主动单的剩余量；FOK 预检触发时为原始委托量）。其他结果不得包含该字段。
- `execution_analysis`：仅在 EXECUTION_REPORT 的 `REPORTED` 结果下出现，序列化于 `result` 之后、`trades` 之前，字段见上文 EXECUTION_REPORT 一节。其他结果不得包含该字段。
- `position_analysis`：仅在 ACCOUNT_REPORT 的 `REPORTED` 结果下出现，序列化于 `result` 之后、`trades` 之前，字段见上文 ACCOUNT_REPORT 一节。其他结果不得包含该字段。
- `reconciliation`：仅在 DAY_END_RECONCILIATION 的 `RECONCILED`/`BREAKS_FOUND` 结果下出现，序列化于 `result` 之后、`trades` 之前，字段见上文 DAY_END_RECONCILIATION 一节。其他结果不得包含该字段。
- `impact_analysis`：仅在 IMPACT_REPORT 的 `REPORTED` 结果下出现，序列化于 `result`/`reason` 之后、`trades` 之前，字段见上文 IMPACT_REPORT 一节。其他结果不得包含该字段。
- `trades`：按发生顺序排列；每笔含 `maker_order_id`、`taker_order_id`、`price`、`quantity` 与 `trade_id`。
- `bids` 按价格降序、`asks` 按价格升序，每档含整数 `price` 与汇总 `quantity`。

拒绝原因：`INVALID_JSON`、`INVALID_SCHEMA`（非对象、缺字段、字段类型或枚举错误、未知字段）、`DUPLICATE_EVENT_ID`、`DUPLICATE_ORDER_ID`、`UNKNOWN_ORDER`、`UNKNOWN_ACCOUNT`。拒绝对象带空 `trades` 和拒绝前盘口，不改变订单簿、成交编号或后续优先级。

## 现有公开接口

- 命令行程序 `order-book-engine`（`version`、`replay`、`events`）
- Python 包 `order_book_engine`，其 `__version__` 为当前版本号
- 多证券有序事件回放与快照（`replay_events`、`EventReplayer`、`export_snapshot`、`restore_replayer`、`canonical_json`、`SnapshotError`），见下文。

## 多证券有序事件回放

在不改变基线撮合规则、优先级、拒绝语义与成交记录的前提下，新增一个确定性的事件回放入口：调用方一次提交一个或多个证券的有序订单事件流，得到逐事件结果、最终盘口、成交明细和可继续回放的内存快照。事件覆盖基线已支持的 `ADD`、`CANCEL`、`REPLACE`，可恢复的 TWAP 母单命令 `TWAP_START`、`TWAP_SLICE`、`TWAP_CANCEL`、`TWAP_REPORT`，VWAP 母单命令 `VWAP_START`、`VWAP_SLICE`、`VWAP_CANCEL`、`VWAP_REPORT`，POV 母单命令 `POV_START`、`POV_VOLUME`、`POV_CANCEL`、`POV_REPORT`，单订单只读查询 `EXECUTION_REPORT`，按某证券当前盘口估算匿名市价单可执行量与成本的只读查询 `IMPACT_REPORT`，跨证券只读查询 `PORTFOLIO_REPORT`，用账户既有成交评估多个价格情景的跨证券只读查询 `PORTFOLIO_STRESS_REPORT`，一次核对全部证券成交与账户账簿的只读查询 `SESSION_RECONCILIATION`，按指定评估价补齐母单实施缺口分析的只读查询 `PLAN_TCA_REPORT`，按已提交序列重建该证券历史盘口队列的只读查询 `BOOK_RECONSTRUCTION_REPORT`，以及盘中涨跌停调整 `PRICE_LIMIT_UPDATE`（不重新定义任何订单类型的撮合规则；`ACCOUNT_REPORT`/`DAY_END_RECONCILIATION` 仍只属于基线 JSON Lines 入口）。计划不读取墙钟：TWAP/VWAP 只由各自的 SLICE 事件推进，POV 只由 `POV_VOLUME` 事件喂入的市场成交量推进。

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
- 基线订单字段或 TWAP/VWAP/POV 命令字段可**内联**携带（`type` + 对应字段），也可放在嵌套的 `event` 对象中（该对象必须重复相同的 `event_id` 与 `type`）。内联形式只允许信封字段与该 `type` 自身的字段；嵌套形式只允许信封字段加 `event`，任何未知字段按 `INVALID_EVENT` 拒绝。

### 逐事件结果

每个结果都关联 `event_id`、`symbol`、`sequence`，并含：

- `status`：`ACCEPTED`（已接受）、`REJECTED`（业务拒绝）或 `DUPLICATE`（幂等重复）。
- `result`：仅 `ACCEPTED` 时出现，沿用基线结果码（`RESTING`、`FILLED`、`REPLACED`、`CANCELLED` 等）。
- `rejection_code`：仅 `REJECTED` 时出现，见下文错误码。
- `expected_sequence`：序列类拒绝时给出该证券当前期望序列。
- `trades`：该事件产生的成交，按发生顺序排列，字段与基线完全一致（`trade_id` 在**各证券内**从 1 连续递增）。
- `book_changes`：本次盘口变更，`bids` 降序、`asks` 升序；仅列出数量发生变化的档位，被移除的档位以 `"quantity": 0` 表示。
- `bids`/`asks`：该事件处理后的该证券完整盘口（已知证券的拒绝事件回显未变化盘口；未知证券的预分发拒绝为空盘口且不创建证券）。
- `execution_plan`：仅 TWAP/VWAP/POV 命令的结果出现，字段见下文 TWAP 母单、VWAP 母单与 POV 母单三节。
- `execution_analysis`：仅 `EXECUTION_REPORT` 的成功结果出现，字段见下文「单订单执行报告 EXECUTION_REPORT」一节；其他结果不得包含该字段。
- `impact_analysis`：仅 `IMPACT_REPORT` 的成功结果出现，字段与基线单证券入口完全一致，见下文「盘口冲击估算 IMPACT_REPORT」一节；其他结果不得包含该字段。
- `portfolio_analysis`：仅 `PORTFOLIO_REPORT` 的成功结果出现，字段见下文「跨证券组合报告 PORTFOLIO_REPORT」一节；其他结果不得包含该字段。
- `portfolio_stress_analysis`：仅 `PORTFOLIO_STRESS_REPORT` 的成功结果出现，字段见下文「多情景组合压力报告 PORTFOLIO_STRESS_REPORT」一节；其他结果不得包含该字段。
- `reconciliation`：仅 `SESSION_RECONCILIATION` 的成功结果出现（`result` 为 `RECONCILED` 或 `BREAKS_FOUND` 时都附带），字段见下文「全会话对账 SESSION_RECONCILIATION」一节；其他结果不得包含该字段。
- `plan_tca_analysis`：仅 `PLAN_TCA_REPORT` 的成功结果出现，字段见下文「母单实施缺口报告 PLAN_TCA_REPORT」一节；其他结果不得包含该字段。
- `book_reconstruction`：仅 `BOOK_RECONSTRUCTION_REPORT` 的成功结果出现，字段见下文「历史盘口重建报告 BOOK_RECONSTRUCTION_REPORT」一节；其他结果不得包含该字段。
- `active_price_limits`：仅 `PRICE_LIMIT_UPDATE` 的成功结果出现（`{"lower_price": ..., "upper_price": ...}`），回显替换后的活动区间；其他结果不得包含该字段。

### TWAP 母单

TWAP 母单是属于**单个证券**的可恢复计划，沿用信封的 `event_id`、`symbol`、`sequence` 顺序与幂等语义，不读取墙钟，只由 `TWAP_SLICE` 事件释放子单。

TWAP_START：

```json
{"event_id": "e2", "symbol": "AAA", "sequence": 2, "type": "TWAP_START",
 "plan_id": "p1", "side": "BUY", "total_quantity": 5, "slice_count": 2,
 "order_type": "LIMIT", "benchmark_price": 99, "price": 100, "account_id": "acct-1"}
```

- `plan_id`：非空字符串，在该证券内唯一。
- `side`：`BUY` 或 `SELL`。
- `total_quantity`、`slice_count`、`benchmark_price`：均为正整数（布尔值不算整数），且 `total_quantity` 不得小于 `slice_count`。
- `order_type`：`LIMIT` 或 `MARKET`（不含 ICEBERG）。LIMIT 必须带正整数 `price`；MARKET 不得带非空 `price`（可省略或为 `null`）。
- `account_id`：可选非空字符串，子单继承该账户用于自成交防护。
- 字段缺失、多出、类型错误、空标识、LIMIT/MARKET 价格规则违反、总量小于片数等均按 `INVALID_EVENT` 拒绝，不占用 `event_id` 与序列。
- 启动**不撮合**。总量整除分片：每片数量为 `total_quantity // slice_count`，余数从最早片起每片加一单位（如 5 量 2 片为 3、2）。
- 重复 `plan_id` 返回 `DUPLICATE_EXECUTION_PLAN`；派生标识（见下）与既有订单或其他计划冲突时返回 `DUPLICATE_ORDER_ID`。二者都占用 `event_id` 并推进序列，但不建立计划、不保留任何标识。

TWAP_SLICE：

```json
{"event_id": "e3", "symbol": "AAA", "sequence": 3, "type": "TWAP_SLICE", "plan_id": "p1"}
```

- 按序释放下一片：子单标识固定为 `plan_id#片号`（片号从 1 开始），按释放时盘口提交同方向子单——LIMIT 计划提交带计划价格的 `IOC` 限价单，MARKET 计划提交市价子单；沿用现有撮合、冰山补片与自成交防护规则。
- 响应保留子单的 `result`（如 `FILLED`、`PARTIALLY_FILLED_CANCELLED`、`UNFILLED_CANCELLED`、`SELF_TRADE_PREVENTED`）、`trades`、`book_changes` 与盘口，并附加 `execution_plan`（含本片编号 `slice_number` 与 `child_order_id`）。
- 未知计划返回 `UNKNOWN_EXECUTION_PLAN`；计划已关闭（`COMPLETED`/`CANCELLED`）再切片返回 `EXECUTION_PLAN_CLOSED`，不撮合、不改变盘口或成交编号。

TWAP_CANCEL 与 TWAP_REPORT 均只携带 `event_id`、`type`、`plan_id`：

- `TWAP_CANCEL`：将**未释放量**计入取消量并把计划置为 `CANCELLED`；不撤销已释放子单（IOC 子单在释放事件结束时即已终结），不改变盘口、成交编号或历史成交。已关闭计划返回 `EXECUTION_PLAN_CLOSED`。
- `TWAP_REPORT`：只读返回相同的累计汇总，不改变任何状态；关闭后的计划仍可查询。未知计划返回 `UNKNOWN_EXECUTION_PLAN`。
- 全部片释放后计划为 `COMPLETED`；关闭后的计划仍保留可查。

每个 TWAP 命令响应中的 `execution_plan` 含：

- `status`：`ACTIVE`、`COMPLETED` 或 `CANCELLED`。
- `released_quantity`：已释放片的原始数量之和；`filled_quantity`：子单累计成交量；`cancelled_quantity`：取消时计入的未释放量。
- `remaining_slices`：剩余片数（计划关闭后为 0）。
- `executed_notional`：累计成交额（成交价乘数量）。
- `vwap`：精确分数 `{"numerator": executed_notional, "denominator": filled_quantity}`；无成交时为 `null`。
- `slippage_notional`：买单为 `executed_notional − benchmark_price × filled_quantity`，卖单取相反数；负值表示相对基准改善。
- 仅 `TWAP_SLICE` 成功结果额外含 `slice_number`（本片编号）与 `child_order_id`（本片子单标识）。

标识保留与快照：

- `TWAP_START` 接受后即保留全部派生标识 `plan_id#1 … plan_id#N`；外部 `ADD` 不得占用这些标识为 `order_id`，外部事件也不得将其用作 `event_id`（分别按基线 `DUPLICATE_ORDER_ID`、`DUPLICATE_EVENT_ID` 拒绝）。计划之间派生标识冲突在启动时按 `DUPLICATE_ORDER_ID` 拒绝。
- 业务拒绝（未知/已关闭/重复计划、派生标识冲突）都占用该 `event_id` 并推进该证券序列，但不改变计划或盘口状态；结构非法（`INVALID_EVENT`）不占用。
- 计划进度、保留标识与累计分析（成交量、成交额、取消量、片量列表）全部进入快照；恢复后继续执行与不中断回放逐字节一致。

### VWAP 母单

VWAP 母单与 TWAP 母单同属**单个证券**的可恢复计划，共用同一 `plan_id` 命名空间、派生标识方案（`plan_id#1 … plan_id#N` 启动即保留）、生命周期（`ACTIVE`/`COMPLETED`/`CANCELLED`）、拒绝码与幂等/序列语义；差别仅在分片方式：VWAP 按调用方给出的成交量曲线分片，同样不读取墙钟，只由 `VWAP_SLICE` 事件释放子单。

VWAP_START：

```json
{"event_id": "e2", "symbol": "AAA", "sequence": 2, "type": "VWAP_START",
 "plan_id": "p1", "side": "BUY", "total_quantity": 10, "volume_weights": [1, 2, 3],
 "order_type": "LIMIT", "benchmark_price": 99, "price": 100, "account_id": "acct-1"}
```

- `plan_id`、`side`、`order_type`、`benchmark_price`、`price`、`account_id` 的规则与 TWAP_START 完全一致。
- `total_quantity`：正整数（布尔值不算整数），且不得小于权重个数（每桶先分一单位）。
- `volume_weights`：非空的正整数数组，每个元素是一个桶的目标权重；桶数即片数。
- 字段缺失、多出、类型错误、空标识、空权重数组、非正权重、LIMIT/MARKET 价格规则违反、总量小于桶数等均按 `INVALID_EVENT` 拒绝，不占用 `event_id` 与序列。
- 启动**不撮合**。分片算法：每桶先分一单位；余量按权重比例取整数商分配；取整剩下的单位按除法余数**降序**每桶补一单位，余数相同则较早桶优先。各片数量之和恒等于 `total_quantity`（如总量 10、权重 `[1, 2, 3]` 分为 2、3、5）。
- 重复 `plan_id`（无论已有计划是 TWAP 还是 VWAP）返回 `DUPLICATE_EXECUTION_PLAN`；派生标识与既有订单或其他计划的保留标识冲突时返回 `DUPLICATE_ORDER_ID`。二者都占用 `event_id` 并推进序列，但不建立计划、不保留任何标识。

VWAP_SLICE / VWAP_CANCEL / VWAP_REPORT 只携带 `event_id`、`type`、`plan_id`，语义与对应 TWAP 命令一致：

- `VWAP_SLICE` 按序释放下一桶：LIMIT 计划生成同价 `IOC` 子单，MARKET 计划生成市价子单，沿用现有撮合、冰山补片与自成交防护规则。未知计划返回 `UNKNOWN_EXECUTION_PLAN`；已关闭计划返回 `EXECUTION_PLAN_CLOSED`。
- `VWAP_CANCEL` 将未释放片量计入 `cancelled_quantity` 并把计划置为 `CANCELLED`，不改变盘口、成交编号或历史成交。
- `VWAP_REPORT` 只读返回同一累计汇总；关闭后的计划仍可查询；全部片释放后计划为 `COMPLETED`。

VWAP 命令响应中的 `execution_plan` 沿用 TWAP 的全部累计指标（`status`、`released_quantity`、`filled_quantity`、`cancelled_quantity`、`remaining_slices`、`executed_notional`、`vwap`、`slippage_notional`），并额外给出 `algorithm: "VWAP"`；`VWAP_SLICE` 成功结果再额外含 `slice_number`、`child_order_id`、`target_weight`（本桶权重）与 `scheduled_quantity`（本桶计划片量）。

计划完整状态（算法、权重曲线、各片数量、进度与累计分析）进入快照；快照格式版本保持 `event-replay/2`，恢复后继续执行与不中断回放逐字节一致。

### POV 母单

POV（参与率）母单与 TWAP/VWAP 母单同属**单个证券**的可恢复计划，共用同一 `plan_id` 命名空间、派生标识方案（`plan_id#N`）、生命周期（`ACTIVE`/`COMPLETED`/`CANCELLED`）、拒绝码与幂等/序列语义；差别在于推进方式：POV 没有固定分片表，而是按调用方逐次喂入的市场累计成交量与参与率，动态计算每次应释放的数量，同样不读取墙钟，只由 `POV_VOLUME` 事件推进。

POV_START：

```json
{"event_id": "e2", "symbol": "AAA", "sequence": 2, "type": "POV_START",
 "plan_id": "p1", "side": "BUY", "total_quantity": 100, "participation_bps": 2000,
 "order_type": "LIMIT", "benchmark_price": 99, "price": 100, "account_id": "acct-1"}
```

- `plan_id`：非空字符串，在该证券内唯一（TWAP/VWAP/POV 共用命名空间）。
- `side`：`BUY` 或 `SELL`。
- `total_quantity`：正整数（布尔值不算整数），为参与率公式收敛的目标总量。
- `participation_bps`：参与率，单位基点，**1 至 10000 的整数**（含边界；布尔、零、负数、10001、浮点、字符串均非法）。
- `order_type`：`LIMIT` 或 `MARKET`（不含 ICEBERG）。LIMIT 必须带正整数 `price`；MARKET 不得带非空 `price`（可省略或为 `null`）。
- `benchmark_price`：正整数，用于滑点核算。
- `account_id`：可选非空字符串，子单继承该账户用于自成交防护。
- 字段缺失、多出、类型错误、空标识、参与率越界、LIMIT/MARKET 价格规则违反等均按 `INVALID_EVENT` 拒绝，**不占用** `event_id` 与序列。
- 启动**不撮合**。与 TWAP/VWAP 一样，`POV_START` 接受后即保留全部潜在派生标识 `plan_id#1 … plan_id#total_quantity`（外部 `ADD` 不得占用为 `order_id`，外部事件不得用作 `event_id`）；重复 `plan_id` 返回 `DUPLICATE_EXECUTION_PLAN`，派生标识与既有订单或其他计划冲突返回 `DUPLICATE_ORDER_ID`，二者都占用 `event_id` 并推进序列，但不建立计划、不保留任何标识。

POV_VOLUME：

```json
{"event_id": "e3", "symbol": "AAA", "sequence": 3, "type": "POV_VOLUME",
 "plan_id": "p1", "market_volume_increment": 50}
```

- 载荷只含 `event_id`、`type`、`plan_id`、`market_volume_increment`；增量为正整数（布尔、零、负、浮点、字符串、空值均非法），字段缺失或多出按 `INVALID_EVENT` 拒绝且不占用。
- 增量累加到计划的**累计市场成交量** `M`。目标累计释放量为 `min(total_quantity, floor(M × participation_bps ÷ 10000))`；本次释放量为目标值减去已释放量。
- 本次释放量为**零**时事件仍成功：市场量照常累计、占用 `event_id` 并推进序列，但**不创建子单**、不消耗成交编号，响应无 `result` 字段，且 `execution_plan.child_order_id` 为 `null`、不含 `release_number`。
- 本次释放量**大于零**时按已发生的正数释放次数生成子单标识 `plan_id#N`（N 从 1 开始、只随正数释放递增），提交一笔该数量的子单——LIMIT 计划提交带计划价的 `IOC` 限价单，MARKET 计划提交市价子单；沿用现有撮合、冰山补片与自成交防护规则。
- 响应保留子单的 `result`、`trades`、`book_changes` 与盘口，并附加 `execution_plan`（含 `release_number` 与 `child_order_id`）。
- 未知计划返回 `UNKNOWN_EXECUTION_PLAN`；计划已关闭（`COMPLETED`/`CANCELLED`）再喂量返回 `EXECUTION_PLAN_CLOSED`，不撮合、不累计市场量、不改变盘口或成交编号。
- 释放驱动命令按计划种类区分：`POV_VOLUME` 只能驱动 POV 计划，`TWAP_SLICE`/`VWAP_SLICE` 只能驱动固定分片的 TWAP/VWAP 计划；id 存在但种类不符时返回 `UNKNOWN_EXECUTION_PLAN`（同名的 TWAP/VWAP 分片命令彼此通用）。`POV_CANCEL`/`POV_REPORT` 与 TWAP/VWAP 的对应命令一样对三种计划通用。
- 当已释放量达到 `total_quantity` 时计划置为 `COMPLETED`（参与率公式已被总量封顶）；完成后继续喂量返回 `EXECUTION_PLAN_CLOSED`。

POV_CANCEL 与 POV_REPORT 均只携带 `event_id`、`type`、`plan_id`：

- `POV_CANCEL`：将**未释放量**（`total_quantity − 已释放量 − 已取消量`）计入取消量并把计划置为 `CANCELLED`；不撤销已释放子单（IOC 子单在释放事件结束时即已终结），不改变盘口、成交编号或历史成交。对已关闭计划返回 `EXECUTION_PLAN_CLOSED`。
- `POV_REPORT`：只读返回相同的累计汇总，不改变任何状态（包括累计市场量）；关闭后的计划仍可查询。未知计划返回 `UNKNOWN_EXECUTION_PLAN`。

每个 POV 命令响应中的 `execution_plan` 含：

- `status`：`ACTIVE`、`COMPLETED` 或 `CANCELLED`。
- `released_quantity`：已释放子单的原始数量之和；`filled_quantity`：子单累计成交量；`cancelled_quantity`：取消时计入的未释放量；`unreleased_quantity`：尚未释放也未取消的数量（计划关闭后为 0）。POV 没有固定片表，因此用 `unreleased_quantity` 取代 TWAP/VWAP 的 `remaining_slices`。
- `executed_notional`：累计成交额（成交价乘数量）。
- `vwap`：精确分数 `{"numerator": executed_notional, "denominator": filled_quantity}`；无成交时为 `null`。
- `slippage_notional`：买单为 `executed_notional − benchmark_price × filled_quantity`，卖单取相反数；负值表示相对基准改善。
- `algorithm`：恒为 `"POV"`。
- 仅 `POV_VOLUME` 的成功结果额外含 `child_order_id`（本次子单标识；零释放时为 `null`）；当且仅当本次发生正数释放时再额外含 `release_number`（本次正数释放序号，从 1 开始、只随正数释放递增）。

LIMIT POV 计划在每次正数释放**前**按当时的活动涨跌停区间复核计划价：越界返回 `PRICE_LIMIT_EXCEEDED`，占用该 `event_id` 并推进序列，但**不撮合、不消耗成交编号、不累计本次市场量、不改变释放状态**，已保留的派生标识继续保留（喂量前的复核先于市场量累计）。`POV_START` 的价格校验与 TWAP_START/VWAP_START 相同：重复计划、派生标识冲突等既有拒绝码优先于越界。

POV 计划完整状态（参与率、累计市场成交量、总量、每次正数释放的原始数量、进度与累计分析）进入快照；快照格式版本保持 `event-replay/2`，恢复后继续执行的结果、盘口与规范 JSON 与不中断回放逐字节一致。

### 单订单执行报告 EXECUTION_REPORT

`EXECUTION_REPORT` 是属于**单个证券**的只读查询（与基线 JSON Lines 入口的同名查询口径一致），按 `order_id` 汇总该证券内某个已接受订单的累计执行情况。它沿用事件信封的 `event_id`、`symbol`、`sequence` 顺序与幂等语义：

```json
{"event_id": "e7", "symbol": "AAA", "sequence": 4, "type": "EXECUTION_REPORT",
 "order_id": "o1", "benchmark_price": 100}
```

- 载荷只含 `event_id`、`type`、`order_id`、`benchmark_price`；`type` 固定为 `EXECUTION_REPORT`，`order_id` 为非空字符串，`benchmark_price` 为正整数（布尔值不算整数）。字段缺失、多出、类型错误或空 `order_id` 均按 `INVALID_EVENT` 拒绝，**不占用** `event_id` 且**不推进** `sequence`。
- 该证券内任何已被接受的订单均可查询：在簿、已成交、已撤销的订单，包括 ICEBERG 订单、REPLACE 后沿用原标识的订单，以及已释放的 TWAP/VWAP/POV 子单（`plan_id#N`）；母计划标识与尚未释放的派生标识不算订单。不同证券的同名 `order_id` 互不串查；未知名称的证券同样按 `UNKNOWN_ORDER` 处理。
- 结构合法但该证券内订单不存在时返回 `UNKNOWN_ORDER`：这是业务拒绝，**占用** `event_id`、**推进**信封 `symbol` 的序列，并回显该证券的未变盘口；其 `event_id` 只进入重放日志，不进入任何证券的引擎事件集合。
- 查询只读取该证券的订单记录与成交日志：不撮合、不补充冰山片段、不释放计划切片，也不改变订单、队列、成交日志、账户集合、计划或下一个成交编号。

查询成功时 `status` 为 `ACCEPTED`、`result` 为 `REPORTED`；`trades` 与 `book_changes` 为空，`bids`/`asks` 为该证券的未变盘口，并附加 `execution_analysis`（与基线单证券入口的字段口径完全一致）：

- `side`：`BUY` 或 `SELL`。
- `current_status`：`RESTING`、`FILLED` 或 `CANCELLED`。
- `open_quantity`：在簿订单的总余量（ICEBERG 含未公开储备）；其他状态为零。
- `filled_quantity`：累计成交数量。
- `executed_notional`：成交价乘数量的总和。
- `vwap`：无成交时为 `null`，否则为 `{"numerator": executed_notional, "denominator": filled_quantity}` 的精确分数。
- `slippage_notional`：买单为 `executed_notional − benchmark_price × filled_quantity`，卖单取相反数；负值表示相对基准改善。
- `trade_attribution`：按 `trade_id` 升序的逐笔归因，每项含 `trade_id`、`role`（`MAKER` 或 `TAKER`）、`counterparty_order_id`、`event_id`、`price`、`quantity`；maker 与 taker 成交均归集到本订单，REPLACE 前后与 ICEBERG 补片的成交都计入原 `order_id`。

信封、幂等与顺序规则优先适用：`DUPLICATE`、`EVENT_ID_CONFLICT`、`SEQUENCE_GAP`、`OUT_OF_ORDER` 的优先级与其他事件相同。查询的幂等记录进入快照；恢复后的重复识别、成交编号、字段顺序与规范 JSON 与不中断回放逐字节一致。

### 盘口冲击估算 IMPACT_REPORT

`IMPACT_REPORT` 是属于**单个证券**的只读假设分析（与基线 JSON Lines 入口的同名查询口径一致）：按信封 `symbol` 当时盘口，估算一笔匿名市价委托此刻的可执行量与成本。它沿用事件信封的 `event_id`、`symbol`、`sequence` 顺序与幂等语义，支持内联与嵌套两种载荷形式：

```json
{"event_id": "e8", "symbol": "AAA", "sequence": 4, "type": "IMPACT_REPORT",
 "side": "BUY", "quantity": 10, "benchmark_price": 100}
```

- 载荷只含 `event_id`、`type`、`side`、`quantity`、`benchmark_price`；`side` 固定为 `BUY` 或 `SELL`，`quantity` 与 `benchmark_price` 为正整数（布尔值不算整数）。字段缺失、多出、`side` 非法（含大小写不符、非字符串）或两个数值非正整数（布尔、零、负、浮点、字符串、空值）均按 `INVALID_EVENT` 拒绝，**不占用** `event_id` 且**不推进** `sequence`；嵌套形式下内层对象必须重复相同 `event_id`，否则同样按 `INVALID_EVENT` 拒绝。
- 合法查询**总是成功**（`status` 为 `ACCEPTED`、`result` 为 `REPORTED`），没有业务拒绝码：结构合法但首次出现的 `symbol` 按**空盘口**成功报告，同时注册该证券并推进其序列；之后该证券即按已建盘口回答。
- 模拟严格按价格时间优先进行：买单从最低卖价、卖单从最高买价起逐档成交，成交价取 maker 价格；冰山被动单只使用当前可见片段，片段耗尽但仍有储备时补一片 `min(display_quantity, remaining)` 并排到同价队尾，使逐片明细的次序与粒度与真实撮合一致。模拟单匿名无账户，**不触发**自成交防护；作为只读查询也**不受**活动涨跌停区间阻断（区间外既有挂单照常计入流动性）。
- 查询不撮合、不产生成交，不改变盘口、订单、队列、冰山可见量、计划、账户、成交日志、下一个成交编号或活动涨跌停区间；`trades` 与 `book_changes` 为空，`bids`/`asks` 回显该证券未变盘口。

成功结果附加 `impact_analysis`，字段与基线单证券入口**完全一致**：

- `side`、`requested_quantity`、`benchmark_price`：回显查询参数。
- `executable_quantity`：可执行数量；`unfilled_quantity`：`requested_quantity − executable_quantity`。
- `executed_notional`：各片成交价乘数量之和。
- `best_price`：查询前最佳对手价（买单为最低卖价、卖单为最高买价）；无流动性时为 `null`。
- `vwap`：无可成交时为 `null`，否则为 `{"numerator": executed_notional, "denominator": executable_quantity}` 的精确分数。
- `slippage_notional`：买单为 `executed_notional − benchmark_price × executable_quantity`，卖单取相反数；负值表示相对基准改善。
- `impact_notional`：以**查询前最佳对手价**替代基准价按同一公式计算的盘口冲击成本；卖单同样取相反数。
- `price_breakdown`：按模拟成交顺序排列的明细，每消耗一个被动可见片段产生一项（含同价不同 maker 与冰山各次补片），每项含 `price` 与 `quantity`。
- 空盘口（含首次出现的 `symbol`）与流动性不足沿用零值、`null` 与部分可执行语义；部分成交时两项成本只按实际可执行量计算。全部数值只用整数与精确分数，不使用浮点或舍入。

信封、幂等与顺序规则优先适用：成功查询**占用** `event_id`、推进信封证券序列，其 id 只进入重放日志，不进入任何证券的引擎事件集合；相同规范内容重试返回 `DUPLICATE`，同一 id 对应不同内容或另一证券返回 `EVENT_ID_CONFLICT`，序列空洞/倒退仍为 `SEQUENCE_GAP`/`OUT_OF_ORDER`。查询的幂等记录与序列状态进入现有快照（快照格式版本保持 `event-replay/2`）；恢复响应与连续回放逐字节一致。

### 跨证券组合报告 PORTFOLIO_REPORT

`PORTFOLIO_REPORT` 是属于多证券事件流的只读跨证券查询，在确定时点汇总一个账户跨全部证券的持仓、资金与风险。它沿用事件信封的 `event_id`、`symbol`、`sequence` 顺序与幂等语义：

```json
{"event_id": "e8", "symbol": "AAA", "sequence": 4, "type": "PORTFOLIO_REPORT",
 "account_id": "acct-1", "mark_prices": {"AAA": 100, "BBB": 50}}
```

- `account_id`：非空字符串。账户在**任一证券**上已有携带该 `account_id` 的已接受 ADD（无论订单最终在簿、成交、撤销或被自成交防护取消）即视为已知；已释放的 TWAP/VWAP/POV 子单继承母单账户，同样计入，已接受的 TWAP/VWAP/POV 母单即使尚未释放任何子单也使该账户在该证券上已知；仅出现在被拒绝事件中的账户不算已知。
- `mark_prices`：对象，键为非空证券名字符串，值为正整数标记价（布尔值不算整数）。键集合必须**恰好**覆盖该账户出现过的全部证券——既不能缺，也不能多。
- 字段缺失、多出、类型错误、`account_id` 为空标识、`mark_prices` 不是对象、含空键或非法价格（布尔、零、负、浮点、字符串、空值）均按 `INVALID_EVENT` 拒绝，**不占用** `event_id` 且**不推进** `sequence`。
- 结构合法但账户未知：`UNKNOWN_ACCOUNT`；账户已知但 `mark_prices` 键集合不符（含空对象）：`MARK_PRICE_MISMATCH`。二者都是业务拒绝：**占用** `event_id`、**推进**信封 `symbol` 的序列，并回显该信封证券的未变盘口；其 `event_id` 只进入重放日志，不进入任何证券的引擎事件集合。
- 查询只读取此前已接受事件产生的成交：不撮合、不释放计划切片，也不改变盘口、成交编号、计划或账户集合。

查询成功时 `status` 为 `ACCEPTED`、`result` 为 `REPORTED`；`trades` 与 `book_changes` 为空，`bids`/`asks` 为**信封 `symbol`** 的未变盘口，并附加 `portfolio_analysis`：

- `account_id`：回显查询账户。
- `positions`：按 `symbol` 字典序排列的每证券持仓项。统计同时计入该账户作为 maker、taker、REPLACE 后继承账户以及冰山补片的成交，未带 `account_id` 的订单不计入；每项含：
  - `symbol`、`mark_price`：证券代码与本次标记价。
  - `buy_quantity`、`sell_quantity`：累计买入、卖出数量。
  - `buy_notional`、`sell_notional`：累计买入、卖出成交额（成交价乘数量）。
  - `net_position`：`buy_quantity − sell_quantity`。
  - `cash_balance`：`sell_notional − buy_notional`。
  - `buy_vwap`、`sell_vwap`：对应数量为零时为 `null`，否则为 `{"numerator": 成交额, "denominator": 数量}` 的精确分数。
  - `turnover_notional`：`buy_notional + sell_notional`。
  - `position_market_value`：`net_position × mark_price`。
  - `risk_exposure`：`|net_position| × mark_price`（绝对风险敞口）。
  - `mark_to_market_pnl`：`cash_balance + position_market_value`。
- `totals`：跨证券汇总：
  - `buy_notional`、`sell_notional`、`cash_balance`（卖出额减买入额）、`turnover_notional` 为各证券对应项之和。
  - `position_market_value`：各证券 `net_position × mark_price` 之和（不先取绝对值，多空市值可相消）。
  - `risk_exposure`：各证券绝对风险敞口之和。
  - `mark_to_market_pnl`：`totals.cash_balance + totals.position_market_value`，与各证券盯市损益之和相等。

信封、幂等与顺序规则优先适用：成功与两类业务拒绝都占用 `event_id` 并推进信封证券序列；相同 `event_id` 同内容的重试返回 `DUPLICATE`，同 id 不同内容（或另一证券）返回 `EVENT_ID_CONFLICT`；序列空洞/倒退仍为 `SEQUENCE_GAP`/`OUT_OF_ORDER`。查询不改变快照中任何撮合相关结构；快照恢复后的查询结果与不中断连续回放逐字节一致。

### 多情景组合压力报告 PORTFOLIO_STRESS_REPORT

`PORTFOLIO_STRESS_REPORT` 是属于多证券事件流的只读跨证券查询，在确定时点用一个账户的既有成交先按基准标记价盯市，再按调用方给出的多个价格情景逐情景重估。账户归属、统计口径与 `PORTFOLIO_REPORT` 完全一致（计入 maker、taker、REPLACE 继承账户、冰山补片与已释放 TWAP/VWAP/POV 子单的累计成交，未带 `account_id` 的订单不建账），全程只使用整数。它沿用事件信封的 `event_id`、`symbol`、`sequence` 顺序与幂等语义：

```json
{"event_id": "e8b", "symbol": "AAA", "sequence": 5, "type": "PORTFOLIO_STRESS_REPORT",
 "account_id": "acct-1",
 "mark_prices": {"AAA": 100, "BBB": 50},
 "scenarios": [
   {"name": "crash", "prices": {"AAA": 90, "BBB": 45}},
   {"name": "boom", "prices": {"AAA": 110, "BBB": 55}}]}
```

- `account_id`：非空字符串，账户已知规则与 `PORTFOLIO_REPORT` 相同。
- `mark_prices`：基准标记价对象，规则与 `PORTFOLIO_REPORT` 相同——键为非空证券名字符串、值为正整数（布尔值不算整数），键集合必须**恰好**覆盖该账户出现过的全部证券。
- `scenarios`：**非空**数组，按请求顺序处理与返回。每项是**恰好**含 `name` 与 `prices` 的对象：`name` 为非空字符串且在本数组内**唯一**；`prices` 为价格对象，键为非空证券名字符串、值为正整数（布尔值不算整数），键集合同样必须**恰好**覆盖该账户出现过的全部证券。
- 字段缺失、多出、类型错误、`account_id` 为空标识、`mark_prices` 或任一 `prices` 不是对象、含空键或非法价格（布尔、零、负、浮点、字符串、空值）、`scenarios` 不是数组或为空、成员不是对象、成员缺字段或多字段、`name` 为空或重复，均按 `INVALID_EVENT` 拒绝，**不占用** `event_id` 且**不推进** `sequence`。
- 结构合法但账户未知：`UNKNOWN_ACCOUNT`；账户已知但 `mark_prices` 或**任一情景** `prices` 的键集合未恰好覆盖其全部证券（含空价格对象）：`MARK_PRICE_MISMATCH`。二者都是业务拒绝：**占用** `event_id`、**推进**信封 `symbol` 的序列，并回显该信封证券的未变盘口；其 `event_id` 只进入重放日志，不进入任何证券的引擎事件集合。账户未知优先于键集合校验。
- 查询只读取此前已接受事件产生的成交：不撮合、不释放计划切片，也不改变盘口、成交编号、计划或账户集合。

查询成功时 `status` 为 `ACCEPTED`、`result` 为 `REPORTED`；`trades` 与 `book_changes` 为空，`bids`/`asks` 为**信封 `symbol`** 的未变盘口，并附加 `portfolio_stress_analysis`：

- `account_id`：回显查询账户。
- `baseline`：基准盯市，含：
  - `positions`：按 `symbol` 字典序排列的每证券基准项，每项含 `symbol`、`mark_price`（基准价）、`net_position`（`buy_quantity − sell_quantity`）、`cash_balance`（`sell_notional − buy_notional`）、`position_market_value`（`net_position × mark_price`）与 `mark_to_market_pnl`（`cash_balance + position_market_value`）。
  - `mark_to_market_pnl`：各证券基准盯市损益之和（多空市值可相消）。
  - `risk_exposure`：各证券 `|net_position| × mark_price` 之和（基准绝对敞口）。
- `scenarios`：按**请求顺序**返回的情景结果。每个情景含：
  - `name`：回显情景名。
  - `positions`：按 `symbol` 字典序排列的每证券项，每项含 `symbol`、`shocked_price`（该情景下此证券的价格）、`net_position` 与 `pnl_change`。单证券 `pnl_change = net_position × (shocked_price − 基准价)`（多头涨价为正、空头涨价为负）。
  - `stressed_mark_to_market_pnl`：`baseline.mark_to_market_pnl + 本情景 total_pnl_change`，盯市损益仍按现金余额加持仓市值计算。
  - `risk_exposure`：各证券 `|net_position| × shocked_price` 之和。
  - `total_pnl_change`：各证券 `pnl_change` 之和。
- `worst_scenario`：`total_pnl_change` 最小的情景名；完全并列时取 `name` 字典序最前者。

信封、幂等与顺序规则优先适用：成功与两类业务拒绝都占用 `event_id` 并推进信封证券序列；相同 `event_id` 同内容的重试返回 `DUPLICATE`，同 id 不同内容（或另一证券）返回 `EVENT_ID_CONFLICT`；序列空洞/倒退仍为 `SEQUENCE_GAP`/`OUT_OF_ORDER`。查询不改变快照中任何撮合相关结构（快照格式版本保持 `event-replay/2`）；查询的幂等记录进入快照，恢复后的查询结果与不中断连续回放逐字节一致。

单证券 JSON Lines 入口（`order-book-engine replay`）与 `Engine` 不接受该事件：`PORTFOLIO_STRESS_REPORT` 按基线 `INVALID_SCHEMA` 拒绝，既有事件与公开入口的行为全部不变。

### 全会话对账 SESSION_RECONCILIATION

`SESSION_RECONCILIATION` 是属于多证券事件流的只读查询，一次把外部记录与**全部证券**的全历史成交和全部账户账簿核对。它不进入单证券回放，也不改变基线 `DAY_END_RECONCILIATION`：后者仍只属于 JSON Lines 入口、只核对此前事件流中的单个证券，且外部记录不含 `symbol`。本查询沿用事件信封的 `event_id`、`symbol`、`sequence` 顺序与幂等语义：

```json
{"event_id": "e9", "symbol": "AAA", "sequence": 5, "type": "SESSION_RECONCILIATION",
 "expected_trades": [
   {"symbol": "AAA", "trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1",
    "price": 100, "quantity": 3},
   {"symbol": "BBB", "trade_id": 1, "maker_order_id": "w1", "taker_order_id": "p1#1",
    "price": 70, "quantity": 1}
 ],
 "expected_accounts": [
   {"symbol": "AAA", "account_id": "acct-1", "net_position": -3, "cash_balance": 300},
   {"symbol": "BBB", "account_id": "acct-2", "net_position": 1, "cash_balance": -70}
 ]}
```

- `expected_trades`：外部成交记录数组。每项**恰好**含 `symbol`（非空字符串）、`trade_id`（正整数）、`maker_order_id` 与 `taker_order_id`（非空字符串）、`price` 与 `quantity`（正整数，布尔值不算整数）；以 `(symbol, trade_id)` 为唯一键——成交编号在各证券内独立从 1 递增，不同证券的相同 `trade_id` 不是重复，同一证券内重复才非法。
- `expected_accounts`：外部账户记录数组。每项**恰好**含 `symbol`（非空字符串）、`account_id`（非空字符串）、`net_position` 与 `cash_balance`（均可为任意整数，含零与负数；布尔值不算整数）；以 `(symbol, account_id)` 为唯一键，同证券内账户标识重复即非法。
- 字段缺漏或多出、标识为空或重复、数组本身非法（非数组或 `null`）或成员非法（非对象）、成交数值非正（零、负、浮点、字符串、布尔）、账户数值以布尔冒充整数，均按 `INVALID_EVENT` 拒绝，**不占用** `event_id`、**不推进** `sequence`；两个数组为空是合法查询。

实际侧由会话状态重建：实际成交取每个证券的**全历史**成交（比较字段不含内部 `event_id`，但记录带其所属 `symbol`）；实际账户为已接受 `ADD` 携带的账户与已接受 TWAP/VWAP/POV 母单携带的账户（含母单从未释放切片的零成交账户），持仓为买入量减卖出量，现金为卖出成交额减买入成交额，统计口径与基线对账及 `PORTFOLIO_REPORT` 一致（REPLACE 继承账户、冰山补片沿用同一 maker id，未带 `account_id` 的订单不建账）。实际侧按 `(symbol, ...)` 唯一，因此两侧记录天然按键全外连接比较。

结构合法的查询总是 `status: ACCEPTED` 并**占用** `event_id`、推进信封 `symbol` 的序列（无业务拒绝码）；其 `event_id` 只进入重放日志，不进入任何引擎事件集合。完全一致时 `result` 为 `RECONCILED`，否则为 `BREAKS_FOUND`；两种结果的 `trades` 与 `book_changes` 均为空，`bids`/`asks` 回显**信封 `symbol`** 的未变盘口，并附加 `reconciliation`：

- `trade_breaks`、`account_breaks`：差异数组。`trade_breaks` 按 `(symbol, trade_id)`、`account_breaks` 按 `(symbol, account_id)` 稳定升序排列（先比 `symbol` 字典序，再比标识），无差异时为空数组；外部数组的输入顺序不影响输出。
- 每项含 `identifier`、`expected`、`actual`、`reason`：`identifier` 为复合键的二元素数组，成交项为 `[symbol, trade_id]`、账户项为 `[symbol, account_id]`；仅外部存在（实际缺失）时 `reason` 为 `MISSING_ACTUAL`，仅实际存在（外部缺失）时为 `MISSING_EXPECTED`，两侧都存在但任一字段不同时为 `FIELD_MISMATCH`；缺失一侧为 `null`，存在一侧保留含 `symbol` 的完整记录对象。

查询是只读的：不撮合、不释放计划切片，不改变订单、计划、账户集合、任何盘口、成交日志、下一个成交编号或活动涨跌停区间。相同 `event_id` 同内容的重试返回 `DUPLICATE`，同 id 不同内容（或另一证券）返回 `EVENT_ID_CONFLICT`，序列空洞/倒退仍为 `SEQUENCE_GAP`/`OUT_OF_ORDER`。查询的幂等记录进入快照；恢复前后的拒绝结果、差异顺序与规范 JSON 逐字节一致。

### 母单实施缺口报告 PLAN_TCA_REPORT

`PLAN_TCA_REPORT` 是属于多证券事件流的只读查询，用调用方指定的**评估价**补齐某个 TWAP/VWAP/POV 母单的实施缺口（implementation shortfall）分析，不新增也不改变任何计划命令（`TWAP_REPORT`/`VWAP_REPORT`/`POV_REPORT` 的既有汇总口径不变）。它沿用事件信封的 `event_id`、`symbol`、`sequence` 顺序与幂等语义：

```json
{"event_id": "e10", "symbol": "AAA", "sequence": 6, "type": "PLAN_TCA_REPORT",
 "plan_id": "p1", "mark_price": 103}
```

- 载荷只含 `event_id`、`type`、`plan_id`、`mark_price`；`type` 固定为 `PLAN_TCA_REPORT`，`plan_id` 为非空字符串，`mark_price` 为正整数（布尔值不算整数）。字段缺失、多出、类型错误或空 `plan_id` 均按 `INVALID_EVENT` 拒绝，**不占用** `event_id` 且**不推进** `sequence`。
- 信封 `symbol` 下任何已接受的 TWAP、VWAP、POV 计划均可查询，包括 `ACTIVE`、`COMPLETED` 与 `CANCELLED` 状态；计划 id 在该证券不存在——包括只在其他证券存在的同名计划——时返回 `UNKNOWN_EXECUTION_PLAN`。这是业务拒绝：**占用** `event_id`、**推进**信封 `symbol` 的序列，并回显该证券的未变盘口；其 `event_id` 只进入重放日志，不进入任何证券的引擎事件集合。
- 查询只读取该证券的计划状态：不撮合、不释放切片、不喂入 POV 市场量，也不改变计划、订单、账户集合、盘口、成交日志、下一个成交编号或活动涨跌停区间。

查询成功时 `status` 为 `ACCEPTED`、`result` 为 `REPORTED`；`trades` 与 `book_changes` 为空，`bids`/`asks` 为该证券的未变盘口，并附加 `plan_tca_analysis`：

- `plan_id`：回显查询的计划标识。
- `algorithm`：`"TWAP"`、`"VWAP"` 或 `"POV"`。
- `side`：`BUY` 或 `SELL`；`status`：`ACTIVE`、`COMPLETED` 或 `CANCELLED`。
- `benchmark_price`：计划启动时的基准价；`total_quantity`：计划总量。
- `executed_notional`：累计成交额（成交价乘数量）；`vwap`：无成交时为 `null`，否则为 `{"numerator": executed_notional, "denominator": filled_quantity}` 的精确分数。
- `mark_price`：回显本次评估价。
- `opportunity_quantity`：机会数量，等于 `total_quantity − filled_quantity`。
- `execution_slippage_notional`：成交滑点。买单为 `executed_notional − benchmark_price × filled_quantity`，卖单取相反数；负值表示相对基准改善。
- `opportunity_cost_notional`：机会成本。买单为 `(mark_price − benchmark_price) × opportunity_quantity`，卖单取相反数；负值表示改善。
- `implementation_shortfall_notional`：实施缺口，为成交滑点与机会成本之和；负值表示改善。

全部计算只使用整数与既有精确分数（`vwap`），不引入浮点或舍入；TWAP/VWAP/POV 三种算法与三种生命周期状态使用同一口径。信封、幂等与顺序规则优先适用：成功与 `UNKNOWN_EXECUTION_PLAN` 业务拒绝都占用 `event_id` 并推进信封证券序列；相同 `event_id` 同内容的重试返回 `DUPLICATE`，同 id 不同内容（或另一证券）返回 `EVENT_ID_CONFLICT`，序列空洞/倒退仍为 `SEQUENCE_GAP`/`OUT_OF_ORDER`。查询不改变快照中任何撮合相关结构（快照格式版本保持 `event-replay/2`）；查询的幂等记录进入快照，恢复后的查询结果与不中断连续回放逐字节一致。

单证券 JSON Lines 入口（`order-book-engine replay`）与 `Engine` 不接受该事件：`PLAN_TCA_REPORT` 按基线 `INVALID_SCHEMA` 拒绝，既有事件与快照的兼容行为全部不变。

### 历史盘口重建报告 BOOK_RECONSTRUCTION_REPORT

`BOOK_RECONSTRUCTION_REPORT` 是属于多证券事件流的只读历史查询：按信封 `symbol` 查询该证券在本次会话某个**已提交序列结束时**的完整盘口队列（含逐档订单与价格时间优先级），而不是查询时盘口。它沿用事件信封的 `event_id`、`symbol`、`sequence` 顺序与幂等语义：

```json
{"event_id": "e11", "symbol": "AAA", "sequence": 8, "type": "BOOK_RECONSTRUCTION_REPORT",
 "target_sequence": 3}
```

- 载荷只含 `event_id`、`type`、`target_sequence`；`target_sequence` 必须是**非布尔的非负整数**（布尔、浮点、字符串、空值均非法）。字段缺失、多出、类型错误或为负数均按 `INVALID_EVENT` 拒绝，**不占用** `event_id` 且**不推进** `sequence`；支持内联与嵌套（`event` 对象必须重复相同 `event_id` 与 `type`）两种载荷形式。
- `target_sequence` 为 `0` 表示首个事件**之前**的状态：空盘口与会话初始涨跌停区间（取自静态 `price_limits` 配置；未配置为 `null`）。未知证券查询 `0` 时按空盘口成功回答，并同时注册该证券、推进其序列。
- `target_sequence` 为正数时必须是该证券本次会话已经提交的序列（被业务拒绝的事件与其他只读报告虽占序列但不改变状态，同样可以作为目标，重建结果等于其前一状态变更事件结束时）。目标晚于查询前该证券最后已提交序列（含对未知证券查询正数）时返回 `TARGET_SEQUENCE_NOT_FOUND`：这是业务拒绝，**占用** `event_id`、**推进**信封 `symbol` 的序列且盘口状态不变（与其他业务拒绝一样会注册该证券，其簿保持为空）。
- 重建通过把该证券已提交事件按序列顺序在一次性的隔离会话中重放到目标序列完成：撮合、冰山补片排到同价队尾、REPLACE 失去队列优先级、已成交/撤销/被替换订单出簿、TWAP/VWAP/POV 算法子单的实际结果、以及 `PRICE_LIMIT_UPDATE` 对活动区间的替换，都按目标时点的实际结果重现；只读报告事件不被重新执行。隔离会话用后即弃，因此查询绝不撮合、绝不释放计划、绝不改变活会话的任何交易状态、活动区间或下一个成交编号。

查询成功时 `status` 为 `ACCEPTED`、`result` 为 `REPORTED`；`trades` 与 `book_changes` 为空，外层 `bids`/`asks` 仍是**查询时**（当前）盘口，并附加 `book_reconstruction`：

- `symbol`：回显信封证券；`target_sequence`：回显目标序列。
- `active_price_limits`：**目标时点**的活动涨跌停区间 `{"lower_price": ..., "upper_price": ...}`；目标时点不限时为 `null`；目标为 `0` 时恒为会话初始区间。
- `bid_queues`：按价格**降序**的买方档位；`ask_queues`：按价格**升序**的卖方档位；每档含 `price`、该档公开量合计 `visible_quantity` 和按**原撮合优先级**（同档即时间优先、含冰山补片排到队尾后的实际次序）排列的 `orders`。
- 每个订单含 `order_id`、`order_type`（在簿订单只可能是 `LIMIT` 或 `ICEBERG`）、`remaining_quantity` 与 `visible_quantity`：冰山单的 `remaining_quantity` 含未公开储备，`visible_quantity` 仅为当前公开片段；普通限价单二者相等。已成交、已撤销或被替换的状态不出现。

信封、幂等与顺序规则优先适用：成功与 `TARGET_SEQUENCE_NOT_FOUND` 业务拒绝都占用 `event_id` 并推进信封证券序列；其 id 只进入重放日志，不进入任何证券的引擎事件集合；相同 `event_id` 同内容的重试返回 `DUPLICATE`（不附带 `book_reconstruction`），同 id 不同内容（或另一证券）返回 `EVENT_ID_CONFLICT`，序列空洞/倒退仍为 `SEQUENCE_GAP`/`OUT_OF_ORDER`。查询的幂等记录进入现有快照（快照格式版本保持 `event-replay/2`）；快照恢复后的重建结果、队列顺序与重复投递响应与不中断连续回放逐字节一致，其他事件及旧快照恢复行为不变。

单证券 JSON Lines 入口（`order-book-engine replay`）与 `Engine` 不接受该事件：`BOOK_RECONSTRUCTION_REPORT` 按基线 `INVALID_SCHEMA` 拒绝且不占用 `event_id`，既有事件与快照的兼容行为全部不变。

### 提交语义与错误码

- 逐事件提交：先前成功事件不会因后续失败回滚；失败事件不留下订单、成交、计数器或盘口变更。
- `INVALID_EVENT`：信封/必填字段缺失、非法数值、未知事件类型、载荷与信封不一致等（对应基线结构错误，不占用 `event_id` 与序列）。
- `SEQUENCE_GAP`：当前证券序列出现空洞（大于期望值）；不占用该序列与 `event_id`，修正后的事件可立即提交。
- `OUT_OF_ORDER`：序列倒退（小于期望值）。
- `EVENT_ID_CONFLICT`：已见 `eventId` 但规范化内容不一致（或用于另一证券）；不再次撮合、不占用序列。
- `DUPLICATE`：已见 `eventId` 且规范化内容（键排序后的紧凑 JSON）完全一致；不再次撮合、无成交无盘口变更。重试投递携带旧序列时仍识别为重复。
- 撤单/改单不存在或已终结订单，继续沿用基线拒绝码（如 `UNKNOWN_ORDER`、`DUPLICATE_ORDER_ID`）；这类有效事件与基线一样占用其 `event_id` 并推进该证券序列。
- TWAP/VWAP/POV 业务拒绝码：`UNKNOWN_EXECUTION_PLAN`（未知计划）、`EXECUTION_PLAN_CLOSED`（对已关闭计划切片、喂量或取消）、`DUPLICATE_EXECUTION_PLAN`（同证券重复 `plan_id`，三种计划共用命名空间）；派生标识冲突沿用 `DUPLICATE_ORDER_ID`。这些有效命令同样占用 `event_id` 并推进序列，但不改变计划或盘口状态。
- 跨证券组合报告拒绝码：`UNKNOWN_ACCOUNT`（账户在任一证券上均无已接受 ADD 或已接受 TWAP/VWAP/POV 母单）、`MARK_PRICE_MISMATCH`（已知账户但 `mark_prices` 键集合未恰好覆盖其全部证券）。二者都是结构合法后的业务拒绝，占用 `event_id`、推进信封证券序列并回显未变盘口；结构错误仍为 `INVALID_EVENT`，不占用 `event_id` 与序列。
- 多情景组合压力报告拒绝码：沿用 `PORTFOLIO_REPORT` 的 `UNKNOWN_ACCOUNT` 与 `MARK_PRICE_MISMATCH`，后者在 `mark_prices` 或**任一情景** `prices` 的键集合未恰好覆盖账户全部证券时返回；情景数组为空、情景名空或重复、任一价格对象结构或价格非法为 `INVALID_EVENT`。业务拒绝同样占用 `event_id`、推进信封证券序列并回显未变盘口；结构错误不占用 `event_id` 与序列。
- 单订单执行报告拒绝码：`UNKNOWN_ORDER`（该证券内无此已接受订单，含母计划标识、未释放派生标识、他证券同名标识与未知证券）。同为结构合法后的业务拒绝，占用 `event_id`、推进信封证券序列并回显未变盘口；结构错误仍为 `INVALID_EVENT`，不占用 `event_id` 与序列。
- 盘口冲击估算：`IMPACT_REPORT` 结构合法时总是成功，没有业务拒绝码——首次出现的证券按空盘口成功报告并注册该证券、推进序列；查询不触发自成交防护，也不受活动涨跌停阻断。只有结构错误才为 `INVALID_EVENT`（不占用 `event_id` 与序列）；其 id 只进入重放日志，不进入任何证券的引擎事件集合。
- 母单实施缺口报告拒绝码：`UNKNOWN_EXECUTION_PLAN`（信封证券下无此 TWAP/VWAP/POV 计划，含只在其他证券存在的同名计划；`ACTIVE`/`COMPLETED`/`CANCELLED` 状态的计划均可查询）。同为结构合法后的业务拒绝，占用 `event_id`、推进信封证券序列并回显未变盘口；结构错误仍为 `INVALID_EVENT`，不占用 `event_id` 与序列。
- 历史盘口重建报告拒绝码：`TARGET_SEQUENCE_NOT_FOUND`（正数 `target_sequence` 晚于查询前该证券最后已提交序列，含对未知证券查询正数）。同为结构合法后的业务拒绝，占用 `event_id`、推进信封证券序列且盘口状态不变（与其他业务拒绝一样注册该证券，其簿保持为空）；目标 `0` 总是成功（未知证券返回空盘口与会话初始涨跌停区间）；结构错误（布尔、负数、字段缺失/多出等）仍为 `INVALID_EVENT`，不占用 `event_id` 与序列。
- 相同初始状态、配置和事件流产生字段顺序稳定、数值表示一致、可逐字节比较的 JSON（规范化序列化：键排序、紧凑分隔、整数不丢精度、无浮点）。
- 静态涨跌停越界码：`PRICE_LIMIT_EXCEEDED`，见下文「静态涨跌停 `price_limits`」一节。

### 静态涨跌停 `price_limits`

可选的会话级配置，按证券给出静态涨跌停闭区间，放在 `config` 中：

```json
{"price_limits": {"AAA": {"lower": 95, "upper": 105},
                  "BBB": {"lower": 10, "upper": 20}}}
```

- 键为非空证券名字符串；值是**只含** `lower` 与 `upper` 的对象。
- `lower`、`upper` 均为正整数（布尔值不算整数，不接受字符串、浮点等），且 `lower <= upper`；二者可以相等（区间退化为单一允许价格）。
- 缺省 `config`、空块 `{"price_limits": {}}` 或未配置的证券保持基线行为。
- 配置在处理任何事件、采纳任何快照之前校验；非法配置使 Python 入口抛出 `ValueError`，CLI 返回退出码 2 与既有 `INVALID_REQUEST` 错误文档。

校验范围（闭区间，边界价合法）：

- LIMIT 与 ICEBERG 的 `ADD`：其 `price` 必须在区间内；`MARKET` 的 ADD 不受校验。
- `REPLACE`：新 `price` 必须在区间内。越界替换是业务拒绝：**保留原委托及其队列优先级**，不撮合、不改变盘口或成交编号。
- `TWAP_START`、`VWAP_START`、`POV_START`：仅 LIMIT 计划校验其 `price`；MARKET 计划不受校验。越界启动**不创建计划、不保留任何派生标识**（`plan_id#n` 可立即被其他委托或新计划使用）。
- 校验时机在现有信封、幂等、序列与标识冲突检查**之后**、撮合与任何状态变更**之前**。因此 `DUPLICATE_ORDER_ID`（含派生标识冲突）、`UNKNOWN_ORDER`、`DUPLICATE_EXECUTION_PLAN` 等既有结果优先于越界；结构非法仍为 `INVALID_EVENT` 且不占用任何标识。

越界事件的结果：`status` 为 `REJECTED`、`rejection_code` 为 `PRICE_LIMIT_EXCEEDED`，`trades` 与 `book_changes` 均为空，并回显未变化盘口。它与其他业务拒绝一样**占用 `event_id` 并推进该证券 `sequence`**，但不留下订单或计划、不消耗成交编号；合法事件的价格时间优先、FOK 原子性、IOC 余量取消、冰山补片与自成交防护规则不变。

涨跌停配置属于会话 `config`，随快照的 `config`、`config_digest` 与 `content_digest` 做确定性序列化（键排序的规范化 JSON）：

- 相同配置从快照恢复后，后续结果、成交编号与最终盘口和连续回放逐字节一致。
- 恢复时调用方配置（含涨跌停边界）与快照不一致时，抛出 `code` 为 `CONFIG_MISMATCH` 的 `SnapshotError`，CLI 沿用对应错误文档与退出码 2。
- 单标的 `order-book-engine replay`、`Engine` 的既有输入输出、未传 `price_limits` 的历史调用，以及 TWAP/VWAP 的分片、汇总与报告字段均不因该特性改变。

### 盘中涨跌停调整 `PRICE_LIMIT_UPDATE`

`PRICE_LIMIT_UPDATE` 是多证券事件流专属的盘中调整命令（`replay_events`/`EventReplayer`/`events` CLI；单证券 JSON Lines 入口与 `Engine` 不接受该类型），把某证券的**活动涨跌停区间**整体替换为新值。活动区间以静态 `price_limits` 配置为初始值（未配置则不限），此后由该证券每个已接受的 `PRICE_LIMIT_UPDATE` 完整替换：

```json
{"event_id": "e5", "symbol": "AAA", "sequence": 5,
 "type": "PRICE_LIMIT_UPDATE", "lower_price": 98, "upper_price": 102}
```

- 载荷只含 `event_id`、`type`、`lower_price`、`upper_price`；两个边界均为正整数（布尔值不算整数）且 `lower_price <= upper_price`。字段缺失、多出、类型不符、非正边界或上下界倒置均为 `INVALID_EVENT`，不占用 `event_id` 与序列。
- 接受后结果为 `status: ACCEPTED`、`result: PRICE_LIMIT_UPDATED`，`trades` 与 `book_changes` 为空、盘口保持不变，并附 `active_price_limits: {"lower_price": ..., "upper_price": ...}` 回显新区间。相同边界的更新仍然成功；重复、冲突与序列错误沿用 `DUPLICATE`、`EVENT_ID_CONFLICT`、`SEQUENCE_GAP`、`OUT_OF_ORDER` 等既有语义。
- 活动区间约束**之后提交**的限价：LIMIT/ICEBERG `ADD`、`REPLACE`、LIMIT `TWAP_START`/`VWAP_START`/`POV_START` 按既有优先级校验（标识冲突等既有拒绝码优先）；已启动的 LIMIT 计划在每次 TWAP/VWAP 切片或 POV 正数释放前按**当时**的活动区间复核（POV 的复核先于本次市场量累计：越界时市场量与释放状态均不变）。越界统一返回 `PRICE_LIMIT_EXCEEDED`，占用 `event_id` 并推进序列，但不撮合、不消耗成交编号：`ADD` 不建单，`REPLACE` 保留原单及队列位置，`START` 不建计划，`SLICE`/`POV_VOLUME` 不推进片号/市场量与累计量且已保留的派生标识继续保留。市价订单与市价计划不受限制。
- 缩窄区间不撤销也不移动既有挂单：区间外旧单仍按原优先级留在队列中并可成为 maker（限制对象是新提交的限价，不是成交价）。
- 每证券活动区间进入快照（`price_limits`，`null` 表示不限）并受 `content_digest` 保护；恢复后与连续回放逐字节一致。缺少该字段的旧格式快照在恢复时从 `config` 的静态区间初始化。

### 快照与恢复

`replay_events` 默认在响应中附带处理完最后一个事件后的 `snapshot`；`snapshot_after=None` 可省略，`snapshot_after={"symbol": ..., "sequence": ...}` 可在指定的**已接受**事件之后导出。有状态的会话也可用 `EventReplayer`（`submit`/`book`）配合 `export_snapshot`/`restore_replayer` 增量处理。

快照是 JSON 对象：

- `format_version`：格式版本（当前 `event-replay/2`；较 `event-replay/1` 在每证券状态中增加 `plans`、在引擎状态中增加 `reserved_order_ids`）。
- `engine_version`、`config`（撮合配置摘要）与 `config_digest`（配置的 SHA-256）。
- `content`：各证券完整状态——价格时间队列顺序（含每档订单 id 队列）、订单剩余量、冰山当前公开量 `visible` 与补量所需 `display_quantity`、各证券最后序列 `last_sequence`、活动涨跌停区间 `price_limits`（`null` 表示不限；旧格式快照缺少该字段，恢复时从 `config` 静态区间初始化）、已接受事件日志、TWAP/VWAP/POV 计划列表 `plans`（计划参数——VWAP 计划另含 `algorithm` 与 `volume_weights`，POV 计划另含 `algorithm`、`total_quantity`、`participation_bps` 与累计市场成交量 `market_volume`——各次正数释放的原始数量、已释放次数与已释放量、成交量、取消量、成交额、生命周期状态）、引擎保留的未释放派生标识 `reserved_order_ids`、累计成交 `trade_log`、生成后续成交标识所需的 `next_trade_id`、账户集合。
- `content_digest`：基于规范化内容（连同版本与配置）计算的 SHA-256。

恢复（`restore_replayer(snapshot, config=None)` 或在 `replay_events` 中传 `snapshot=`）先验证版本、配置与摘要，再做结构与内部一致性交叉校验，全部通过后才采纳状态：

- 摘要不符：`SNAPSHOT_CORRUPT`。
- 版本不支持：`SNAPSHOT_VERSION_UNSUPPORTED`。
- 配置或配置摘要不一致：`CONFIG_MISMATCH`。
- 任何恢复失败都抛出 `SnapshotError`（其 `code` 为上述代码）且不创建部分恢复状态；CLI 将其映射为退出码 2 的错误文档。
- 恢复成功后继续生成的逐笔成交标识、成交顺序和最终结果，与不中断的一次性回放完全一致（逐字节比较）。


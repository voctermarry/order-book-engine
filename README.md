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
order-book-engine replay     # 从标准输入回放订单事件
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
  - 拒绝：`REJECTED`（附加 `reason`）
- `self_trade_prevention`：仅在两种自成交防护结果下出现，序列化于 `result`/`reason` 之后、`trades` 之前，含 `maker_order_id`（触发的被动单）、`taker_order_id`（被取消的主动单）与 `cancelled_quantity`（取消量，等于触发时主动单的剩余量；FOK 预检触发时为原始委托量）。其他结果不得包含该字段。
- `execution_analysis`：仅在 `REPORTED` 结果下出现，序列化于 `result` 之后、`trades` 之前，字段见上文 EXECUTION_REPORT 一节。其他结果不得包含该字段。
- `trades`：按发生顺序排列；每笔含 `maker_order_id`、`taker_order_id`、`price`、`quantity` 与 `trade_id`。
- `bids` 按价格降序、`asks` 按价格升序，每档含整数 `price` 与汇总 `quantity`。

拒绝原因：`INVALID_JSON`、`INVALID_SCHEMA`（非对象、缺字段、字段类型或枚举错误、未知字段）、`DUPLICATE_EVENT_ID`、`DUPLICATE_ORDER_ID`、`UNKNOWN_ORDER`。拒绝对象带空 `trades` 和拒绝前盘口，不改变订单簿、成交编号或后续优先级。

## 现有公开接口

- 命令行程序 `order-book-engine`（`version`、`replay`）
- Python 包 `order_book_engine`，其 `__version__` 为当前版本号

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
- `account_id`：可选，非空字符串，用于按账户标识的自成交防护（见下）；空字符串或非字符串取值按 `INVALID_SCHEMA` 拒绝。
- `time_in_force`：可选，取值 `GTC`、`IOC`、`FOK`。
  - LIMIT 省略时按 `GTC` 处理（余量入簿）；`GTC`、`IOC`、`FOK` 均要求有效 `price`。
  - ICEBERG 仅接受省略或显式 `GTC`（其余时效、`null` 及非字符串取值按 `INVALID_SCHEMA` 拒绝）；合法订单先以全部余量主动撮合，剩余部分仅将 `min(display_quantity, remaining)` 纳入盘口，返回 `FILLED`、`PARTIALLY_FILLED_RESTING` 或 `RESTING`。
  - MARKET 省略时保持「立即成交、余量取消」语义；可显式指定 `IOC` 或 `FOK`，不得为 `GTC`。
  - `IOC`：仅撮合事件到达时可成交的数量，余量一律取消、不入簿；完全成交为 `FILLED`，部分成交为 `PARTIALLY_FILLED_CANCELLED`，完全未成交为 `UNFILLED_CANCELLED`。
  - `FOK`：先依据事件到达前的可成交盘口判断全部数量能否在限价范围内成交。数量足够时一次性生成全部成交；数量不足时不产生任何成交、不改变盘口、不消耗成交编号，返回 `UNFILLED_CANCELLED`。预检按真实价格时间顺序模拟撮合，ICEBERG 的储备按既有补片次序逐片计入可成交量。预检在凑足数量前遇到同账户被动单时，原子返回 `SELF_TRADE_PREVENTED`（`cancelled_quantity` 为原始委托量），不成交、不改变盘口、不消耗成交编号。失败的 FOK 仍是已处理订单，其 `event_id` 与 `order_id` 均被占用。
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

替换成功时先移除目标全部余量，再将新委托作为本事件到达的 GTC 委托处理：即使参数未变也失去原队列优先级，不会与旧状态成交，但可作为 taker 撮合其他订单。无成交且余量入簿时 `result` 为 `REPLACED`，部分成交后入簿为 `PARTIALLY_FILLED_RESTING`，全部成交为 `FILLED`。冰山余量仅展示 `min(display_quantity, remaining)`，补片仍排到同价队尾。替换沿用原 `order_id`，不触发 `DUPLICATE_ORDER_ID`，且该 id 不允许后续 ADD 重用；移除、撮合与余量入簿是不可分割的状态变更。目标未知的有效事件占用 `event_id`，结构错误不占用。

### 撮合规则

- 买单匹配最低卖价，卖单匹配最高买价；同价位先到者优先（价格时间优先）。
- 限价单不得越过自身价格；成交价取被动单（maker）价格。
- GTC 限价单余量入簿；IOC 余量取消、绝不入簿；FOK 要么在事件前盘口上全部成交，要么完全不成交；市价单余量取消；撤单不产生成交。
- ICEBERG 被动成交只消耗当前公开片段；片段耗尽但仍有储备时，立即公开下一片 `min(display_quantity, remaining)`，并排到同价已有可见订单之后，因此同一主动单可在其他同价单之后再次遇到它。各片沿用同一 `maker_order_id`；`bids`/`asks` 只汇总当前公开片段。市价单、IOC 与普通限价单均可消耗补片；FOK 失败时不补片、不改变盘口。
- 成交编号从 1 开始连续递增（失败的 FOK 不消耗编号）。

### 自成交防护（按账户标识）

- ADD 可携带 `account_id`（非空字符串），适用于 LIMIT、MARKET、ICEBERG 各类新增委托；REPLACE 继承目标订单的 `account_id`，事件自身不得携带该字段；CANCEL 不变。`account_id` 出现在非 ADD 事件、为空或非字符串时按 `INVALID_SCHEMA` 拒绝，不占用 `event_id`、不改变状态。
- 仅当主动单与被动单都带有且 `account_id` 完全相同时触发防护；任一方未提供时按既有规则撮合。
- 撮合仍按价格时间优先查找对手。主动单将命中首笔同账户被动单时：不生成该笔成交，不改变被动单的队列位置、剩余量与冰山公开片段，立即取消主动单全部余量，也不越过它寻找其他流动性。
- 触发前已完成的外部成交保留并占用 `trade_id`；此前无成交时 `result` 为 `SELF_TRADE_PREVENTED`，否则为 `PARTIALLY_FILLED_SELF_TRADE_PREVENTED`。两种结果的输出附加 `self_trade_prevention` 对象，含 `maker_order_id`、`taker_order_id` 与 `cancelled_quantity`（等于触发时主动单余量）；其他结果不含该对象。
- 被取消的订单仍占用 `event_id` 与 `order_id`，之后不能撤销、替换或通过 ADD 重用。
- REPLACE 先移除旧委托，再让继承账户的新委托应用防护；触发后不恢复旧委托。

### 每个事件的输出

```json
{"input_line": "…", "event_id": "e1", "result": "FILLED", "trades": [
  {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1", "price": 100, "quantity": 5}
], "bids": [], "asks": []}
```

- `input_line`：去除行终止符后的原始输入文本。
- `event_id`：可取得时为字符串，否则为 `null`。
- `result`：
  - GTC 限价单/ICEBERG：`FILLED`、`RESTING`、`PARTIALLY_FILLED_RESTING`
  - IOC 限价单/市价单：`FILLED`、`PARTIALLY_FILLED_CANCELLED`、`UNFILLED_CANCELLED`
  - FOK 限价单/市价单：`FILLED`、`UNFILLED_CANCELLED`
  - 撤单成功：`CANCELLED`
  - 替换成功：`REPLACED`、`PARTIALLY_FILLED_RESTING`、`FILLED`
  - 自成交防护：`SELF_TRADE_PREVENTED`、`PARTIALLY_FILLED_SELF_TRADE_PREVENTED`（附加 `self_trade_prevention`）
  - 拒绝：`REJECTED`（附加 `reason`）
- `trades`：按发生顺序排列；每笔含 `maker_order_id`、`taker_order_id`、`price`、`quantity` 与 `trade_id`。
- `bids` 按价格降序、`asks` 按价格升序，每档含整数 `price` 与汇总 `quantity`。

拒绝原因：`INVALID_JSON`、`INVALID_SCHEMA`（非对象、缺字段、字段类型或枚举错误、未知字段）、`DUPLICATE_EVENT_ID`、`DUPLICATE_ORDER_ID`、`UNKNOWN_ORDER`。拒绝对象带空 `trades` 和拒绝前盘口，不改变订单簿、成交编号或后续优先级。

## 现有公开接口

- 命令行程序 `order-book-engine`（`version`、`replay`）
- Python 包 `order_book_engine`，其 `__version__` 为当前版本号

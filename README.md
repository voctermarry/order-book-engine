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
{"event_id": "e1", "type": "ADD", "order_id": "o1", "side": "BUY", "order_type": "LIMIT", "quantity": 5, "price": 100}
```

- `event_id`：全流唯一字符串。
- `order_id`：唯一字符串。
- `side`：`BUY` 或 `SELL`。
- `order_type`：`LIMIT` 或 `MARKET`。
- `quantity`：正整数。
- `price`：LIMIT 必填正整数；MARKET 不得带非空 `price`（可省略或为 `null`）。

CANCEL：

```json
{"event_id": "e2", "type": "CANCEL", "order_id": "o1"}
```

按 `order_id` 撤销未成交余量；已成交、已撤销或不存在的订单返回 `UNKNOWN_ORDER`。

### 撮合规则

- 买单匹配最低卖价，卖单匹配最高买价；同价位先到者优先（价格时间优先）。
- 限价单不得越过自身价格；成交价取被动单（maker）价格。
- 限价单余量入簿；市价单余量取消；撤单不产生成交。
- 成交编号从 1 开始连续递增。

### 每个事件的输出

```json
{"input_line": "…", "event_id": "e1", "result": "FILLED", "trades": [
  {"trade_id": 1, "maker_order_id": "s1", "taker_order_id": "b1", "price": 100, "quantity": 5}
], "bids": [], "asks": []}
```

- `input_line`：去除行终止符后的原始输入文本。
- `event_id`：可取得时为字符串，否则为 `null`。
- `result`：
  - 限价单：`FILLED`、`RESTING`、`PARTIALLY_FILLED_RESTING`
  - 市价单：`FILLED`、`PARTIALLY_FILLED_CANCELLED`、`UNFILLED_CANCELLED`
  - 撤单成功：`CANCELLED`
  - 拒绝：`REJECTED`（附加 `reason`）
- `trades`：按发生顺序排列；每笔含 `maker_order_id`、`taker_order_id`、`price`、`quantity` 与 `trade_id`。
- `bids` 按价格降序、`asks` 按价格升序，每档含整数 `price` 与汇总 `quantity`。

拒绝原因：`INVALID_JSON`、`INVALID_SCHEMA`（非对象、缺字段、字段类型或枚举错误、未知字段）、`DUPLICATE_EVENT_ID`、`DUPLICATE_ORDER_ID`、`UNKNOWN_ORDER`。拒绝对象带空 `trades` 和拒绝前盘口，不改变订单簿、成交编号或后续优先级。

## 现有公开接口

- 命令行程序 `order-book-engine`（`version`、`replay`）
- Python 包 `order_book_engine`，其 `__version__` 为当前版本号

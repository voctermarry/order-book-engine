## 用途

本项目是「限价订单簿撮合与执行分析平台」的代码仓库，用于逐步实现该方向的撮合、执行与风险分析能力。

当前支持单标的订单事件回放与价格时间优先撮合。

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
order-book-engine replay     # 从标准输入回放 JSON Lines 订单事件
```

## replay 输入输出约定

`replay` 从标准输入读取 UTF-8 JSON Lines，逐行向标准输出一个 JSON 对象，
不读写任何文件；相同输入的输出逐字节一致。

空行忽略。每个非空行按顺序作为一个事件，字段如下：

- ADD：`event_id`（全流唯一字符串）、`type="ADD"`、`order_id`（唯一字符串）、
  `side`（`BUY`/`SELL`）、`order_type`（`LIMIT`/`MARKET`）、正整数 `quantity`；
  LIMIT 另含正整数 `price`，MARKET 不得带非空 `price`（缺省或 null 均可）。
- CANCEL：`event_id`、`type="CANCEL"`、`order_id`，撤销未成交余量。

撮合规则：买单匹配最低卖价、卖单匹配最高买价，同价先到先得；限价单不越过
自身价格，成交价取被动单（maker）价格。限价余量入簿，市价余量取消，撤单
不产生成交。

每行输出对象包含：`input_line`、可取得的 `event_id`（取不到为 null）、
`result`、按发生顺序排列的 `trades`、处理后的完整聚合盘口 `order_book`
（`bids` 按价格降序、`asks` 升序，每档含整数 `price` 与汇总 `quantity`）。
成交含 `maker_order_id`、`taker_order_id`、`price`、`quantity`，按发生顺序
排列；内部成交计数从 1 连续递增，且拒绝事件不会推进该计数。

结果取值：

- 限价单：`FILLED` / `RESTING` / `PARTIALLY_FILLED_RESTING`
- 市价单：`FILLED` / `PARTIALLY_FILLED_CANCELLED` / `UNFILLED_CANCELLED`
- 成功撤单：`CANCELLED`
- 拒绝：`result="REJECTED"`，并带 `rejection_reason`：
  - `INVALID_JSON`：JSON 无法解析
  - `INVALID_SCHEMA`：非对象、缺字段、字段类型或枚举错误、未知字段
  - `DUPLICATE_EVENT_ID`：event_id 重复
  - `DUPLICATE_ORDER_ID`：order_id 重复
  - `UNKNOWN_ORDER`：撤销不存在、已成交或已撤销的订单

拒绝对象带空 `trades` 与拒绝前盘口，不改变订单簿、成交编号或后续优先级。

退出码：正常完成返回 0；标准输入读取或标准输出写入失败返回 1，并向标准
错误输出单行 `ERROR_IO`。

## 现有公开接口

- 命令行程序 `order-book-engine`（`version` 与 `replay` 子命令）
- Python 包 `order_book_engine`，其 `__version__` 为当前版本号
- 撮合逻辑位于 `order_book_engine.engine`（`OrderBook`、`validate_event`）

## 限制

- 单标的、内存态撮合，不持久化任何数据。
- 无运行时第三方依赖。

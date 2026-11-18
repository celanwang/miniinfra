# MiniDB

MiniDB 是独立运行的 Go 关系型数据库。当前实现验证短事务的并发写入、语句级快照、同步提交和原盘恢复。 [设计](docs/plan/design.md)

## 启动

```sh
mkdir -p /tmp/minidb-data
go run ./cmd/minidbd -listen 127.0.0.1:7432 -data /tmp/minidb-data
```

Go 应用通过 `github.com/celanwang/miniinfra/minidb/client` 接入。`client.Open("127.0.0.1:7432")` 返回 `*sql.DB`。用 `BeginTx` 开启事务、用 `?` 绑定参数、用 `RowsAffected` 判断条件扣减是否成功。任何一项库存不足时回滚整单。

当前 SQL 可以随建表声明主键、唯一键和二级索引。以下订单会按请求号进行唯一键仲裁，并在一次提交中发布订单、扣减记录和明细。

```sql
CREATE TABLE inventory (sku INT PRIMARY KEY, available INT NOT NULL);
CREATE TABLE orders (id INT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE);
CREATE TABLE items (order_id INT, sku INT, quantity INT NOT NULL,
                    PRIMARY KEY (order_id, sku), INDEX by_order (order_id));
INSERT INTO inventory VALUES (1, 10), (2, 20);

BEGIN;
INSERT INTO orders VALUES (42, 'r-42');
UPDATE inventory SET available = available - 2 WHERE sku = 1 AND available >= 2;
UPDATE inventory SET available = available - 3 WHERE sku = 2 AND available >= 3;
INSERT INTO items VALUES (42, 1, 2), (42, 2, 3);
COMMIT;

SELECT o.id, i.sku, i.quantity
FROM orders o JOIN items i ON o.id = i.order_id
WHERE o.request_id = 'r-42';
```

提交连接断开时，驱动返回 `client.CommitUnknown`。重新连接后用原请求号查询或重试，同一个请求号只允许创建一笔已提交的订单。 [重试语义](docs/plan/execution.md#断线与重试)

## 验证

```sh
go test -race ./...
```

测试包含 TCP 回包丢失后强制停止进程与原盘重启。当前服务默认监听回环地址，TCP 协议面向本地和受控测试环境。数据以 WAL 为持久化来源，重启时重建内存中的堆页和索引。 [恢复边界](docs/plan/recovery.md)

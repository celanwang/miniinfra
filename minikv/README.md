# MiniKV

MiniKV 是面向可回源数据的单实例内存键值缓存。已写入的值可能在 TTL 前被淘汰，进程重启后从空缓存开始。客户端在未命中时负责回源。

[设计与容量契约](docs/plan/design.md) · [连接与在途额度](docs/plan/connections.md) · [实验与复跑](docs/plan/capacity.md#实验导航)

## 运行

在本目录运行以下命令。默认监听本地回环地址，服务使用明文 TCP，部署于受信网络。运行参数可用 `go run ./cmd/minikv -help` 查看。

```sh
go run ./cmd/minikv -listen 127.0.0.1:7321
go test -race ./...
```

`-cache-bytes` 和 `-entries` 分别限制条目记账字节数和槽位数，均分到各分片并向下取整。每槽预扣 112 字节估算条目元数据和索引；键的长度和值的底层容量另行记账。网络在途额度单独配置。进程实际内存还包含连接、运行时和 GC 余量，默认最多另外保留 8 MiB 空闲大值接收缓冲。160 MiB 容器下的混合读写实测与软内存限制对照见[宽尺寸混合回放](docs/experiments/write-memory.md#宽尺寸混合回放)。

## TCP 协议

请求头以 LF 结尾，字段由一个空格分隔，包含 LF 最长 128 字节。头后紧跟指定字节数的原始键，再跟原始值，帧体后直接开始下一条请求。键至少一字节，值可以为空。所有长度使用十进制字节数。

| 请求头 | 行为 |
| --- | --- |
| `GET <key-bytes>` | 读取未过期的值。命中返回 `VALUE <value-bytes>\n` 和原始值，未命中返回 `MISS\n`。 |
| `SET <key-bytes> <value-bytes> <ttl-ms或NONE> [NX]` | TTL 为正整数毫秒或显式的 `NONE`。成功返回 `STORED\n`。带 `NX` 时只回填缺失或过期的键，仍有有效值就返回 `EXISTS\n`。 |

`NO_SPACE` 表示在目标分片的回收工作上限内写入失败，本次操作保留旧值和原截止时间。`BUSY` 表示在途额度不足。`TOO_LARGE` 表示对象超限。`BAD_REQUEST` 表示输入不合法。状态响应以 LF 结束。若服务在收完请求帧前因格式、尺寸或入站额度而拒绝请求，回状态后关闭连接。请求被截断或达到连接数上限时，服务直接关闭连接。请求接收和响应发送分别受完整操作的截止时间约束。断开时丢弃不完整的响应，未收到写入结果时将提交结果视为未知。

批量读取一页对象时，客户端可以在同一连接预发少量完整的 `GET` 帧，再按发送顺序读取响应。每个 `VALUE` 头后先读足正文的字节数。服务在该连接上逐帧执行，同连接后续 `GET` 可读到先前写入的值，其他连接仍可在各帧之间写入。完整帧返回的 `EXISTS`、`NO_SPACE` 或出站 `BUSY` 都占一个响应位置，后面的帧可继续执行。入站额度不足时，服务可能回 `BUSY` 后关闭连接，客户端遇到断开时须处理剩余未确认的请求。

用请求数和预计响应字节数限制窗口，并及时读取响应。大值发送阻塞时，同连接的后续小请求也会等。窗口 4 在 512 B 小值回放中缩短了整页时间，页内单条 GET 的延迟有所增加。[浅流水实测](docs/experiments/tcp.md#一页对象的浅流水)

```python
import socket

with socket.create_connection(("127.0.0.1", 7321)) as conn, conn.makefile("rb") as reader:
    key = b"product:42:detail"
    value = b'{"name":"coffee"}'
    conn.sendall(f"SET {len(key)} {len(value)} 10000 NX\n".encode() + key + value)
    print(reader.readline().decode().strip())
    conn.sendall(f"GET {len(key)}\n".encode() + key)
    header = reader.readline().split()
    if header[0] == b"VALUE":
        print(reader.read(int(header[1])).decode())
```

### 连接池复用与重连

默认单实例有 256 个连接槽，空闲连接也占槽。服务准备读取下一帧时开始计算 15 秒读期限，空闲等待、请求头和请求体共用这段时间。响应另有 5 秒写期限。槽满时服务直接关闭新接入的 TCP 连接，连上后仍可能在首个请求遇到 EOF 或连接重置。连接池应限制连接总数，及时关闭闲置连接，并对并发重连限速和退避。

复用时遇到 EOF 或连接重置，丢弃该连接，在新连接上重试读取。未收到完整写入结果时，写可能没有执行，也可能已经生效。可先读取当前值，再按应用规则决定是否重试。读到的只是当前缓存状态，后续写入、到期或淘汰都可能改变它。连接断开本身不会删除已经写入的缓存值。[闲置池恢复验收](docs/experiments/tcp.md#闲置连接池的恢复)

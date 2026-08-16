# Minlabel Rust Server

合作音频标注工具服务器。HTTP 负责传输音频/标注数据/文件列表，WebSocket 负责实时同步所有人的标注情况。

## 功能

- **房间**：创建房间获得 6 位房间码，成员凭码加入；广播、标注、进度全部按房间隔离
- **按需上传**：开房只注册文件元数据（名称/大小），音频字节在成员请求时才由拥有者上传
- **HTTP API**：房间、文件列表、音频上传/下载、标注读写、进度
- **WebSocket**：实时同步
  - 谁正在标注什么（`presence` / `release`）
  - 某人标注完成（`annotated`，含 `is_check` / `lab` / `lab_without_tone` / `raw_text`）
  - 总进度（`progress`）
  - 按需取音频（`request_file` → `file_requested` / `file_ready` / `file_unavailable` / `file_uploaded`）
- **SQLite** 持久化（WAL 模式），旧库自动迁移到 `legacy` 房间

## 运行

```bash
cargo run --release
```

环境变量：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MINLABEL_ADDR` | `0.0.0.0:8080` | 监听地址（默认所有网卡，其他机器才能连） |
| `MINLABEL_DATA_DIR` | `data` | 数据目录（SQLite + 音频文件） |

> **Windows 防火墙**：其他机器连不上时，先确认服务器监听 `0.0.0.0`（`netstat -ano | findstr 8080`），再以管理员运行添加入站规则：
> `netsh advfirewall firewall add rule name="minlabel-server" dir=in action=allow protocol=TCP localport=8080`

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/rooms` | 创建房间，body `{"user": "张三"}`，返回 `{"id": "ABC234"}` |
| GET | `/api/rooms/{room}/files` | 房间文件列表（含 `size`/`uploaded`/`owner`/状态/标注人） |
| POST | `/api/rooms/{room}/files` | 注册文件元数据（不传字节），body `{"user": "张三", "files": [{"name": "a.wav", "size": 1024}]}` |
| POST | `/api/rooms/{room}/files/{id}/audio` | 拥有者按需上传音频（multipart：`user` + `file`） |
| GET | `/api/files/{id}/audio` | 下载音频（未上传返回 404） |
| GET | `/api/annotations/{id}` | 获取标注 |
| PUT | `/api/annotations/{id}` | 保存标注（JSON：`is_check`/`lab`/`lab_without_tone`/`raw_text`/`user`） |
| GET | `/api/progress` | 总进度 `{done, total}` |

## WebSocket

连接：`ws://host:8080/ws?user={用户名}&room={房间码}`（房间不存在返回 404）

客户端 → 服务器：

```json
{"type": "claim", "file_id": 3}
{"type": "release", "file_id": 3}
{"type": "request_file", "file_id": 3}
{"type": "annotate", "file_id": 3, "data": {"is_check": 1, "lab": "...", "lab_without_tone": "...", "raw_text": "..."}}
```

服务器 → 客户端（按房间广播）：

```json
{"type": "presence", "user": "张三", "file_id": 3}
{"type": "release", "user": "张三", "file_id": 3}
{"type": "annotated", "user": "张三", "file_id": 3, "data": {"is_check": 1, "lab": "...", "lab_without_tone": "...", "raw_text": "..."}}
{"type": "progress", "done": 12, "total": 50}
{"type": "file_requested", "file_id": 3}
{"type": "file_uploaded", "file_id": 3}
{"type": "file_ready", "file_id": 3}
{"type": "file_unavailable", "file_id": 3}
```

`request_file` 流程：成员请求 → 服务器通知拥有者 `file_requested` → 拥有者 POST 上传 → 服务器广播 `file_uploaded` → 请求者下载；若字节已在服务器上则直接回 `file_ready`，拥有者不在线则回 `file_unavailable`。

## 构建

GitHub Actions 自动在 Ubuntu / Windows 上编译（`cargo fmt --check` + `clippy -D warnings` + `cargo build --release` + `cargo test`），产物作为 artifact 上传。

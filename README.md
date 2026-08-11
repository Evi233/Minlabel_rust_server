# Minlabel Rust Server

合作音频标注工具服务器。HTTP 负责传输音频/标注数据/文件列表，WebSocket 负责实时同步所有人的标注情况。

## 功能

- **HTTP API**：文件列表、音频上传/下载、标注读写、总进度
- **WebSocket**：实时同步
  - 谁正在标注什么（`presence` / `release`）
  - 某人标注完成（`annotated`，含 `is_check` / `lab` / `lab_without_tone` / `raw_text`）
  - 总进度（`progress`）
- **SQLite** 持久化（WAL 模式）

## 运行

```bash
cargo run --release
```

环境变量：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MINLABEL_ADDR` | `0.0.0.0:8080` | 监听地址 |
| `MINLABEL_DATA_DIR` | `data` | 数据目录（SQLite + 音频文件） |

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/files` | 文件列表（含状态、标注人、正在标注的人） |
| POST | `/api/files` | 上传音频（multipart，字段名 `file`） |
| GET | `/api/files/{id}/audio` | 下载音频 |
| GET | `/api/annotations/{id}` | 获取标注 |
| PUT | `/api/annotations/{id}` | 保存标注（JSON：`is_check`/`lab`/`lab_without_tone`/`raw_text`/`user`） |
| GET | `/api/progress` | 总进度 `{done, total}` |

## WebSocket

连接：`ws://host:8080/ws?user={用户名}`

客户端 → 服务器：

```json
{"type": "claim", "file_id": 3}
{"type": "release", "file_id": 3}
{"type": "annotate", "file_id": 3, "data": {"is_check": 1, "lab": "...", "lab_without_tone": "...", "raw_text": "..."}}
```

服务器 → 客户端（广播）：

```json
{"type": "presence", "user": "张三", "file_id": 3}
{"type": "release", "user": "张三", "file_id": 3}
{"type": "annotated", "user": "张三", "file_id": 3, "data": {"is_check": 1, "lab": "...", "lab_without_tone": "...", "raw_text": "..."}}
{"type": "progress", "done": 12, "total": 50}
```

## 构建

GitHub Actions 自动在 Ubuntu / Windows 上编译（`cargo fmt --check` + `clippy -D warnings` + `cargo build --release`），产物作为 artifact 上传。

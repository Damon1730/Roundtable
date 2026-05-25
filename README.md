# 圆桌会议 (Roundtable)

AI 多角色圆桌讨论系统。给定主题，自动从 211 个 AI 角色中选择 3-10 个最相关角色，并行讨论后生成会议总结。

## 功能

- **自动选角** — DeepSeek 根据主题从角色库中选择 3-10 个角色
- **并行讨论** — DAG 引擎并发执行（concurrency: 3），角色独立发言
- **会议总结** — 所有角色发言后自动生成结论/分歧/行动建议
- **追问** — 支持最多 2 轮追问，角色基于上下文继续讨论
- **容错** — 单角色超时/失败不阻塞整场会议
- **持久化** — SQLite 存储会议历史，断线重连可回看
- **实时推送** — WebSocket 实时推送 thinking/speech/status 事件

## 技术栈

| 层 | 技术 |
|---|------|
| 前端 | React 19 + Vite 7 |
| 后端 | Node.js + ws + better-sqlite3 |
| LLM | DeepSeek（通过 agency-orchestrator） |
| 角色库 | agency-agents-zh（211 个中文 AI 角色） |

## 快速开始

```bash
# 安装依赖
npm install

# 配置环境变量
export DEEPSEEK_API_KEY=your_key_here

# 启动后端
npm start

# 开发模式（前端热更新 + 代理后端）
npm run dev
```

访问 `http://localhost:5173`（dev 模式）或 `http://localhost:3000`（生产模式需自行 serve dist/）。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DEEPSEEK_API_KEY` | — | DeepSeek API 密钥（必填） |
| `DEEPSEEK_MODEL` | `deepseek-chat` | 模型名称 |
| `DEEPSEEK_BASE_URL` | — | 自定义 API 地址 |
| `ROUNDTABLE_PORT` | `3000` | 服务端口 |
| `ROUNDTABLE_MOCK_LLM` | — | 设为 `1` 启用 mock 模式（不消耗 API） |
| `ROLE_TIMEOUT_MS` | `60000` | 单角色超时时间（ms） |
| `DB_PATH` | `roundtable.db` | SQLite 数据库路径 |
| `AGENTS_DIR` | 自动检测 | 角色库目录 |

## 测试

```bash
# 启动 mock 服务
ROUNDTABLE_MOCK_LLM=1 ROLE_TIMEOUT_MS=3000 DB_PATH=roundtable-test.db ROUNDTABLE_PORT=3002 npm start

# 运行 E2E 测试（另一个终端）
TEST_PORT=3002 node test-e2e.js
```

78 项断言覆盖：完整流程、WS 消息格式、超时隔离、3/5/10 角色、mock 模式、持久化、追问轮次限制。

## 项目结构

```
├── server/index.js          # 后端：HTTP + WebSocket + DAG + LLM
├── src/
│   ├── main.jsx             # 前端 React 应用
│   ├── styles.css           # 样式
│   └── useRoundtableSocket.js  # WebSocket hook
├── test-e2e.js              # E2E 测试
├── vite.config.js           # Vite 配置（dev 代理）
└── 会议纪要/                # 项目文档
```

## License

MIT

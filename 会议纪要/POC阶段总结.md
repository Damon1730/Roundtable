# 圆桌会议 — POC 阶段总结

## 项目背景

### 动机

许先生希望实现多 Agent 协作的"圆桌会议"功能：给定一个主题，自动选取多个专业角色（前端、后端、测试等），让它们各自从专业视角发言、交叉讨论，最终汇总结论。

### 核心诉求

1. **实时推送** — 角色逐个发言时前端即时显示，而非等全部完成后一次性返回
2. **透明可见** — 每个角色的思考和发言过程对用户可见
3. **可追问** — 第一轮讨论后支持追问，角色基于上下文继续讨论
4. **持久化** — 会议记录存库，支持历史回看

### 两种实现路径对比

| 维度 | Hermes 内置 delegate_task | 独立服务（本 POC） |
|------|--------------------------|-------------------|
| 实时性 | 差（同步阻塞，全部完成后才返回） | 好（WebSocket 逐条推送） |
| 透明度 | 差（子代理是黑盒） | 好（每步 thinking/speech 事件） |
| 部署 | 零成本 | 需独立 Node 服务 |
| 适用场景 | CLI/WebUI 日常快速讨论 | 产品化、小程序、网页 |

**结论：** 产品化走独立服务路线，基于 agency-orchestrator 的 `executeDAG` + `onStepComplete` 回调。

---

## 技术架构

```
┌─────────────┐     WebSocket      ┌──────────────────┐
│  React 前端  │ ◄──────────────── │  Node.js 服务端   │
│  (Vite)     │ ────────────────► │  (HTTP + WS)     │
└─────────────┘   run_roundtable   │                  │
                                   │  agency-orchestrator
                                   │  (executeDAG)    │
                                   │                  │
                                   │  better-sqlite3  │
                                   │  (WAL mode)      │
                                   └──────────────────┘
```

### 技术栈

- **服务端：** Node.js (ESM) + ws + better-sqlite3 (WAL) + agency-orchestrator
- **前端：** React 19 + Vite 7
- **通信：** WebSocket（实时推送）+ HTTP REST（历史查询、追问触发）
- **存储：** SQLite（meetings + messages 两表）
- **端口：** `ROUNDTABLE_PORT` 环境变量，默认 3000

### 数据模型

```sql
meetings (id TEXT PK, title TEXT, created_at TEXT)
messages (id INTEGER PK, meeting_id TEXT FK, speaker TEXT, content TEXT, created_at TEXT)
```

### WebSocket 消息协议

| 方向 | type | 含义 |
|------|------|------|
| S→C | `thinking` | 角色开始思考 |
| S→C | `speech` | 角色发言完成（含 content） |
| S→C | `role_error` | 角色执行失败 |
| S→C | `done` | 会议/追问轮次结束 |
| C→S | `run_roundtable` | 发起新会议 |
| C→S | `run_followup` | 发起追问 |

---

## POC 任务链（Kanban: roundtable-poc）

| 编号 | 任务 | 负责 | 状态 |
|------|------|------|------|
| T1 | AO Spike 验证：executeDAG + onStepComplete 回调 | backend | done |
| T2 | Node 服务骨架：HTTP + WebSocket + SQLite | backend | done |
| T3 | 跑通 3 角色实时推送 | backend | done |
| T4 | 前端接入：气泡渲染 + loading 态 + 历史回看 | frontend | done |
| T5 | 追问机制 | backend | done |
| T6 | 集成测试与验收 | tester | done |

**全部 6 个任务已完成。**

---

## 关键验证结论

### T1 Spike 结论

- `executeDAG` 的 `onStepComplete` 回调确认为**按步骤即时触发**（非全部结束后统一触发）
- 这保证了实时推送的可行性

### T3 实时推送验证

- 3 角色（前端/后端/测试）串行执行，每个角色完成后立即通过 WebSocket 推送 `speech` 事件
- 前端收到后即时渲染气泡

### T5 追问机制

- 支持最多 2 轮追问（`MAX_FOLLOWUP_ROUNDS = 2`）
- 追问时角色收到完整历史上下文，避免重复
- HTTP POST `/api/meeting/:id/followup` 或 WebSocket `run_followup` 均可触发

### T6 集成测试

- 集成测试脚本 `test-integration.js` 覆盖：健康检查、WebSocket 连接、会议创建、角色发言、追问、超时处理

---

## 当前限制与待解决问题

1. **LLM 未接入** — 当前 `runRole()` 是 echo 模式（直接返回 userMessage），未对接真实 LLM
2. **角色固定 3 个** — `normalizeRoles` 强制要求 roles 数组长度为 3，需改为动态
3. **无鉴权** — 服务无认证机制
4. **无前端部署** — `dist/` 已构建但未配置静态服务
5. **agency-agents 角色库** — 已有中文版 `agency-agents-zh/` 在项目目录，但尚未与服务端集成

---

## 下一步方向（待许先生确认）

1. **接入真实 LLM** — 对接 DeepSeek/Claude API，替换 echo 模式
2. **动态角色选取** — 根据主题从 200+ 角色库自动选角
3. **前端体验优化** — 角色头像、打字机效果、Markdown 渲染
4. **部署方案** — 考虑部署到阿里云东京服务器，张小姐可共用
5. **与 Hermes WebUI 集成** — 作为 WebUI 的一个功能模块，或独立运行

---

## 文件结构

```
E:\Hermes\圆桌会议\
├── server/index.js          # 服务端主文件（HTTP + WS + SQLite + AO）
├── src/
│   ├── main.jsx             # React 入口
│   ├── useRoundtableSocket.js  # WebSocket hook
│   └── styles.css           # 样式
├── scripts/                 # 辅助脚本
├── .roundtable-agents/      # AO 动态生成的角色文件
├── agency-agents-zh/        # 中文角色库
├── dist/                    # Vite 构建产物
├── test-integration.js      # 集成测试
├── roundtable.db            # 生产数据库
├── package.json             # 依赖声明
├── vite.config.js           # Vite 配置
└── index.html               # HTML 入口
```

---

## 时间线

- **2025-05-24** — POC 全部 6 个任务完成
- **2025-05-25** — 尝试通过 WebUI 群聊测试 Agent 间通信（commander → frontend/backend/tester），发现 gateway 未启动导致无法发送消息；前端/后端/测试 Agent 在群聊中未响应，原因待排查

---

*文档生成时间：2025-05-25*

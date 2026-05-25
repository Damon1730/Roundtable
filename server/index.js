import http from 'node:http';
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { basename, dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer, WebSocket } from 'ws';
import Database from 'better-sqlite3';
import { buildDAG, createConnector, executeDAG } from 'agency-orchestrator';

const PORT = Number(process.env.ROUNDTABLE_PORT || 3000);
const DB_PATH = process.env.DB_PATH || 'roundtable.db';
const ROLE_TIMEOUT_MS = Number(process.env.ROLE_TIMEOUT_MS || 60_000);
const ROLE_SELECTION_TIMEOUT_MS = Number(process.env.ROLE_SELECTION_TIMEOUT_MS || 60_000);
const DEEPSEEK_MODEL = process.env.DEEPSEEK_MODEL || 'deepseek-chat';
const AGENTS_DIR = process.env.AGENTS_DIR || resolveAgentsDir();
const MAX_FOLLOWUP_ROUNDS = 2;
const SUMMARY_ROLE = { id: 'summary', name: '会议总结', prompt: '总结会议结论、分歧和行动建议。' };

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

db.exec(`
  CREATE TABLE IF NOT EXISTS meetings (
    id TEXT PRIMARY KEY,
    title TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    meeting_id TEXT NOT NULL,
    speaker TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (meeting_id) REFERENCES meetings(id)
  );
`);

const meetingColumns = db.prepare('PRAGMA table_info(meetings)').all().map((column) => column.name);
if (!meetingColumns.includes('status')) {
  db.exec("ALTER TABLE meetings ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'");
}

const ensureMeeting = db.prepare(`
  INSERT INTO meetings (id, title, status)
  VALUES (@id, @title, @status)
  ON CONFLICT(id) DO NOTHING
`);
const updateMeetingStatus = db.prepare(`
  UPDATE meetings
  SET status = @status
  WHERE id = @id
`);
const insertMessage = db.prepare(`
  INSERT INTO messages (meeting_id, speaker, content)
  VALUES (@meetingId, @speaker, @content)
`);
const insertSpeech = db.transaction((message) => {
  ensureMeeting.run({ id: message.meetingId, title: message.meetingTitle || null, status: 'pending' });
  return insertMessage.run(message);
});
const listMeetings = db.prepare(`
  SELECT
    meetings.id,
    meetings.title,
    meetings.status,
    meetings.created_at AS createdAt,
    COUNT(messages.id) AS messageCount
  FROM meetings
  LEFT JOIN messages ON messages.meeting_id = meetings.id
  GROUP BY meetings.id
  ORDER BY meetings.created_at DESC
`);
const getMeeting = db.prepare(`
  SELECT id, title, status, created_at AS createdAt
  FROM meetings
  WHERE id = ?
`);
const countMessagesByMeeting = db.prepare(`
  SELECT COUNT(*) AS count
  FROM messages
  WHERE meeting_id = ?
`);
const countRoleMessagesByMeeting = db.prepare(`
  SELECT COUNT(*) AS count
  FROM messages
  WHERE meeting_id = ? AND speaker != ?
`);
const listMessagesByMeeting = db.prepare(`
  SELECT id, speaker, content, created_at AS createdAt
  FROM messages
  WHERE meeting_id = ?
  ORDER BY id ASC
`);

const DEFAULT_ROLES = [
  { id: 'frontend', name: '前端', prompt: '从前端体验和实现复杂度角度发言。' },
  { id: 'backend', name: '后端', prompt: '从接口、数据和稳定性角度发言。' },
  { id: 'tester', name: '测试', prompt: '从风险、验收和边界场景角度发言。' },
];

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'GET' && url.pathname === '/health') {
    sendHttpJson(res, 200, { ok: true });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/meetings') {
    sendHttpJson(res, 200, { meetings: listMeetings.all() });
    return;
  }

  if (url.pathname.startsWith('/api/meeting/')) {
    const suffix = url.pathname.slice('/api/meeting/'.length);
    const segments = suffix.split('/');
    const meetingId = decodeURIComponent(segments[0] || '');

    if (segments.length === 2 && segments[1] === 'followup') {
      if (req.method !== 'POST') {
        sendHttpJson(res, 405, { error: 'method_not_allowed' });
        return;
      }

      void handleFollowupRequest(req, res, meetingId);
      return;
    }

    if (req.method !== 'GET') {
      sendHttpJson(res, 405, { error: 'method_not_allowed' });
      return;
    }

    if (segments.length !== 1) {
      sendHttpJson(res, 404, { error: 'not_found' });
      return;
    }

    const meeting = getMeeting.get(meetingId);
    if (!meeting) {
      sendHttpJson(res, 404, { error: 'meeting_not_found' });
      return;
    }

    sendHttpJson(res, 200, {
      meeting,
      messages: listMessagesByMeeting.all(meetingId),
    });
    return;
  }

  sendHttpJson(res, 404, { error: 'not_found' });
});

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  ws.on('message', (data) => {
    void handleWebSocketMessage(ws, data);
  });
});

server.listen(PORT, () => {
  console.log(`server listening on http://localhost:${PORT}`);
  console.log(`websocket listening on ws://localhost:${PORT}/ws`);
});

async function handleWebSocketMessage(ws, data) {
  try {
    const payload = parseJsonMessage(data);

    if (payload.action === 'run_roundtable') {
      await runRoundtable(ws, payload);
      return;
    }

    if (payload.action === 'run_followup') {
      await runFollowup(ws, payload);
      return;
    }

    const speech = parseSpeechPayload(payload);
    const result = storeSpeech(speech);
    sendJson(ws, { type: 'stored', id: result.lastInsertRowid });
  } catch (error) {
    sendJson(ws, {
      type: 'error',
      code: 'bad_message',
      message: error.message,
    });
  }
}

async function handleFollowupRequest(req, res, meetingId) {
  try {
    const meeting = getMeeting.get(meetingId);
    if (!meeting) {
      sendHttpJson(res, 404, { error: 'meeting_not_found' });
      return;
    }

    const payload = await readJsonBody(req);
    const roles = normalizeRoles(payload.roles || DEFAULT_ROLES);
    const question = normalizeRequiredString(payload.question || payload.topic, 'question');
    const meetingTitle = normalizeOptionalString(payload.meetingTitle) || meeting.title || null;
    const messageCount = countRoleMessagesByMeeting.get(meetingId, SUMMARY_ROLE.name).count;

    if (messageCount < roles.length || messageCount % roles.length !== 0) {
      sendHttpJson(res, 409, { error: 'meeting_not_ready' });
      return;
    }

    const currentRound = messageCount / roles.length;
    if (currentRound > MAX_FOLLOWUP_ROUNDS) {
      sendHttpJson(res, 409, { error: 'followup_limit_reached' });
      return;
    }

    sendHttpJson(res, 202, {
      ok: true,
      meetingId,
      round: currentRound + 1,
      message: 'followup started',
    });

    void runFollowup(
      createBroadcaster(),
      {
        meetingId,
        meetingTitle,
        topic: question,
        roles,
        round: currentRound + 1,
      },
    ).catch((error) => {
      broadcastJson({
        type: 'error',
        code: 'followup_failed',
        meetingId,
        message: error.message,
      });
      console.error('followup failed', error);
    });
  } catch (error) {
    sendHttpJson(res, 400, { error: 'bad_request', message: error.message });
  }
}

function createBroadcaster() {
  return {
    send(payload) {
      broadcastJson(payload);
    },
  };
}

function broadcastJson(payload) {
  const data = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  }
}

async function runRoundtable(ws, payload) {
  const meetingId = normalizeRequiredString(payload.meetingId, 'meetingId');
  const meetingTitle = normalizeOptionalString(payload.meetingTitle);
  const topic = normalizeRequiredString(payload.topic, 'topic');
  const roles = Array.isArray(payload.roles) ? normalizeRoles(payload.roles) : await selectRoles(topic);

  if (!Array.isArray(payload.roles)) {
    sendJson(ws, {
      type: 'roles_selected',
      meetingId,
      roles: roles.map(({ category, filename, name, reason }) => ({ category, filename, name, reason })),
    });
  }

  ensureMeeting.run({ id: meetingId, title: meetingTitle, status: 'pending' });

  const workflow = buildRoundtableWorkflow(roles, topic);
  const dag = buildDAG(workflow);
  hydrateDagAgents(dag, [...roles, SUMMARY_ROLE]);
  const connector = createRoundtableConnector(meetingId, meetingTitle, (payload) => sendJson(ws, payload), [...roles, SUMMARY_ROLE]);
  const agentsDir = ensureAgentFiles([...roles, SUMMARY_ROLE]);

  updateMeetingState(meetingId, 'running', (payload) => sendJson(ws, payload));
  const result = await executeDAG(dag, {
    connector,
    agentsDir,
    llmConfig: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: 3,
    inputs: new Map([['topic', topic]]),
    onStepStart: (node) => {
      const role = roleById([...roles, SUMMARY_ROLE], node.step.id);
      sendJson(ws, {
        type: 'thinking',
        meetingId,
        roleId: role.id,
        roleName: role.name,
        content: node.step.id === SUMMARY_ROLE.id ? '会议总结正在整理' : `${role.name} 正在思考`,
      });
    },
    onStepComplete: (node) => {
      const role = roleById([...roles, SUMMARY_ROLE], node.step.id);
      if (node.status === 'failed') {
        sendJson(ws, {
          type: 'role_error',
          meetingId,
          roleId: role.id,
          roleName: role.name,
          message: node.error || 'role execution failed',
        });
      }
    },
  });

  if (meetingCompleted(result)) {
    updateMeetingState(meetingId, 'done', (payload) => sendJson(ws, payload));
    sendJson(ws, { type: 'done', meetingId, status: 'done' });
  } else {
    updateMeetingState(meetingId, 'failed', (payload) => sendJson(ws, payload));
    sendJson(ws, { type: 'done', meetingId, status: 'failed' });
  }
}

async function runFollowup(ws, payload) {
  const meetingId = normalizeRequiredString(payload.meetingId, 'meetingId');
  const meetingTitle = normalizeOptionalString(payload.meetingTitle);
  const topic = normalizeRequiredString(payload.topic, 'topic');
  const roles = normalizeRoles(payload.roles || DEFAULT_ROLES);
  const round = normalizeFollowupRound(payload.round);

  const meeting = getMeeting.get(meetingId);
  if (!meeting) {
    sendJson(ws, { type: 'error', code: 'meeting_not_found', message: 'meeting_not_found' });
    return;
  }

  const messageCount = countRoleMessagesByMeeting.get(meetingId, SUMMARY_ROLE.name).count;
  if (messageCount < roles.length || messageCount % roles.length !== 0) {
    sendJson(ws, { type: 'error', code: 'meeting_not_ready', message: 'meeting_not_ready' });
    return;
  }

  const currentRound = messageCount / roles.length;
  if (currentRound > MAX_FOLLOWUP_ROUNDS) {
    sendJson(ws, { type: 'error', code: 'followup_limit_reached', message: 'followup_limit_reached' });
    return;
  }

  const contextMessages = listMessagesByMeeting.all(meetingId);
  const workflow = buildFollowupWorkflow(roles, topic, contextMessages, round);
  const dag = buildDAG(workflow);
  hydrateDagAgents(dag, [...roles, SUMMARY_ROLE]);
  const emitFn = typeof ws.send === 'function' && ws.readyState !== undefined
    ? (payload) => sendJson(ws, payload)
    : (payload) => ws.send(payload);
  const connector = createRoundtableConnector(meetingId, meetingTitle || meeting.title || null, emitFn, [...roles, SUMMARY_ROLE]);
  const agentsDir = ensureAgentFiles([...roles, SUMMARY_ROLE]);

  updateMeetingState(meetingId, 'running', emitFn);
  const result = await executeDAG(dag, {
    connector,
    agentsDir,
    llmConfig: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: 3,
    inputs: new Map([
      ['topic', topic],
      ['context', formatMeetingContext(contextMessages)],
      ['round', String(round)],
    ]),
    onStepStart: (node) => {
      const role = roleById([...roles, SUMMARY_ROLE], node.step.id);
      emitFn({
        type: 'thinking',
        meetingId,
        roleId: role.id,
        roleName: role.name,
        content: node.step.id === SUMMARY_ROLE.id ? '会议总结正在整理' : `${role.name} 正在追问`,
      });
    },
    onStepComplete: (node) => {
      const role = roleById([...roles, SUMMARY_ROLE], node.step.id);
      if (node.status === 'failed') {
        emitFn({
          type: 'role_error',
          meetingId,
          roleId: role.id,
          roleName: role.name,
          message: node.error || 'role execution failed',
        });
      }
    },
  });

  if (meetingCompleted(result)) {
    updateMeetingState(meetingId, 'done', emitFn);
    emitFn({ type: 'done', meetingId, round, status: 'done' });
  } else {
    updateMeetingState(meetingId, 'failed', emitFn);
    emitFn({ type: 'done', meetingId, round, status: 'failed' });
  }
}

function buildFollowupWorkflow(roles, topic, contextMessages, round) {
  const contextText = formatMeetingContext(contextMessages);
  const roleSteps = roles.map((role) => ({
    id: role.id,
    role: role.id,
    name: role.name,
    task: `请以${role.name}的专业视角，基于以下会议上下文，仅进行第 ${round} 轮追问，最多追问 ${MAX_FOLLOWUP_ROUNDS} 轮。\n\n会议主题：${topic}\n\n上下文：\n${contextText}\n\n要求：提出一个直接针对主题和上下文的新追问，避免重复前面的内容，不要复述你的身份设定。`,
    output: `${role.id}_followup_${round}`,
    depends_on: undefined,
  }));

  return {
    name: 'roundtable-followup',
    agents_dir: '.',
    llm: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: 3,
    steps: [
      ...roleSteps,
      buildSummaryStep(roles, topic, roleSteps, contextText),
    ],
  };
}

function formatMeetingContext(messages) {
  return messages.map((message) => `${message.speaker}: ${message.content}`).join('\n');
}

function meetingCompleted(result) {
  const summary = result.steps.find((step) => step.id === SUMMARY_ROLE.id);
  return summary?.status === 'completed';
}

function updateMeetingState(meetingId, status, emit) {
  updateMeetingStatus.run({ id: meetingId, status });
  emit({ type: 'meeting_status', meetingId, status });
}

function normalizeFollowupRound(value) {
  const round = Number(value || 1);
  if (!Number.isInteger(round) || round < 1 || round > MAX_FOLLOWUP_ROUNDS) {
    throw new Error('round must be 1 or 2');
  }
  return round;
}

function buildRoundtableWorkflow(roles, topic) {
  const roleSteps = roles.map((role) => ({
    id: role.id,
    role: role.id,
    name: role.name,
    task: `请以${role.name}的专业视角，围绕以下主题发表你的观点和建议：\n\n主题：${topic}\n\n要求：直接针对主题给出专业分析，不要复述你的身份设定。`,
    output: `${role.id}_speech`,
    depends_on: undefined,
  }));

  return {
    name: 'roundtable',
    agents_dir: '.',
    llm: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: 3,
    steps: [
      ...roleSteps,
      buildSummaryStep(roles, topic, roleSteps),
    ],
  };
}

function buildSummaryStep(roles, topic, roleSteps, contextText = '') {
  const roleNames = roles.map((role) => role.name).join('、');
  const contextBlock = contextText ? `\n\n既有会议上下文：\n${contextText}` : '';

  return {
    id: SUMMARY_ROLE.id,
    role: SUMMARY_ROLE.id,
    name: SUMMARY_ROLE.name,
    task: `以${SUMMARY_ROLE.name}身份发言：请基于本轮圆桌内容整理会议总结。\n\n会议主题：${topic}${contextBlock}\n\n参与角色：${roleNames}\n\n输出三段：1. 结论 2. 分歧 3. 行动建议。`,
    output: 'meeting_summary',
    depends_on: roleSteps.map((step) => step.id),
    depends_on_mode: 'any_completed',
  };
}

function ensureAgentFiles(roles) {
  const agentsDir = join(process.cwd(), '.roundtable-agents');
  mkdirSync(agentsDir, { recursive: true });

  for (const role of roles) {
    writeFileSync(
      join(agentsDir, `${role.id}.md`),
      role.prompt || `---\nname: ${role.name}\ndescription: Roundtable role\n---\n\n你是${role.name}。`,
      'utf8',
    );
  }

  return agentsDir;
}

function hydrateDagAgents(dag, roles) {
  for (const role of roles) {
    const node = dag.nodes.get(role.id);
    if (node) {
      node.agentName = role.name;
    }
  }
}

function createDeepSeekConnector() {
  if (process.env.ROUNDTABLE_MOCK_LLM === '1') {
    return createMockConnector();
  }

  return createConnector({
    provider: 'deepseek',
    api_key: process.env.DEEPSEEK_API_KEY,
    model: DEEPSEEK_MODEL,
    base_url: process.env.DEEPSEEK_BASE_URL,
  });
}

function createMockConnector() {
  return {
    async chat(systemPrompt, userMessage) {
      if (systemPrompt.includes('[[timeout]]') || userMessage.includes('[[timeout]]')) {
        await new Promise(() => {});
      }
      return {
        content: userMessage,
        usage: {
          input_tokens: userMessage.length,
          output_tokens: userMessage.length,
        },
      };
    },
  };
}

function createRoundtableConnector(meetingId, meetingTitle, emit, roles) {
  const connector = createDeepSeekConnector();

  return {
    async chat(systemPrompt, userMessage, config) {
      const result = await withTimeout(connector.chat(systemPrompt, userMessage, {
        ...config,
        provider: 'deepseek',
        model: DEEPSEEK_MODEL,
        timeout: config.timeout || ROLE_TIMEOUT_MS,
      }), config.timeout || ROLE_TIMEOUT_MS);
      const content = result.content;
      const roleName = currentRoleName(userMessage);
      const role = currentRole(roles, roleName);
      const insertResult = storeSpeech({ meetingId, meetingTitle, speaker: roleName, content });
      const messageType = role.id === SUMMARY_ROLE.id ? 'summary' : 'speech';

      emit({
        type: 'speech',
        messageType,
        meetingId,
        roleId: role.id,
        roleName,
        content,
        id: insertResult.lastInsertRowid,
      });

      return result;
    },
  };
}

function currentRoleName(userMessage) {
  const match = userMessage.match(/以(.+?)(?:身份发言|的专业视角)/);
  return match ? match[1] : '角色';
}

function currentRole(roles, roleName) {
  return roles.find((role) => role.name === roleName) || { id: null };
}

function storeSpeech(message) {
  return insertSpeech({
    meetingId: message.meetingId,
    meetingTitle: message.meetingTitle || null,
    speaker: message.speaker,
    content: message.content,
  });
}

function roleById(roles, id) {
  return roles.find((role) => role.id === id) || { id, name: id };
}

async function selectRoles(topic) {
  const catalog = loadRoleCatalog();
  if (catalog.length === 0) {
    return DEFAULT_ROLES;
  }

  if (process.env.ROUNDTABLE_MOCK_LLM === '1') {
    return catalog.slice(0, 3).map((role) => ({
      id: slugify(`${role.category}-${role.filename.replace(/\.md$/i, '')}`),
      category: role.category,
      filename: role.filename,
      name: role.name,
      reason: 'mock role selection',
      prompt: readFileSync(role.path, 'utf8'),
    }));
  }

  const systemPrompt = `你是圆桌会议选角器。根据用户主题，从候选角色中选择3-10个最适合参与讨论的角色。只返回JSON数组，不要Markdown，不要解释。数组元素格式必须是：{"category":"类别","filename":"文件名.md","name":"角色名","reason":"选择理由"}`;
  const userMessage = `用户主题：${topic}\n\n候选角色：\n${catalog.map((role) => `- ${role.category}/${role.filename}: ${role.name} — ${role.description}`).join('\n')}`;
  const connector = createDeepSeekConnector();
  const result = await connector.chat(systemPrompt, userMessage, {
    provider: 'deepseek',
    model: DEEPSEEK_MODEL,
    timeout: ROLE_SELECTION_TIMEOUT_MS,
    max_tokens: 2048,
  });

  const selected = parseSelectedRoles(result.content);
  const roleByKey = new Map(catalog.map((role) => [`${role.category}/${role.filename}`, role]));
  const roles = [];
  const seen = new Set();

  for (const item of selected) {
    const category = normalizeOptionalString(item.category);
    const filename = normalizeOptionalString(item.filename);
    if (!category || !filename) {
      continue;
    }

    const key = `${category}/${filename}`;
    if (seen.has(key)) {
      continue;
    }

    const catalogRole = roleByKey.get(key);
    if (!catalogRole) {
      continue;
    }

    seen.add(key);
    roles.push({
      id: slugify(`${category}-${filename.replace(/\.md$/i, '')}`),
      category,
      filename,
      name: catalogRole.name,
      reason: normalizeOptionalString(item.reason),
      prompt: readFileSync(catalogRole.path, 'utf8'),
    });
  }

  if (roles.length < 3 || roles.length > 10) {
    throw new Error('DeepSeek role selection must return 3 to 10 valid roles');
  }

  return roles;
}

function loadRoleCatalog() {
  const listPath = join(AGENTS_DIR, 'AGENT-LIST.md');
  if (existsSync(listPath)) {
    const content = readFileSync(listPath, 'utf8');
    const rows = [];
    for (const line of content.split('\n')) {
      const match = line.match(/^\| `([^`]+)` \| ([^|]+) \| ([^|]+) \|/);
      if (!match) {
        continue;
      }
      const filename = `${match[1]}.md`;
      const path = findRoleFile(AGENTS_DIR, filename);
      if (!path) {
        continue;
      }
      rows.push({
        category: relative(AGENTS_DIR, dirname(path)).replace(/\\/g, '/'),
        filename,
        name: match[2].trim(),
        description: match[3].trim(),
        path,
      });
    }
    return rows;
  }

  return listMarkdownFiles(AGENTS_DIR).map((path) => {
    const content = readFileSync(path, 'utf8');
    const name = content.match(/^name:\s*(.+)$/m)?.[1]?.trim() || basename(path, '.md');
    const description = content.match(/^description:\s*(.+)$/m)?.[1]?.trim() || '';
    return {
      category: relative(AGENTS_DIR, dirname(path)).replace(/\\/g, '/'),
      filename: basename(path),
      name,
      description,
      path,
    };
  });
}

function findRoleFile(root, filename) {
  const target = filename.replace(/\.md$/i, '');
  return listMarkdownFiles(root).find((path) => basename(path, '.md') === target || basename(path) === filename) || null;
}

function listMarkdownFiles(root) {
  const result = [];
  if (!existsSync(root)) {
    return result;
  }

  const entries = readdirSync(root, { withFileTypes: true });
  for (const entry of entries) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      if (entry.name.startsWith('.')) {
        continue;
      }
      result.push(...listMarkdownFiles(path));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith('.md')) {
      result.push(path);
    }
  }
  return result;
}

function parseSelectedRoles(content) {
  const text = content.trim().replace(/^```(?:json)?\s*/i, '').replace(/```$/i, '').trim();
  const start = text.indexOf('[');
  const end = text.lastIndexOf(']');
  if (start === -1 || end === -1 || end <= start) {
    throw new Error('DeepSeek role selection did not return a JSON array');
  }
  const value = JSON.parse(text.slice(start, end + 1));
  if (!Array.isArray(value)) {
    throw new Error('DeepSeek role selection must be a JSON array');
  }
  return value;
}

function resolveAgentsDir() {
  const local = join(process.cwd(), 'agency-agents');
  if (existsSync(local)) {
    return local;
  }

  const localZh = join(process.cwd(), 'agency-agents-zh');
  if (existsSync(join(localZh, 'AGENT-LIST.md'))) {
    return localZh;
  }

  return join(dirname(fileURLToPath(import.meta.url)), '..', 'node_modules', 'agency-agents-zh');
}

function slugify(value) {
  const slug = String(value).toLowerCase().replace(/\.md$/i, '').replace(/[^a-z0-9_-]+/g, '-').replace(/^-+|-+$/g, '');
  return slug || `role_${Date.now()}`;
}

function normalizeRoles(value) {
  if (!Array.isArray(value) || value.length < 2 || value.length > 10) {
    throw new Error('roles must be an array of 2 to 10 roles');
  }

  return value.map((role, index) => {
    if (!role || typeof role !== 'object') {
      throw new Error(`roles[${index}] must be an object`);
    }

    const name = normalizeRequiredString(role.name, `roles[${index}].name`);
    const filename = normalizeOptionalString(role.filename);
    const category = normalizeOptionalString(role.category);
    const id = normalizeOptionalString(role.id) || slugify(filename || name || `role_${index + 1}`);

    return {
      id,
      category,
      filename,
      name,
      reason: normalizeOptionalString(role.reason),
      prompt: normalizeOptionalString(role.prompt),
    };
  });
}

function parseJsonMessage(data) {
  const raw = data.toString('utf8');
  const payload = JSON.parse(raw);

  if (!payload || typeof payload !== 'object') {
    throw new Error('message must be a JSON object');
  }

  return payload;
}

function parseSpeechPayload(payload) {
  return {
    meetingId: normalizeRequiredString(payload.meetingId, 'meetingId'),
    speaker: normalizeRequiredString(payload.speaker, 'speaker'),
    content: normalizeRequiredString(payload.content, 'content'),
    meetingTitle: normalizeOptionalString(payload.meetingTitle),
  };
}

function normalizeRequiredString(value, name) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`${name} is required`);
  }
  return value.trim();
}

function normalizeOptionalString(value) {
  if (typeof value !== 'string' || value.trim() === '') {
    return null;
  }
  return value.trim();
}

function sendHttpJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'content-type': 'application/json' });
  res.end(JSON.stringify(payload));
}

function sendJson(ws, payload) {
  if (ws.readyState !== WebSocket.OPEN) {
    return;
  }
  ws.send(JSON.stringify(payload));
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => {
      body += chunk;
      if (body.length > 64_000) {
        reject(new Error('body_too_large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (error) {
        reject(error);
      }
    });
    req.on('error', reject);
  });
}

function withTimeout(promise, timeoutMs) {
  if (timeoutMs === 0) {
    return promise;
  }

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('role timeout')), timeoutMs);

    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

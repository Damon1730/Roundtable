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
const ROUNDTABLE_CONCURRENCY = Number(process.env.ROUNDTABLE_CONCURRENCY || 3);
const DISCUSSION_DELAY_MS = Number(process.env.ROUNDTABLE_DISCUSSION_DELAY_MS || 900);
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
    stage TEXT,
    round INTEGER,
    role_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (meeting_id) REFERENCES meetings(id)
  );

  CREATE TABLE IF NOT EXISTS meeting_roles (
    meeting_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    name TEXT NOT NULL,
    prompt TEXT NOT NULL,
    category TEXT,
    filename TEXT,
    reason TEXT,
    position INTEGER NOT NULL,
    PRIMARY KEY (meeting_id, role_id),
    FOREIGN KEY (meeting_id) REFERENCES meetings(id)
  );
`);

const meetingColumns = db.prepare('PRAGMA table_info(meetings)').all().map((column) => column.name);
if (!meetingColumns.includes('status')) {
  db.exec("ALTER TABLE meetings ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'");
}

const messageColumns = db.prepare('PRAGMA table_info(messages)').all().map((column) => column.name);
if (!messageColumns.includes('stage')) {
  db.exec('ALTER TABLE messages ADD COLUMN stage TEXT');
}
if (!messageColumns.includes('round')) {
  db.exec('ALTER TABLE messages ADD COLUMN round INTEGER');
}
if (!messageColumns.includes('role_id')) {
  db.exec('ALTER TABLE messages ADD COLUMN role_id TEXT');
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
  INSERT INTO messages (meeting_id, speaker, content, stage, round, role_id)
  VALUES (@meetingId, @speaker, @content, @stage, @round, @roleId)
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
  SELECT id, speaker, content, stage, round, role_id AS roleId, created_at AS createdAt
  FROM messages
  WHERE meeting_id = ?
  ORDER BY id ASC
`);
// 追问轮次的唯一真相：数据库里 stage='followup' 的最大 round。
// 取代旧的 messageCount % roles.length 反推（单角色失败会卡死追问）。
const maxFollowupRound = db.prepare(`
  SELECT COALESCE(MAX(round), 0) AS round
  FROM messages
  WHERE meeting_id = ? AND stage = 'followup'
`);
// 初始讨论是否已产出过任意发言（追问就绪判断，不要求消息数整除角色数）。
const countDiscussionSpeeches = db.prepare(`
  SELECT COUNT(*) AS count
  FROM messages
  WHERE meeting_id = ? AND speaker != ? AND (stage IS NULL OR stage != 'followup')
`);
const deleteMeetingRoles = db.prepare(`
  DELETE FROM meeting_roles
  WHERE meeting_id = ?
`);
const deleteMeetingMessages = db.prepare(`
  DELETE FROM messages
  WHERE meeting_id = ?
`);
const deleteMeetingRow = db.prepare(`
  DELETE FROM meetings
  WHERE id = ?
`);
const deleteMeeting = db.transaction((meetingId) => {
  deleteMeetingRoles.run(meetingId);
  deleteMeetingMessages.run(meetingId);
  return deleteMeetingRow.run(meetingId);
});
const upsertMeetingRole = db.prepare(`
  INSERT INTO meeting_roles (meeting_id, role_id, name, prompt, category, filename, reason, position)
  VALUES (@meetingId, @id, @name, @prompt, @category, @filename, @reason, @position)
  ON CONFLICT(meeting_id, role_id) DO UPDATE SET
    name = excluded.name,
    prompt = excluded.prompt,
    category = excluded.category,
    filename = excluded.filename,
    reason = excluded.reason,
    position = excluded.position
`);
const listMeetingRoles = db.prepare(`
  SELECT role_id AS id, name, prompt, category, filename, reason
  FROM meeting_roles
  WHERE meeting_id = ?
  ORDER BY position ASC
`);
const saveMeetingRoles = db.transaction((meetingId, roles) => {
  roles.forEach((role, index) => {
    upsertMeetingRole.run({
      meetingId,
      id: role.id,
      name: role.name,
      prompt: role.prompt || '',
      category: role.category || null,
      filename: role.filename || null,
      reason: role.reason || null,
      position: index,
    });
  });
});

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

    if (req.method === 'DELETE' && segments.length === 1) {
      const result = deleteMeeting(meetingId);
      if (result.changes === 0) {
        sendHttpJson(res, 404, { error: 'meeting_not_found' });
        return;
      }
      sendHttpJson(res, 200, { ok: true, meetingId });
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
    const roles = resolveFollowupRoles(meetingId, payload.roles);
    const question = normalizeRequiredString(payload.question || payload.topic, 'question');
    const meetingTitle = normalizeOptionalString(payload.meetingTitle) || meeting.title || null;

    // 与 runFollowup 共用同一份数据库权威的就绪/轮次判断。
    const readiness = evaluateFollowupReadiness(meetingId);
    if (!readiness.ready) {
      sendHttpJson(res, 409, { error: readiness.reason });
      return;
    }

    sendHttpJson(res, 202, {
      ok: true,
      meetingId,
      round: readiness.nextRound,
      message: 'followup started',
    });

    void runFollowup(
      createBroadcaster(),
      {
        meetingId,
        meetingTitle,
        topic: question,
        roles,
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
  const emit = (payload) => sendJson(ws, payload);

  if (!Array.isArray(payload.roles)) {
    emit({
      type: 'roles_selected',
      meetingId,
      roles: roles.map(({ category, filename, name, reason }) => ({ category, filename, name, reason })),
    });
  }

  ensureMeeting.run({ id: meetingId, title: meetingTitle, status: 'pending' });
  saveMeetingRoles(meetingId, roles);
  // stageMeta 在各 stage 之间串行切换；stage 内并发调用共享同一份，安全。
  let stageMeta = { stage: 'discussion', round: 1 };
  const connector = createRoundtableConnector(meetingId, meetingTitle, emit, [...roles, SUMMARY_ROLE], () => stageMeta);
  const agentsDir = ensureAgentFiles([...roles, SUMMARY_ROLE]);
  const runStage = (workflow, stageRoles = roles) => {
    const dag = buildDAG(workflow);
    hydrateDagAgents(dag, [...stageRoles, SUMMARY_ROLE]);
    return executeDAG(dag, {
      connector,
      agentsDir,
      llmConfig: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
      concurrency: ROUNDTABLE_CONCURRENCY,
      inputs: new Map([['topic', topic]]),
      onStepStart: (node) => {
        const role = roleById([...stageRoles, SUMMARY_ROLE], nodeRoleId(node));
        emit({
          type: 'thinking',
          meetingId,
          roleId: role.id,
          roleName: role.name,
          content: nodeRoleId(node) === SUMMARY_ROLE.id ? '会议总结正在整理' : `${role.name} 正在思考`,
        });
      },
      onStepComplete: (node) => {
        const role = roleById([...stageRoles, SUMMARY_ROLE], nodeRoleId(node));
        if (node.status === 'failed') {
          emit({
            type: 'role_error',
            meetingId,
            roleId: role.id,
            roleName: role.name,
            message: node.error || 'role execution failed',
          });
        }
      },
    });
  };

  updateMeetingState(meetingId, 'running', emit);
  stageMeta = { stage: 'discussion', round: 1 };
  await runStage(buildRoundtableWorkflow(roles, topic));
  const firstRoundMessages = listRoundMessages(meetingId);
  if (firstRoundMessages.length > 1) {
    const responseRoles = roles.filter((role) => firstRoundMessages.some((message) => message.speaker === role.name));
    stageMeta = { stage: 'discussion', round: 2 };
    await runStage(buildResponseWorkflow(responseRoles, topic, firstRoundMessages), responseRoles);
  }
  stageMeta = { stage: 'summary', round: null };
  const result = await runStage(buildSummaryWorkflow(roles, topic, listRoundMessages(meetingId)), roles);

  if (meetingCompleted(result)) {
    updateMeetingState(meetingId, 'done', emit);
    emit({ type: 'done', meetingId, status: 'done' });
  } else {
    updateMeetingState(meetingId, 'failed', emit);
    emit({ type: 'done', meetingId, status: 'failed' });
  }
}

async function runFollowup(ws, payload) {
  const meetingId = normalizeRequiredString(payload.meetingId, 'meetingId');
  const meetingTitle = normalizeOptionalString(payload.meetingTitle);
  const topic = normalizeRequiredString(payload.topic, 'topic');
  const roles = resolveFollowupRoles(meetingId, payload.roles);

  const meeting = getMeeting.get(meetingId);
  if (!meeting) {
    sendJson(ws, { type: 'error', code: 'meeting_not_found', message: 'meeting_not_found' });
    return;
  }

  // 轮次由数据库权威决定，不信任 payload.round。
  const readiness = evaluateFollowupReadiness(meetingId);
  if (!readiness.ready) {
    sendJson(ws, { type: 'error', code: readiness.reason, message: readiness.reason });
    return;
  }
  const round = readiness.nextRound;

  const contextMessages = listMessagesByMeeting.all(meetingId);
  const workflow = buildFollowupWorkflow(roles, topic, contextMessages, round);
  const dag = buildDAG(workflow);
  hydrateDagAgents(dag, [...roles, SUMMARY_ROLE]);
  const emitFn = typeof ws.send === 'function' && ws.readyState !== undefined
    ? (payload) => sendJson(ws, payload)
    : (payload) => ws.send(payload);
  const connector = createRoundtableConnector(
    meetingId,
    meetingTitle || meeting.title || null,
    emitFn,
    [...roles, SUMMARY_ROLE],
    () => ({ stage: 'followup', round }),
  );
  const agentsDir = ensureAgentFiles([...roles, SUMMARY_ROLE]);

  updateMeetingState(meetingId, 'running', emitFn);
  const result = await executeDAG(dag, {
    connector,
    agentsDir,
    llmConfig: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: ROUNDTABLE_CONCURRENCY,
    inputs: new Map([
      ['topic', topic],
      ['context', formatMeetingContext(contextMessages)],
      ['round', String(round)],
    ]),
    onStepStart: (node) => {
      const role = roleById([...roles, SUMMARY_ROLE], nodeRoleId(node));
      emitFn({
        type: 'thinking',
        meetingId,
        roleId: role.id,
        roleName: role.name,
        content: nodeRoleId(node) === SUMMARY_ROLE.id ? '会议总结正在整理' : `${role.name} 正在追问`,
      });
    },
    onStepComplete: (node) => {
      const role = roleById([...roles, SUMMARY_ROLE], nodeRoleId(node));
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
  const focus = topicFocusInstruction(topic);
  const roleSteps = roles.map((role) => ({
    id: role.id,
    role: role.id,
    name: role.name,
    task: `请以${role.name}的专业视角，基于以下会议上下文回答第 ${round} 轮追问。\n\n本次唯一议题：${topic}\n\n上下文：\n${contextText}\n\n${focus}\n\n要求：第一句必须直接回应本次唯一议题；像圆桌会议中的一次短发言，控制在 120-220 字；先回应前面观点，再给出一个新的判断或补充；不要复述身份设定；不要编造与议题无关的公司数据、审计数据或案例。`,
    output: `${role.id}_followup_${round}`,
    depends_on: undefined,
  }));

  return {
    name: 'roundtable-followup',
    agents_dir: '.',
    llm: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: ROUNDTABLE_CONCURRENCY,
    steps: [
      ...roleSteps,
      buildSummaryStep(roles, topic, roleSteps, contextText),
    ],
  };
}

function formatMeetingContext(messages) {
  return messages.map((message) => `${message.speaker}: ${message.content}`).join('\n');
}

function listRoundMessages(meetingId) {
  return listMessagesByMeeting.all(meetingId).filter((message) => message.speaker !== SUMMARY_ROLE.name);
}

function formatRoundTranscript(messages) {
  return messages.map((message) => `${message.speaker}: ${message.content}`).join('\n\n');
}

function meetingCompleted(result) {
  const summary = result.steps.find((step) => step.id === SUMMARY_ROLE.id);
  return summary?.status === 'completed';
}

function updateMeetingState(meetingId, status, emit) {
  updateMeetingStatus.run({ id: meetingId, status });
  emit({ type: 'meeting_status', meetingId, status });
}

// 追问就绪与轮次的唯一真相来自数据库，不再用 messageCount % roles.length 反推
// （单角色第一轮失败会让消息数除不尽，旧逻辑会永久卡死追问）。
// ready：初始讨论已产出过任意发言即可。
// nextRound：已有 followup 最大轮次 + 1；超过上限则 limit_reached。
function evaluateFollowupReadiness(meetingId) {
  const discussionSpeeches = countDiscussionSpeeches.get(meetingId, SUMMARY_ROLE.name).count;
  if (discussionSpeeches < 1) {
    return { ready: false, reason: 'meeting_not_ready' };
  }
  const usedRounds = maxFollowupRound.get(meetingId).round;
  if (usedRounds >= MAX_FOLLOWUP_ROUNDS) {
    return { ready: false, reason: 'followup_limit_reached' };
  }
  return { ready: true, nextRound: usedRounds + 1 };
}

function nodeRoleId(node) {
  return node.step.role || node.step.id;
}

function buildRoundtableWorkflow(roles, topic) {
  const focus = topicFocusInstruction(topic);
  const roleSteps = roles.map((role) => ({
    id: role.id,
    role: role.id,
    name: role.name,
    task: `请以${role.name}的专业视角，围绕以下主题做第一轮短发言。\n\n本次唯一议题：${topic}\n\n${focus}\n\n要求：第一句必须直接回答本次唯一议题；像真实会议发言，不要写成报告；控制在 180-320 字；结构为「核心判断」「理由」「想追问/提醒其他角色的一点」；不要复述身份设定；不要编造与议题无关的公司数据、审计数据或案例。`,
    output: `${role.id}_speech`,
    depends_on: undefined,
  }));

  return {
    name: 'roundtable',
    agents_dir: '.',
    llm: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: ROUNDTABLE_CONCURRENCY,
    steps: roleSteps,
  };
}

function buildResponseWorkflow(roles, topic, firstRoundMessages) {
  const transcript = formatRoundTranscript(firstRoundMessages);
  const focus = topicFocusInstruction(topic);
  return {
    name: 'roundtable-response',
    agents_dir: '.',
    llm: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: ROUNDTABLE_CONCURRENCY,
    steps: roles.map((role) => ({
      id: `${role.id}_response`,
      role: role.id,
      name: role.name,
      task: `请以${role.name}的专业视角，基于第一轮所有角色发言做第二轮回应。\n\n本次唯一议题：${topic}\n\n第一轮发言：\n${transcript}\n\n${focus}\n\n要求：第一句必须继续扣住本次唯一议题；像会议中的追问或补充，控制在 100-180 字；必须点名回应至少一个其他角色的观点；避免重复自己的第一轮内容；不要复述身份设定；不要编造与议题无关的公司数据、审计数据或案例。`,
      output: `${role.id}_response`,
      depends_on: undefined,
    })),
  };
}

function buildSummaryWorkflow(roles, topic, roundMessages) {
  const roleNames = roles.map((role) => role.name).join('、');
  const transcript = formatRoundTranscript(roundMessages);
  return {
    name: 'roundtable-summary',
    agents_dir: '.',
    llm: { provider: 'deepseek', model: DEEPSEEK_MODEL, timeout: ROLE_TIMEOUT_MS, retry: 0 },
    concurrency: 1,
    steps: [{
      id: SUMMARY_ROLE.id,
      role: SUMMARY_ROLE.id,
      name: SUMMARY_ROLE.name,
      task: `以${SUMMARY_ROLE.name}身份发言：请基于完整圆桌内容整理会议总结。\n\n会议主题：${topic}\n\n参与角色：${roleNames}\n\n圆桌发言：\n${transcript}\n\n要求：控制在 300-500 字，输出三段：1. 结论 2. 关键分歧 3. 下一步行动建议。`,
      output: 'meeting_summary',
      depends_on: undefined,
    }],
  };
}

function topicFocusInstruction(topic) {
  const text = String(topic || '');
  if (/裁员|劳动|员工|雇佣|解雇|离职|赔偿|社保|公司|合规|法律|合同|风险/.test(text)) {
    return '议题约束：这是一道劳动用工/裁员合规问题。发言必须围绕劳动合同解除、经济补偿或赔偿、裁员程序、沟通方案、证据留痕、社保工资结算、合规审批和争议风险展开。不要转去讨论产品、营销、采购、审计、增长或技术实现。';
  }
  return '议题约束：只讨论用户给出的主题，不要转去讨论角色设定、通用方法论或与主题无关的案例。';
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
      const roleName = currentRoleName(userMessage);
      const topic = extractTopic(userMessage);
      const content = roleName === SUMMARY_ROLE.name
        ? createMockSummary(topic)
        : createMockSpeech(roleName, topic, userMessage);
      return {
        content,
        usage: {
          input_tokens: userMessage.length,
          output_tokens: content.length,
        },
      };
    },
  };
}

function extractTopic(userMessage) {
  const match = userMessage.match(/(?:本次唯一议题|主题|会议主题)：(.+)/);
  return match ? match[1].trim() : '当前议题';
}

function createMockSpeech(roleName, topic, userMessage) {
  const isResponse = userMessage.includes('第二轮回应') || userMessage.includes('第一轮发言：');
  if (isResponse) {
    return `我先回应前面几位的观点：围绕「${topic}」，这个问题不能只看单点动作，必须把风险、成本和执行节奏放在一起评估。我的补充是先设定清晰边界，再选择阻力最小、后患最少的路径。`;
  }
  return `核心判断：围绕「${topic}」，${roleName}建议先把目标拆成可执行、可验证、可留痕的方案。\n\n理由：这类问题通常牵涉成本、风险、沟通和后续影响。如果流程不清楚，短期看似省事，后面可能放大代价。\n\n想提醒其他角色的一点：请重点补充风险边界和替代方案。`;
}

function createMockSummary(topic) {
  return `1. 结论\n围绕「${topic}」，本轮圆桌更倾向于先做结构化评估，再推进低风险动作，而不是直接一步到位。\n\n2. 关键分歧\n分歧主要在执行速度和风险控制之间：一方关注尽快达成目标，另一方关注流程、合规和组织影响。\n\n3. 下一步行动建议\n先列出可替代方案、关键风险点和必要留痕材料，再决定具体执行路径。`;
}

function createRoundtableConnector(meetingId, meetingTitle, emit, roles, getStageMeta) {
  const connector = createDeepSeekConnector();
  const resolveStageMeta = typeof getStageMeta === 'function'
    ? getStageMeta
    : () => ({ stage: 'discussion', round: null });

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
      const isSummary = role.id === SUMMARY_ROLE.id;
      // 总结独立成段；其余发言落到 resolveStageMeta() 给出的 discussion/followup 阶段与轮次。
      const meta = isSummary ? { stage: 'summary', round: null } : resolveStageMeta();
      const insertResult = storeSpeech({
        meetingId,
        meetingTitle,
        speaker: roleName,
        content,
        stage: meta.stage,
        round: meta.round,
        roleId: role.id,
      });
      const messageType = isSummary ? 'summary' : 'speech';

      emit({
        type: 'speech',
        messageType,
        meetingId,
        roleId: role.id,
        roleName,
        content,
        stage: meta.stage,
        round: meta.round,
        id: insertResult.lastInsertRowid,
        playbackDelayMs: isSummary ? DISCUSSION_DELAY_MS * 2 : DISCUSSION_DELAY_MS,
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
    stage: message.stage || null,
    round: message.round ?? null,
    roleId: message.roleId || null,
  });
}

function roleById(roles, id) {
  return roles.find((role) => role.id === id) || { id, name: id };
}

function resolveFollowupRoles(meetingId, payloadRoles) {
  if (Array.isArray(payloadRoles)) {
    return normalizeRoles(payloadRoles);
  }

  const savedRoles = listMeetingRoles.all(meetingId);
  if (savedRoles.length > 0) {
    return savedRoles.map((role) => ({
      ...role,
      category: role.category || undefined,
      filename: role.filename || undefined,
      reason: role.reason || undefined,
    }));
  }

  throw new Error('meeting_roles_not_found');
}

async function selectRoles(topic) {
  const catalog = loadRoleCatalog();
  if (catalog.length === 0) {
    return DEFAULT_ROLES;
  }

  if (process.env.ROUNDTABLE_MOCK_LLM === '1') {
    return selectMockRoles(catalog, topic).map((role) => ({
      id: slugify(`${role.category}-${role.filename.replace(/\.md$/i, '')}`),
      category: role.category,
      filename: role.filename,
      name: role.name,
      reason: 'mock keyword role selection',
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

  let selected = [];
  try {
    selected = parseSelectedRoles(result.content);
  } catch {
    selected = [];
  }
  const roles = materializeSelectedRoles(selected, catalog);

  const guardedRoles = guardSelectedRoles(catalog, topic, roles);
  if (guardedRoles.length >= 3 && guardedRoles.length <= 10) {
    return guardedRoles;
  }

  return selectMockRoles(catalog, topic).map((role) => ({
    id: slugify(`${role.category}-${role.filename.replace(/\.md$/i, '')}`),
    category: role.category,
    filename: role.filename,
    name: role.name,
    reason: 'keyword fallback role selection',
    prompt: readFileSync(role.path, 'utf8'),
  }));
}

function guardSelectedRoles(catalog, topic, roles) {
  const text = String(topic || '');
  if (!/裁员|劳动|员工|雇佣|解雇|离职|赔偿|社保|公司|合规|法律|合同|风险/.test(text)) {
    return roles;
  }

  const relevant = roles.filter((role) => /法务|法律|合同|HR|招聘|绩效|风险|财务/.test(
    `${role.name}${role.category || ''}${role.filename || ''}`,
  ));
  if (relevant.length >= 3) {
    return roles;
  }

  return selectMockRoles(catalog, topic).map((role) => ({
    id: slugify(`${role.category}-${role.filename.replace(/\.md$/i, '')}`),
    category: role.category,
    filename: role.filename,
    name: role.name,
    reason: 'topic guard role selection',
    prompt: readFileSync(role.path, 'utf8'),
  }));
}

function materializeSelectedRoles(selected, catalog) {
  const roles = [];
  const seen = new Set();

  for (const item of selected) {
    const catalogRole = findCatalogRole(catalog, item);
    if (!catalogRole || seen.has(catalogRole.path)) {
      continue;
    }

    seen.add(catalogRole.path);
    roles.push({
      id: slugify(`${catalogRole.category}-${catalogRole.filename.replace(/\.md$/i, '')}`),
      category: catalogRole.category,
      filename: catalogRole.filename,
      name: catalogRole.name,
      reason: normalizeOptionalString(item.reason),
      prompt: readFileSync(catalogRole.path, 'utf8'),
    });
  }

  return roles;
}

function findCatalogRole(catalog, item) {
  const category = normalizeOptionalString(item.category);
  const filename = normalizeOptionalString(item.filename);
  const name = normalizeOptionalString(item.name);
  const id = normalizeOptionalString(item.id || item.agentId || item.agent_id);
  const candidates = [filename, id].filter(Boolean).map((value) => value.replace(/\.md$/i, ''));

  if (category && filename) {
    const exactKey = `${category}/${filename}`;
    const role = catalog.find((entry) => `${entry.category}/${entry.filename}` === exactKey);
    if (role) {
      return role;
    }
  }

  for (const candidate of candidates) {
    const role = catalog.find((entry) => (
      entry.filename.replace(/\.md$/i, '') === candidate
      || basename(entry.filename, '.md') === candidate
    ));
    if (role) {
      return role;
    }
  }

  if (name) {
    return catalog.find((entry) => entry.name === name)
      || catalog.find((entry) => entry.name.includes(name) || name.includes(entry.name));
  }

  return null;
}

function selectMockRoles(catalog, topic) {
  const text = String(topic || '');
  const keywordGroups = [
    {
      test: /裁员|劳动|员工|雇佣|解雇|离职|赔偿|社保|公司|合规|法律|合同|风险/,
      keywords: ['法务合规员', '法律文书审查专家', '合同审查专家', 'HR 入职管理专家', '招聘运营专家', '企业风险评估师', '财务分析师', '财务追踪员'],
    },
    {
      test: /微信|小程序|移动端|H5|前端|后端|技术|架构|部署/,
      keywords: ['微信小程序开发者', '前端开发者', '后端架构师', '安全工程师', 'UX 架构师', 'UI 设计师', '合同审查专家'],
    },
    {
      test: /融资|投资|商业|增长|市场|收入|成本|财务/,
      keywords: ['投资研究员', '财务分析师', '增长黑客', '市场研究员', '企业风险评估师', '财务预测分析师'],
    },
  ];

  const matchedGroup = keywordGroups.find((group) => group.test.test(text));
  const preferred = matchedGroup?.keywords || ['企业风险评估师', '财务分析师', '法务合规员'];
  const selected = [];
  const seen = new Set();

  for (const keyword of preferred) {
    const role = findPreferredRole(catalog, keyword);
    if (role && !seen.has(role.path)) {
      selected.push(role);
      seen.add(role.path);
    }
    if (selected.length >= 5) {
      break;
    }
  }

  for (const role of catalog) {
    if (selected.length >= 3) {
      break;
    }
    if (!seen.has(role.path)) {
      selected.push(role);
      seen.add(role.path);
    }
  }

  return selected.slice(0, 5);
}

function findPreferredRole(catalog, keyword) {
  const normalizedKeyword = keyword.replace(/\s+/g, '').toLowerCase();
  return catalog.find((item) => item.name.replace(/\s+/g, '').includes(normalizedKeyword))
    || catalog.find((item) => item.filename.replace(/\.md$/i, '').toLowerCase().includes(normalizedKeyword))
    || catalog.find((item) => item.description.replace(/\s+/g, '').includes(normalizedKeyword))
    || catalog.find((item) => item.name.includes(keyword) || item.description.includes(keyword) || item.filename.includes(keyword));
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
  if (existsSync(join(local, 'AGENT-LIST.md'))) {
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

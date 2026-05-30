/**
 * T11 端到端集成测试（Mock LLM模式）
 * 
 * 验收标准：
 * 1. 完整流程：发起会议→多角色讨论→总结→追问
 * 2. WS消息顺序和格式验证
 * 3. 单角色超时不影响整场
 * 4. 3/5/10角色场景
 * 5. mock模式跑CI（不消耗API）
 * 
 * 启动服务：
 *   ROUNDTABLE_MOCK_LLM=1 ROLE_TIMEOUT_MS=3000 DB_PATH=roundtable-e2e.db ROUNDTABLE_PORT=3002 node server/index.js
 * 运行测试：
 *   TEST_PORT=3002 node test-e2e.js
 */

import WebSocket from 'ws';

const PORT = process.env.TEST_PORT || 3002;
const BASE = `http://localhost:${PORT}`;
const WS_URL = `ws://localhost:${PORT}/ws`;

let passed = 0;
let failed = 0;
const results = [];

function assert(condition, label) {
  if (condition) {
    passed++;
    results.push(`  PASS: ${label}`);
  } else {
    failed++;
    results.push(`  FAIL: ${label}`);
  }
}

function connectWs() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL);
    ws.on('open', () => resolve(ws));
    ws.on('error', reject);
  });
}

function collectEvents(ws, until, timeout = 60000) {
  return new Promise((resolve) => {
    const events = [];
    const timer = setTimeout(() => { ws.off('message', handler); resolve(events); }, timeout);
    function handler(data) {
      const event = JSON.parse(data.toString());
      events.push({ ...event, _ts: Date.now() });
      if (until(event, events)) {
        clearTimeout(timer);
        ws.off('message', handler);
        resolve(events);
      }
    }
    ws.on('message', handler);
  });
}

async function fetchJson(path, opts) {
  const res = await fetch(`${BASE}${path}`, opts);
  return { status: res.status, body: await res.json() };
}

// ============================================================
// TEST 1: 完整流程 - 3角色讨论 + 总结 + 追问
// ============================================================
async function test1_fullFlow() {
  console.log('\n[TEST 1] 完整流程：发起→讨论→总结→追问');
  const ws = await connectWs();
  const meetingId = `e2e-flow-${Date.now()}`;
  const roles = [
    { id: 'r1', name: '角色A', prompt: '发言A' },
    { id: 'r2', name: '角色B', prompt: '发言B' },
    { id: 'r3', name: '角色C', prompt: '发言C' },
  ];

  // Phase 1: 发起会议
  const eventsPromise = collectEvents(ws, (e) => e.type === 'done', 30000);
  ws.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId,
    meetingTitle: 'E2E完整流程测试',
    topic: '测试主题',
    roles,
  }));
  const events = await eventsPromise;

  // 预期事件序列：meeting_status(running) → 两轮角色speech → thinking(summary), speech(summary) → meeting_status(done) → done
  const typeSequence = events.map((e) => e.type);
  
  // 验证关键事件存在
  assert(typeSequence.includes('meeting_status'), '有meeting_status事件');
  assert(typeSequence.filter((t) => t === 'thinking').length >= 3, '至少3个thinking事件');
  assert(typeSequence.filter((t) => t === 'speech').length >= 7, '至少7个speech事件（两轮角色+总结）');
  assert(typeSequence[typeSequence.length - 1] === 'done', '最后一个事件是done');

  const done = events.find((e) => e.type === 'done');
  assert(done.status === 'done', 'done.status=done');

  // Phase 2: 追问
  const followupPromise = collectEvents(ws, (e) => e.type === 'done', 30000);
  ws.send(JSON.stringify({
    action: 'run_followup',
    meetingId,
    meetingTitle: 'E2E完整流程测试',
    topic: '追问问题',
    roles,
    round: 1,
  }));
  const followupEvents = await followupPromise;
  ws.close();

  const followupSpeeches = followupEvents.filter((e) => e.type === 'speech');
  assert(followupSpeeches.length >= 3, `追问产出>=3条speech (got ${followupSpeeches.length})`);
  const followupDone = followupEvents.find((e) => e.type === 'done');
  assert(!!followupDone, '追问有done事件');
  assert(followupDone?.round === 1, 'done.round=1');
}

// ============================================================
// TEST 2: WS消息格式严格校验
// ============================================================
async function test2_wsMessageFormat() {
  console.log('\n[TEST 2] WS消息顺序和格式验证');
  const ws = await connectWs();
  const meetingId = `e2e-format-${Date.now()}`;
  const roles = [
    { id: 'frontend', name: '前端', prompt: '前端发言' },
    { id: 'backend', name: '后端', prompt: '后端发言' },
    { id: 'tester', name: '测试', prompt: '测试发言' },
  ];

  const eventsPromise = collectEvents(ws, (e) => e.type === 'done', 30000);
  ws.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId,
    meetingTitle: '格式测试',
    topic: '格式验证',
    roles,
  }));
  const events = await eventsPromise;
  ws.close();

  // meeting_status 事件格式
  const statusEvents = events.filter((e) => e.type === 'meeting_status');
  assert(statusEvents.length >= 1, `有meeting_status事件 (got ${statusEvents.length})`);
  if (statusEvents.length > 0) {
    assert(statusEvents[0].meetingId === meetingId, 'meeting_status.meetingId正确');
    assert(statusEvents[0].status === 'running', 'meeting_status首个为running');
  }

  // thinking 事件格式校验
  const thinkings = events.filter((e) => e.type === 'thinking');
  for (const t of thinkings) {
    assert(typeof t.meetingId === 'string' && t.meetingId === meetingId, `thinking.meetingId正确`);
    assert(typeof t.roleId === 'string', `thinking.roleId存在: ${t.roleId}`);
    assert(typeof t.roleName === 'string', `thinking.roleName存在: ${t.roleName}`);
    assert(typeof t.content === 'string', `thinking.content存在`);
  }

  // speech 事件格式校验（排除总结）
  const speeches = events.filter((e) => e.type === 'speech');
  const roleSpeeches = speeches.filter((s) => s.roleId !== 'summary');
  for (const s of roleSpeeches) {
    assert(s.meetingId === meetingId, `speech.meetingId正确`);
    assert(typeof s.roleId === 'string', `speech.roleId存在: ${s.roleId}`);
    assert(typeof s.roleName === 'string', `speech.roleName存在: ${s.roleName}`);
    assert(typeof s.content === 'string' && s.content.length > 0, `speech.content非空`);
    assert(typeof s.id === 'number' || typeof s.id === 'bigint', `speech.id为数字`);
  }

  // done 事件格式
  const done = events.find((e) => e.type === 'done');
  assert(done.meetingId === meetingId, `done.meetingId正确`);
  assert(done.status === 'done' || done.status === 'failed', `done.status有效: ${done.status}`);

  // 顺序校验：角色speech按roles数组顺序出现
  const speechRoleIds = roleSpeeches.map((s) => s.roleId);
  assert(
    JSON.stringify(speechRoleIds) === JSON.stringify(['frontend', 'backend', 'tester', 'frontend', 'backend', 'tester']),
    `speech顺序正确: ${JSON.stringify(speechRoleIds)}`
  );

  // thinking和speech对应：每条speech前都出现过同角色thinking
  for (let i = 0; i < roleSpeeches.length; i++) {
    const speechIdx = events.indexOf(roleSpeeches[i]);
    const priorThinking = events.slice(0, speechIdx).some((event) => (
      event.type === 'thinking' && event.roleId === roleSpeeches[i].roleId
    ));
    assert(
      priorThinking,
      `speech[${i}]前出现过同角色thinking`
    );
  }
}

// ============================================================
// TEST 3: 单角色超时不阻塞（ROLE_TIMEOUT_MS=3000）
// ============================================================
async function test3_timeoutNoBlock() {
  console.log('\n[TEST 3] 单角色超时不影响整场');
  const ws = await connectWs();
  const meetingId = `e2e-timeout-${Date.now()}`;

  // 角色B的prompt含[[timeout]]，触发无限挂起 → withTimeout 3s后reject
  const eventsPromise = collectEvents(
    ws,
    (e) => e.type === 'done',
    15000
  );
  ws.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId,
    meetingTitle: '超时测试',
    topic: '超时验证',
    roles: [
      { id: 'r1', name: '角色A', prompt: '正常发言' },
      { id: 'r2', name: '角色B', prompt: '[[timeout]]触发超时' },
      { id: 'r3', name: '角色C', prompt: '正常发言' },
    ],
  }));
  const events = await eventsPromise;
  ws.close();

  const speeches = events.filter((e) => e.type === 'speech');
  const errors = events.filter((e) => e.type === 'role_error');
  const done = events.find((e) => e.type === 'done');

  // 角色A应该正常完成（在B之前）
  assert(speeches.length >= 1, `至少1个角色正常完成speech (got ${speeches.length})`);
  
  // 角色B应该超时产生role_error
  assert(errors.length >= 1, `超时角色产生role_error (got ${errors.length})`);
  if (errors.length > 0) {
    assert(errors[0].roleId === 'r2', `超时的是角色B: ${errors[0].roleId}`);
  }
  
  // 整个流程应该在超时时间内完成（不卡死）
  assert(!!done, '会议最终完成（done事件存在）');
  
  // 总结步骤使用depends_on_mode: 'any_completed'，所以即使B失败，总结仍应执行
  const summaryThinking = events.find((e) => e.type === 'thinking' && e.roleId === 'summary');
  assert(!!summaryThinking, '总结步骤仍然执行（any_completed模式）');

  // 回归：单角色第一轮失败后，该会议仍能成功追问。
  // 旧逻辑用 messageCount % roles.length 反推轮次，B 失败导致消息数除不尽，追问永久卡死。
  const ws2 = await connectWs();
  const followupPromise = collectEvents(ws2, (e) => e.type === 'done' || e.type === 'error', 15000);
  ws2.send(JSON.stringify({
    action: 'run_followup',
    meetingId,
    meetingTitle: '超时测试',
    topic: '失败后追问',
    roles: [
      { id: 'r1', name: '角色A', prompt: '正常发言' },
      { id: 'r2', name: '角色B', prompt: '正常发言' },
      { id: 'r3', name: '角色C', prompt: '正常发言' },
    ],
  }));
  const followupEvents = await followupPromise;
  ws2.close();
  const followupErr = followupEvents.find((e) => e.type === 'error');
  const followupDone = followupEvents.find((e) => e.type === 'done');
  assert(!followupErr, `失败角色的会议追问不被拒绝 (got error: ${followupErr?.code})`);
  assert(!!followupDone && followupDone.status === 'done', '失败角色的会议仍能成功追问');
}

// ============================================================
// TEST 4: 多角色场景 - 3/5/10
// ============================================================
async function test4_multiRoleScenarios() {
  console.log('\n[TEST 4] 3/5/10角色场景');

  // 4a: 3角色
  console.log('  [4a] 3角色');
  const ws3 = await connectWs();
  const meetingId3 = `e2e-3roles-${Date.now()}`;
  const eventsPromise3 = collectEvents(ws3, (e) => e.type === 'done', 30000);
  ws3.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId: meetingId3,
    meetingTitle: '3角色测试',
    topic: '3角色',
    roles: [
      { id: 'a', name: '甲', prompt: '甲发言' },
      { id: 'b', name: '乙', prompt: '乙发言' },
      { id: 'c', name: '丙', prompt: '丙发言' },
    ],
  }));
  const events3 = await eventsPromise3;
  ws3.close();
  const speeches3 = events3.filter((e) => e.type === 'speech' && e.roleId !== 'summary');
  assert(speeches3.length === 6, `3角色产出两轮共6条speech (got ${speeches3.length})`);
  assert(events3.some((e) => e.type === 'done' && e.status === 'done'), '3角色会议正常结束');

  // 4b: 5角色
  console.log('  [4b] 5角色');
  const ws5 = await connectWs();
  const meetingId5 = `e2e-5roles-${Date.now()}`;
  const eventsPromise5 = collectEvents(ws5, (e) => e.type === 'done' || e.type === 'error', 30000);
  ws5.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId: meetingId5,
    meetingTitle: '5角色测试',
    topic: '5角色',
    roles: [
      { id: 'a', name: '甲', prompt: '甲' },
      { id: 'b', name: '乙', prompt: '乙' },
      { id: 'c', name: '丙', prompt: '丙' },
      { id: 'd', name: '丁', prompt: '丁' },
      { id: 'e', name: '戊', prompt: '戊' },
    ],
  }));
  const events5 = await eventsPromise5;
  ws5.close();
  const error5 = events5.find((e) => e.type === 'error');
  if (error5) {
    assert(false, `5角色被拒绝(不应该): ${error5.message}`);
  } else {
    const speeches5 = events5.filter((e) => e.type === 'speech' && e.roleId !== 'summary');
    assert(speeches5.length === 10, `5角色产出两轮共10条speech (got ${speeches5.length})`);
    assert(events5.some((e) => e.type === 'done'), '5角色会议正常结束');
  }

  // 4c: 10角色
  console.log('  [4c] 10角色');
  const ws10 = await connectWs();
  const meetingId10 = `e2e-10roles-${Date.now()}`;
  const eventsPromise10 = collectEvents(ws10, (e) => e.type === 'done' || e.type === 'error', 60000);
  ws10.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId: meetingId10,
    meetingTitle: '10角色测试',
    topic: '10角色',
    roles: Array.from({ length: 10 }, (_, i) => ({
      id: `r${i}`, name: `角色${i}`, prompt: `角色${i}发言`,
    })),
  }));
  const events10 = await eventsPromise10;
  ws10.close();
  const error10 = events10.find((e) => e.type === 'error');
  if (error10) {
    assert(false, `10角色被拒绝(不应该): ${error10.message}`);
  } else {
    const speeches10 = events10.filter((e) => e.type === 'speech' && e.roleId !== 'summary');
    assert(speeches10.length === 20, `10角色产出两轮共20条speech (got ${speeches10.length})`);
    assert(events10.some((e) => e.type === 'done'), '10角色会议正常结束');
  }
}

// ============================================================
// TEST 5: Mock模式验证（CI友好）
// ============================================================
async function test5_mockMode() {
  console.log('\n[TEST 5] Mock模式验证（不消耗API）');
  const ws = await connectWs();
  const meetingId = `e2e-mock-${Date.now()}`;
  const eventsPromise = collectEvents(ws, (e) => e.type === 'done', 30000);
  ws.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId,
    meetingTitle: 'Mock验证',
    topic: 'Mock测试',
    roles: [
      { id: 'r1', name: '角色A', prompt: '特定内容XYZ' },
      { id: 'r2', name: '角色B', prompt: '特定内容ABC' },
      { id: 'r3', name: '角色C', prompt: '特定内容DEF' },
    ],
  }));
  const events = await eventsPromise;
  ws.close();

  const speeches = events.filter((e) => e.type === 'speech' && e.roleId !== 'summary');
  
  assert(speeches.length === 6, `Mock模式产出两轮共6条speech (got ${speeches.length})`);
  if (speeches.length >= 3) {
    assert(speeches.every((s) => s.content.includes('Mock测试')), 'Mock: speech包含讨论主题');
    assert(speeches.every((s) => !s.content.includes('特定内容')), 'Mock: speech不含原始prompt');
    assert(speeches.every((s) => !s.content.includes('请以')), 'Mock: speech不回显任务prompt');
  }

  const legalWs = await connectWs();
  const legalMeetingId = `e2e-legal-roles-${Date.now()}`;
  const legalEventsPromise = collectEvents(legalWs, (e) => e.type === 'roles_selected', 30000);
  legalWs.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId: legalMeetingId,
    meetingTitle: '法律选角验证',
    topic: '公司裁员如何规避劳动法律风险',
  }));
  const legalEvents = await legalEventsPromise;
  legalWs.close();
  const selected = legalEvents.find((e) => e.type === 'roles_selected')?.roles || [];
  const selectedNames = selected.map((role) => role.name).join('、');
  assert(selected.length >= 3, `法律主题自动选出>=3个角色 (got ${selected.length})`);
  assert(!/前端|后端|测试/.test(selectedNames), `法律主题不选择POC固定技术角色: ${selectedNames}`);

  // Health endpoint
  const health = await fetchJson('/health');
  assert(health.status === 200 && health.body.ok === true, 'Health endpoint正常');
}

// ============================================================
// TEST 6: 历史持久化和断线重连
// ============================================================
async function test6_persistenceAndReconnect() {
  console.log('\n[TEST 6] 历史持久化和断线重连');
  const ws = await connectWs();
  const meetingId = `e2e-persist-${Date.now()}`;
  const roles = [
    { id: 'r1', name: '角色A', prompt: '持久化测试A' },
    { id: 'r2', name: '角色B', prompt: '持久化测试B' },
    { id: 'r3', name: '角色C', prompt: '持久化测试C' },
  ];

  const eventsPromise = collectEvents(ws, (e) => e.type === 'done', 30000);
  ws.send(JSON.stringify({
    action: 'run_roundtable',
    meetingId,
    meetingTitle: '持久化测试',
    topic: '持久化',
    roles,
  }));
  await eventsPromise;
  ws.close();

  // 断线后通过HTTP API获取历史
  const detail = await fetchJson(`/api/meeting/${encodeURIComponent(meetingId)}`);
  assert(detail.status === 200, 'GET /api/meeting/:id 200');
  // 3角色两轮 + 1总结 = 7条消息
  assert(detail.body.messages.length >= 7, `历史消息数>=7 (got ${detail.body.messages.length})`);
  assert(detail.body.meeting.title === '持久化测试', '会议标题持久化正确');
  assert(detail.body.meeting.status === 'done', '会议状态为done');

  // 验证消息顺序（前3条是角色发言）
  assert(detail.body.messages[0].speaker === '角色A', '消息[0]为角色A');
  assert(detail.body.messages[1].speaker === '角色B', '消息[1]为角色B');
  assert(detail.body.messages[2].speaker === '角色C', '消息[2]为角色C');

  // 验证列表API
  const list = await fetchJson('/api/meetings');
  assert(list.status === 200, 'GET /api/meetings 200');
  const found = list.body.meetings.find((m) => m.id === meetingId);
  assert(!!found, '会议出现在列表中');
  assert(found.messageCount >= 3, `列表中messageCount>=3 (got ${found?.messageCount})`);
}

// ============================================================
// TEST 7: 追问轮次限制
// ============================================================
async function test7_followupLimit() {
  console.log('\n[TEST 7] 追问轮次限制（MAX_FOLLOWUP_ROUNDS=2）');
  const ws = await connectWs();
  const meetingId = `e2e-limit-${Date.now()}`;
  const roles = [
    { id: 'r1', name: '角色A', prompt: '发言' },
    { id: 'r2', name: '角色B', prompt: '发言' },
    { id: 'r3', name: '角色C', prompt: '发言' },
  ];

  // Round 0: 初始会议
  let eventsPromise = collectEvents(ws, (e) => e.type === 'done', 30000);
  ws.send(JSON.stringify({
    action: 'run_roundtable', meetingId, meetingTitle: '轮次测试', topic: '初始', roles,
  }));
  await eventsPromise;

  // Round 1: 第一轮追问
  eventsPromise = collectEvents(ws, (e) => e.type === 'done' || e.type === 'error', 30000);
  ws.send(JSON.stringify({
    action: 'run_followup', meetingId, meetingTitle: '轮次测试', topic: '追问1', roles, round: 1,
  }));
  const r1Events = await eventsPromise;
  const r1Done = r1Events.find((e) => e.type === 'done');
  assert(!!r1Done, '第1轮追问完成');

  // Round 2: 第二轮追问
  eventsPromise = collectEvents(ws, (e) => e.type === 'done' || e.type === 'error', 30000);
  ws.send(JSON.stringify({
    action: 'run_followup', meetingId, meetingTitle: '轮次测试', topic: '追问2', roles, round: 2,
  }));
  const r2Events = await eventsPromise;
  const r2Done = r2Events.find((e) => e.type === 'done');
  assert(!!r2Done, '第2轮追问完成');

  // Round 3: 超出限制
  eventsPromise = collectEvents(ws, (e) => e.type === 'done' || e.type === 'error', 10000);
  ws.send(JSON.stringify({
    action: 'run_followup', meetingId, meetingTitle: '轮次测试', topic: '追问3', roles, round: 1,
  }));
  const r3Events = await eventsPromise;
  const limitError = r3Events.find((e) => e.type === 'error');
  assert(!!limitError, '超出追问轮次限制被拒绝');
  if (limitError) {
    assert(
      limitError.code === 'followup_limit_reached' || limitError.code === 'meeting_not_ready',
      `错误码合理: ${limitError.code}`
    );
  }

  ws.close();
}

// ============================================================
// MAIN
// ============================================================
async function main() {
  console.log('========================================');
  console.log('  圆桌会议 T11 端到端集成测试');
  console.log('  Mock模式（不消耗API）');
  console.log(`  Target: ${BASE}`);
  console.log('========================================');

  // 等待服务就绪
  let ready = false;
  for (let i = 0; i < 10; i++) {
    try {
      const res = await fetch(`${BASE}/health`);
      if (res.ok) { ready = true; break; }
    } catch {}
    await new Promise((r) => setTimeout(r, 500));
  }
  if (!ready) {
    console.error('FATAL: 服务未就绪，请先启动服务:');
    console.error(`  ROUNDTABLE_MOCK_LLM=1 ROLE_TIMEOUT_MS=3000 DB_PATH=roundtable-e2e.db ROUNDTABLE_PORT=${PORT} node server/index.js`);
    process.exit(2);
  }

  try {
    await test1_fullFlow();
    await test2_wsMessageFormat();
    await test3_timeoutNoBlock();
    await test4_multiRoleScenarios();
    await test5_mockMode();
    await test6_persistenceAndReconnect();
    await test7_followupLimit();
  } catch (err) {
    console.error('\n!!! 测试执行异常:', err.message, err.stack);
    failed++;
    results.push(`  FATAL: ${err.message}`);
  }

  console.log('\n========================================');
  console.log('  T11 测试结果汇总');
  console.log('========================================');
  results.forEach((r) => console.log(r));
  console.log(`\n  总计: ${passed + failed} | 通过: ${passed} | 失败: ${failed}`);
  console.log('========================================');

  process.exitCode = failed > 0 ? 1 : 0;
}

main();

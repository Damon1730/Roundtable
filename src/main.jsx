import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { useRoundtableSocket } from './useRoundtableSocket.js';
import './styles.css';

const ROLE_EMOJIS = ['🧠', '🧩', '🚀'];
const ROLE_COLORS = ['#6d5dfc', '#0ea5e9', '#f97316', '#10b981', '#e11d48', '#8b5cf6'];
const CHAT_PLAYBACK_DELAY_MS = 900;
const TYPEWRITER_INTERVAL_MS = 12;
const TYPEWRITER_CHUNK_SIZE = 3;

function createMeetingId() {
  return `meeting-${Date.now()}`;
}

function App() {
  const [meetingId, setMeetingId] = useState(() => localStorage.getItem('activeMeetingId') || createMeetingId());
  const [meetingTitle, setMeetingTitle] = useState('未命名圆桌');
  const [topic, setTopic] = useState('如何把 AI Agent 工作流产品化？');
  const [messages, setMessages] = useState([]);
  const [thinkingRole, setThinkingRole] = useState(null);
  const [history, setHistory] = useState([]);
  const [isRunning, setIsRunning] = useState(false);
  const [notice, setNotice] = useState('');
  const runLockRef = useRef(false);
  const loadSeqRef = useRef(0);
  const playbackQueueRef = useRef([]);
  const playbackActiveRef = useRef(false);
  const playbackTimersRef = useRef([]);
  const displayedMessageIdsRef = useRef(new Set());
  const serverDoneRef = useRef(false);

  const handleSocketEvent = useCallback((event) => {
    if (event.type === 'thinking') {
      setThinkingRole({ id: event.roleId, name: event.roleName, color: roleColor(event.roleId || event.roleName) });
      return;
    }

    if (event.type === 'roles_selected') {
      setMessages((prev) => [
        ...prev,
        {
          id: `roles-selected-${Date.now()}`,
          speaker: '系统',
          roleId: 'system',
          content: formatSelectedRoles(event.roles),
          type: 'system',
        },
      ]);
      return;
    }

    if (event.type === 'speech') {
      setThinkingRole(null);
      queuePlayback(toMessage(event), event.playbackDelayMs);
      return;
    }

    if (event.type === 'role_error') {
      setThinkingRole(null);
      setMessages((prev) => [...prev, {
        id: `error-${Date.now()}`,
        speaker: event.roleName,
        roleId: event.roleId,
        content: event.message || '角色执行失败',
        type: 'error',
      }]);
      return;
    }

    if (event.type === 'meeting_status') {
      if (event.status === 'running') {
        setIsRunning(true);
      }
      if (event.status === 'done' || event.status === 'failed') {
        setThinkingRole(null);
        serverDoneRef.current = true;
        finishRunIfPlaybackIdle();
        loadHistory();
      }
      return;
    }

    if (event.type === 'done') {
      setThinkingRole(null);
      serverDoneRef.current = true;
      finishRunIfPlaybackIdle();
      loadHistory();
      return;
    }

    if (event.type === 'error') {
      setThinkingRole(null);
      setIsRunning(false);
      runLockRef.current = false;
      setNotice(event.message || '服务端返回错误');
    }
  }, []);

  const { status, send } = useRoundtableSocket({ meetingId, onEvent: handleSocketEvent });

  const disconnected = status === 'closed' || status === 'error';
  const canRun = status === 'open' && !isRunning;

  const activeHistory = useMemo(
    () => history.find((item) => item.id === meetingId),
    [history, meetingId],
  );
  const meetingStatus = activeHistory?.status || 'pending';

  useEffect(() => {
    localStorage.setItem('activeMeetingId', meetingId);
    if (activeHistory && !isRunning && !playbackActiveRef.current && playbackQueueRef.current.length === 0) {
      loadMeeting(meetingId);
    }
  }, [meetingId, activeHistory, isRunning]);

  useEffect(() => {
    loadHistory();
  }, []);

  useEffect(() => () => {
    clearPlayback();
  }, []);

  async function loadHistory() {
    const response = await fetch('/api/meetings');
    if (!response.ok) {
      return;
    }
    const payload = await response.json();
    setHistory(payload.meetings || []);
  }

  async function loadMeeting(id) {
    const seq = loadSeqRef.current + 1;
    loadSeqRef.current = seq;
    const response = await fetch(`/api/meeting/${encodeURIComponent(id)}`);
    if (loadSeqRef.current !== seq) {
      return;
    }

    if (!response.ok) {
      return;
    }

    const payload = await response.json();
    const loadedMessages = (payload.messages || []).map(toMessage);
    displayedMessageIdsRef.current = new Set(loadedMessages.map((message) => message.id).filter(Boolean));
    setMeetingTitle(payload.meeting?.title || '未命名圆桌');
    setMessages(loadedMessages);
  }

  function queuePlayback(message, delayMs = CHAT_PLAYBACK_DELAY_MS) {
    if (message.id && displayedMessageIdsRef.current.has(message.id)) {
      return;
    }
    if (message.id) {
      displayedMessageIdsRef.current.add(message.id);
    }
    playbackQueueRef.current.push({ message, delayMs });
    void drainPlaybackQueue();
  }

  async function drainPlaybackQueue() {
    if (playbackActiveRef.current) {
      return;
    }

    playbackActiveRef.current = true;
    while (playbackQueueRef.current.length) {
      const item = playbackQueueRef.current.shift();
      await playMessage(item.message, item.delayMs);
    }
    playbackActiveRef.current = false;
    finishRunIfPlaybackIdle();
  }

  async function playMessage(message, delayMs = CHAT_PLAYBACK_DELAY_MS) {
    setThinkingRole({
      id: message.roleId,
      name: message.speaker || message.roleName,
      color: message.color,
    });
    await wait(delayMs);
    setThinkingRole(null);

    const animatedId = message.id || `playback-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const fullContent = String(message.content || '');
    const animatedMessage = { ...message, id: animatedId, content: '', isStreaming: true };

    setMessages((prev) => [...prev, animatedMessage]);

    for (let index = 0; index < fullContent.length; index += TYPEWRITER_CHUNK_SIZE) {
      const nextContent = fullContent.slice(0, index + TYPEWRITER_CHUNK_SIZE);
      setMessages((prev) => prev.map((item) => (
        item.id === animatedId ? { ...item, content: nextContent } : item
      )));
      await wait(TYPEWRITER_INTERVAL_MS);
    }

    setMessages((prev) => prev.map((item) => (
      item.id === animatedId ? { ...item, content: fullContent, isStreaming: false } : item
    )));
    await wait(260);
  }

  function wait(ms) {
    return new Promise((resolve) => {
      const timer = window.setTimeout(() => {
        playbackTimersRef.current = playbackTimersRef.current.filter((item) => item !== timer);
        resolve();
      }, ms);
      playbackTimersRef.current.push(timer);
    });
  }

  function clearPlayback() {
    playbackQueueRef.current = [];
    playbackActiveRef.current = false;
    serverDoneRef.current = false;
    displayedMessageIdsRef.current = new Set();
    for (const timer of playbackTimersRef.current) {
      window.clearTimeout(timer);
    }
    playbackTimersRef.current = [];
  }

  function finishRunIfPlaybackIdle() {
    if (!serverDoneRef.current || playbackActiveRef.current || playbackQueueRef.current.length) {
      return;
    }
    setIsRunning(false);
    runLockRef.current = false;
    serverDoneRef.current = false;
  }

  function startMeeting() {
    if (!canRun || runLockRef.current) {
      return;
    }

    const title = topic.trim() || '未命名圆桌';
    runLockRef.current = true;
    const ok = send({
      action: 'run_roundtable',
      meetingId,
      meetingTitle: title,
      topic: title,
    });

    if (!ok) {
      runLockRef.current = false;
      setNotice('WebSocket 未连接，先别猛点，按钮不是解压玩具。');
      return;
    }

    setMeetingTitle(title);
    clearPlayback();
    displayedMessageIdsRef.current = new Set();
    setMessages([]);
    setThinkingRole(null);
    setIsRunning(true);
    setNotice('');
  }

  function createNewMeeting() {
    if (isRunning) {
      return;
    }
    runLockRef.current = false;
    clearPlayback();
    displayedMessageIdsRef.current = new Set();
    setMeetingId(createMeetingId());
    setMeetingTitle('未命名圆桌');
    setMessages([]);
    setThinkingRole(null);
    setNotice('');
  }

  function replayMeeting(id) {
    if (id === meetingId || isRunning) {
      return;
    }
    runLockRef.current = false;
    clearPlayback();
    displayedMessageIdsRef.current = new Set();
    setMeetingId(id);
    setThinkingRole(null);
    setIsRunning(false);
    setNotice('');
  }

  return (
    <main className="app-shell">
      <aside className="sidebar" aria-label="历史会议">
        <div className="sidebar-header">
          <h1>圆桌会议</h1>
          <button onClick={createNewMeeting} disabled={isRunning}>新会议</button>
        </div>
        <div className="history-list">
          {history.length === 0 ? (
            <p className="empty-text">暂无历史，先开一桌。</p>
          ) : history.map((item) => (
            <button
              key={item.id}
              className={item.id === meetingId ? 'history-item active' : 'history-item'}
              onClick={() => replayMeeting(item.id)}
              disabled={isRunning}
            >
              <span>{item.title || item.id}</span>
              <small>{formatMeetingStatus(item.status)} · {item.createdAt}</small>
            </button>
          ))}
        </div>
      </aside>

      <section className="meeting-panel">
        <header className="meeting-header">
          <div>
            <p className="eyebrow">{activeHistory ? '历史回看' : '实时会议'}</p>
            <h2>{meetingTitle}</h2>
          </div>
          <span className={disconnected ? 'status danger' : 'status'}>
            {disconnected ? '连接已断开' : `会议：${formatMeetingStatus(meetingStatus)} · ${status === 'open' ? '已连接' : '连接中'}`}
          </span>
        </header>

        {disconnected && <div className="notice danger">WebSocket 断线了，当前会议不会继续推送。</div>}
        {notice && <div className="notice">{notice}</div>}

        <section className="composer" aria-label="发起圆桌">
          <input
            value={topic}
            onChange={(event) => setTopic(event.target.value)}
            disabled={isRunning}
            placeholder="输入会议主题"
          />
          <button onClick={startMeeting} disabled={!canRun}>
            {isRunning ? '进行中...' : '开始圆桌'}
          </button>
        </section>

        <section className="bubble-list" aria-live="polite">
          {messages.length === 0 && !thinkingRole ? (
            <div className="empty-state">输入主题开会，三位角色会逐条发言。</div>
          ) : messages.map((message, index) => (
            <MessageBubble key={message.id || `${message.speaker}-${index}`} message={message} index={index} />
          ))}
          {thinkingRole && <LoadingBubble role={thinkingRole} />}
        </section>
      </section>
    </main>
  );
}

function MessageBubble({ message, index }) {
  const emoji = message.emoji || ROLE_EMOJIS[index % ROLE_EMOJIS.length];
  const color = message.color || ROLE_COLORS[index % ROLE_COLORS.length];
  const className = [
    'bubble',
    message.type === 'error' ? 'error' : '',
    message.type === 'system' ? 'system' : '',
    message.type === 'summary' || isSummaryMessage(message) ? 'summary' : '',
    message.isStreaming ? 'streaming' : '',
  ].filter(Boolean).join(' ');

  return (
    <article className={className} style={{ '--role-color': color }}>
      <div className="bubble-speaker">
        <span className="role-avatar" aria-hidden="true">{message.type === 'error' ? '⚠️' : emoji}</span>
        <strong>{message.speaker || message.roleName || '角色'}</strong>
      </div>
      {message.type === 'error' ? (
        <div className="error-card">
          <strong>角色执行失败</strong>
          <p>{message.content}</p>
        </div>
      ) : (
        <MarkdownContent content={message.content} />
      )}
    </article>
  );
}

function LoadingBubble({ role }) {
  const roleName = role.name || '角色';
  return (
    <article className="bubble loading active" style={{ '--role-color': role.color || roleColor(role.id || roleName) }}>
      <div className="bubble-speaker">
        <span className="role-avatar thinking" aria-hidden="true">💭</span>
        <strong>{roleName}</strong>
      </div>
      <p>{roleName}正在思考...</p>
      <span className="thinking-dots" aria-hidden="true"><i></i><i></i><i></i></span>
    </article>
  );
}

function formatMeetingStatus(status) {
  return ({ pending: '待开始', running: '进行中', done: '已完成', failed: '失败' })[status] || '待开始';
}

function toMessage(raw) {
  const roleKey = raw.roleId || raw.speaker || raw.roleName;
  return {
    id: raw.id,
    speaker: raw.speaker || raw.roleName,
    roleId: raw.roleId,
    emoji: emojiForRole(roleKey),
    color: roleColor(roleKey),
    content: raw.content,
    createdAt: raw.createdAt,
    type: raw.messageType || raw.type || (isSummaryRole(roleKey) ? 'summary' : undefined),
  };
}

function formatSelectedRoles(roles = []) {
  if (!roles.length) {
    return '已启用自动选角，服务端未返回角色列表。';
  }

  const lines = roles.map((role, index) => {
    const label = role.name || role.filename || `角色${index + 1}`;
    const reason = role.reason ? `：${role.reason}` : '';
    return `- ${label}${reason}`;
  });
  return ['已自动选择本轮圆桌角色：', ...lines].join('\n');
}

function MarkdownContent({ content }) {
  const blocks = parseMarkdown(content || '');
  return (
    <div className="markdown-content">
      {blocks.map((block, index) => renderMarkdownBlock(block, index))}
    </div>
  );
}

function parseMarkdown(content) {
  const lines = String(content).split(/\r?\n/);
  const blocks = [];
  let paragraph = [];
  let codeLines = null;

  function flushParagraph() {
    if (paragraph.length) {
      blocks.push({ type: 'paragraph', text: paragraph.join('\n') });
      paragraph = [];
    }
  }

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];

    if (line.trim().startsWith('```')) {
      if (codeLines) {
        blocks.push({ type: 'code', text: codeLines.join('\n') });
        codeLines = null;
      } else {
        flushParagraph();
        codeLines = [];
      }
      continue;
    }

    if (codeLines) {
      codeLines.push(line);
      continue;
    }

    if (!line.trim()) {
      flushParagraph();
      continue;
    }

    const heading = line.match(/^(#{1,3})\s+(.+)$/);
    if (heading) {
      flushParagraph();
      blocks.push({ type: 'heading', level: heading[1].length, text: heading[2] });
      continue;
    }

    if (/^\s*[-*]\s+/.test(line)) {
      flushParagraph();
      const items = [line.replace(/^\s*[-*]\s+/, '')];
      while (index + 1 < lines.length && /^\s*[-*]\s+/.test(lines[index + 1])) {
        index += 1;
        items.push(lines[index].replace(/^\s*[-*]\s+/, ''));
      }
      blocks.push({ type: 'list', items });
      continue;
    }

    if (line.includes('|') && /^\s*\|?\s*:?-{3,}:?\s*\|/.test(lines[index + 1] || '')) {
      flushParagraph();
      const rows = [splitTableRow(line)];
      index += 1;
      while (index + 1 < lines.length && lines[index + 1].includes('|') && lines[index + 1].trim()) {
        index += 1;
        rows.push(splitTableRow(lines[index]));
      }
      blocks.push({ type: 'table', header: rows[0], rows: rows.slice(1) });
      continue;
    }

    paragraph.push(line);
  }

  if (codeLines) {
    blocks.push({ type: 'code', text: codeLines.join('\n') });
  }
  flushParagraph();
  return blocks.length ? blocks : [{ type: 'paragraph', text: '' }];
}

function splitTableRow(line) {
  return line.trim().replace(/^\|/, '').replace(/\|$/, '').split('|').map((cell) => cell.trim());
}

function renderMarkdownBlock(block, index) {
  if (block.type === 'heading') {
    const Tag = `h${block.level + 2}`;
    return <Tag key={index}>{renderInline(block.text)}</Tag>;
  }

  if (block.type === 'list') {
    return <ul key={index}>{block.items.map((item, itemIndex) => <li key={itemIndex}>{renderInline(item)}</li>)}</ul>;
  }

  if (block.type === 'code') {
    return <pre key={index}><code>{block.text}</code></pre>;
  }

  if (block.type === 'table') {
    return (
      <div className="markdown-table-wrap" key={index}>
        <table>
          <thead><tr>{block.header.map((cell, cellIndex) => <th key={cellIndex}>{renderInline(cell)}</th>)}</tr></thead>
          <tbody>{block.rows.map((row, rowIndex) => <tr key={rowIndex}>{row.map((cell, cellIndex) => <td key={cellIndex}>{renderInline(cell)}</td>)}</tr>)}</tbody>
        </table>
      </div>
    );
  }

  return <p key={index}>{renderInline(block.text)}</p>;
}

function renderInline(text) {
  return String(text).split(/(`[^`]+`)/g).map((part, index) => {
    if (part.startsWith('`') && part.endsWith('`')) {
      return <code key={index}>{part.slice(1, -1)}</code>;
    }
    return part;
  });
}

function isSummaryMessage(message) {
  return isSummaryRole(`${message.roleId || ''} ${message.speaker || ''}`);
}

function isSummaryRole(role) {
  const key = String(role || '').toLowerCase();
  return key.includes('summary') || key.includes('总结');
}

function roleColor(role) {
  const key = String(role || '').toLowerCase();
  if (isSummaryRole(role)) {
    return '#10b981';
  }
  let hash = 0;
  for (let i = 0; i < key.length; i += 1) {
    hash = (hash + key.charCodeAt(i)) % ROLE_COLORS.length;
  }
  return ROLE_COLORS[hash];
}

function emojiForRole(role) {
  if (isSummaryRole(role)) {
    return '🧾';
  }
  return '💬';
}

createRoot(document.getElementById('root')).render(<App />);

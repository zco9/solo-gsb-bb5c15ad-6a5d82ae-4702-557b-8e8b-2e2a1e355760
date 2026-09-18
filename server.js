'use strict';

const path = require('path');
const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
const DEFAULT_TIMEOUT_SEC = 30;
const MAX_LOG_ENTRIES = 500;

fs.mkdirSync(DATA_DIR, { recursive: true });

// ---------------------------------------------------------------------------
// 会话状态（单场次，内存态；事件日志同时落盘到 data/events-<sessionId>.jsonl）
// ---------------------------------------------------------------------------
const state = {
  sessionId: crypto.randomUUID(),
  phase: 'idle', // idle | active | done
  sequence: [], // [{ id, seq, text, targets: [string] }]
  timeoutSec: DEFAULT_TIMEOUT_SEC,
  currentIndex: -1,
  confirmations: {}, // position -> { clientId, at, late }
  timedOut: [], // 超时未确认的岗位
  substitutions: {}, // 当前口令的临时替岗：原岗位 -> { substitute, at }，仅对本条口令生效
  cueStartedAt: null,
  deadline: null,
  log: [], // [{ ts, event, detail }]
};

let timeoutTimer = null;
const logStream = fs.createWriteStream(
  path.join(DATA_DIR, `events-${state.sessionId}.jsonl`),
  { flags: 'a' }
);

function addLog(event, detail = {}) {
  const entry = { ts: new Date().toISOString(), event, detail };
  state.log.push(entry);
  if (state.log.length > MAX_LOG_ENTRIES) state.log.shift();
  logStream.write(JSON.stringify(entry) + '\n');
  return entry;
}

function currentCue() {
  if (state.phase !== 'active') return null;
  return state.sequence[state.currentIndex] || null;
}

// 当前口令的有效目标：被替岗的原岗位由其替补顶替，仅对当前口令生效
function effectiveTargets(cue) {
  if (!cue) return [];
  return cue.targets.map((t) => {
    const sub = state.substitutions[t];
    return sub ? sub.substitute : t;
  });
}

function allConfirmed() {
  const cue = currentCue();
  if (!cue) return false;
  return effectiveTargets(cue).every((t) => state.confirmations[t]);
}

// 断线重连恢复依赖全量状态快照：任何变化都广播完整公开状态
function publicState() {
  return {
    sessionId: state.sessionId,
    phase: state.phase,
    sequence: state.sequence,
    timeoutSec: state.timeoutSec,
    currentIndex: state.currentIndex,
    currentCue: currentCue(),
    confirmations: state.confirmations,
    timedOut: state.timedOut,
    substitutions: state.substitutions,
    effectiveTargets: effectiveTargets(currentCue()),
    cueStartedAt: state.cueStartedAt,
    deadline: state.deadline,
    serverNow: Date.now(),
    onlinePositions: [...onlinePositions],
    log: state.log.slice(-200),
  };
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const onlinePositions = new Set(); // 当前在线的岗位名

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) client.send(msg);
  }
}

function broadcastState() {
  broadcast({ type: 'state', state: publicState() });
}

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

function sendError(ws, message) {
  send(ws, { type: 'error', message });
}

// ---------------------------------------------------------------------------
// 口令推进逻辑
// ---------------------------------------------------------------------------
function clearCueTimer() {
  if (timeoutTimer) {
    clearTimeout(timeoutTimer);
    timeoutTimer = null;
  }
}

function activateCue(index) {
  clearCueTimer();
  state.currentIndex = index;
  state.phase = 'active';
  // 关键：每条口令的确认/超时/替岗状态独立，绝不串入下一条（下一条仍用导演编排的原目标）
  state.confirmations = {};
  state.timedOut = [];
  state.substitutions = {};
  state.cueStartedAt = Date.now();
  state.deadline = state.cueStartedAt + state.timeoutSec * 1000;

  const cue = currentCue();
  addLog('cue-started', { seq: cue.seq, text: cue.text, targets: cue.targets });

  timeoutTimer = setTimeout(() => {
    const cueNow = currentCue();
    if (!cueNow || cueNow.id !== cue.id) return; // 已推进，忽略旧定时器
    const missing = effectiveTargets(cueNow).filter((t) => !state.confirmations[t]);
    if (missing.length === 0) return;
    state.timedOut = missing;
    addLog('cue-timeout', { seq: cueNow.seq, missing });
    broadcastState();
  }, state.timeoutSec * 1000);

  broadcastState();
}

function finishSequence() {
  clearCueTimer();
  state.phase = 'done';
  state.currentIndex = state.sequence.length;
  state.cueStartedAt = null;
  state.deadline = null;
  addLog('sequence-finished', {});
  broadcastState();
}

function handleMessage(ws, msg) {
  let data;
  try {
    data = JSON.parse(msg);
  } catch {
    return sendError(ws, '无法解析的消息');
  }

  switch (data.type) {
    case 'hello': {
      const prev = ws.meta;
      // 同一连接换岗：先移除旧岗位在线记录
      if (prev && prev.role === 'position' && prev.position && prev.position !== data.position) {
        const stillUsed = [...wss.clients].some(
          (c) => c !== ws && c.meta && c.meta.position === prev.position && c.readyState === c.OPEN
        );
        if (!stillUsed) onlinePositions.delete(prev.position);
      }
      ws.meta = {
        role: data.role,
        position: data.position || null,
        clientId: data.clientId || null,
      };
      if (data.role === 'position' && data.position) {
        onlinePositions.add(data.position);
        addLog('position-online', { position: data.position });
        broadcastState();
      } else {
        send(ws, { type: 'state', state: publicState() });
      }
      break;
    }

    case 'setSequence': {
      if (state.phase !== 'idle') {
        return sendError(ws, '演出进行中，请先重置再修改序列');
      }
      const cues = Array.isArray(data.cues) ? data.cues : [];
      const cleaned = cues
        .map((c) => ({
          text: String(c.text || '').trim(),
          targets: [...new Set((Array.isArray(c.targets) ? c.targets : [])
            .map((t) => String(t).trim())
            .filter(Boolean))],
        }))
        .filter((c) => c.text && c.targets.length > 0);
      if (cleaned.length === 0) {
        return sendError(ws, '序列不能为空，每条口令需包含口令内容与至少一个目标岗位');
      }
      state.sequence = cleaned.map((c, i) => ({
        id: crypto.randomUUID(),
        seq: i + 1,
        text: c.text,
        targets: c.targets,
      }));
      const timeoutSec = Number(data.timeoutSec);
      state.timeoutSec =
        Number.isFinite(timeoutSec) && timeoutSec >= 3 && timeoutSec <= 600
          ? Math.round(timeoutSec)
          : DEFAULT_TIMEOUT_SEC;
      state.currentIndex = -1;
      addLog('sequence-set', {
        count: state.sequence.length,
        timeoutSec: state.timeoutSec,
        cues: state.sequence.map((c) => ({ seq: c.seq, text: c.text, targets: c.targets })),
      });
      broadcastState();
      break;
    }

    case 'start': {
      if (state.phase === 'active') return sendError(ws, '口令正在进行中');
      if (state.sequence.length === 0) return sendError(ws, '请先由导演端创建演出序列');
      if (state.phase === 'done') return sendError(ws, '本场已结束，请重置后重新开始');
      addLog('session-start', {});
      activateCue(0);
      break;
    }

    case 'advance': {
      if (state.phase !== 'active') return sendError(ws, '当前没有进行中的口令');
      if (!allConfirmed()) {
        const cue = currentCue();
        const missing = effectiveTargets(cue).filter((t) => !state.confirmations[t]);
        return sendError(ws, `以下岗位尚未确认，禁止推进：${missing.join('、')}`);
      }
      const doneCue = currentCue();
      addLog('cue-completed', { seq: doneCue.seq });
      if (state.currentIndex + 1 >= state.sequence.length) {
        finishSequence();
      } else {
        activateCue(state.currentIndex + 1);
      }
      break;
    }

    case 'reset': {
      clearCueTimer();
      state.phase = 'idle';
      state.currentIndex = -1;
      state.confirmations = {};
      state.timedOut = [];
      state.substitutions = {};
      state.cueStartedAt = null;
      state.deadline = null;
      addLog('session-reset', {});
      broadcastState();
      break;
    }

    case 'confirm': {
      const meta = ws.meta || {};
      if (meta.role !== 'position' || !meta.position) {
        return sendError(ws, '仅岗位端可以确认口令');
      }
      const cue = currentCue();
      if (!cue) return sendError(ws, '当前没有进行中的口令');
      const position = meta.position;
      if (!effectiveTargets(cue).includes(position)) {
        // 原岗位被替岗后，确认权已移交替补，不得再确认本条
        if (cue.targets.includes(position) && state.substitutions[position]) {
          return sendError(ws, `本岗位的确认任务已移交「${state.substitutions[position].substitute}」，无需再确认`);
        }
        return sendError(ws, `本条口令不涉及岗位「${position}」`);
      }
      // 重复确认：实时标记进日志，不改变状态
      if (state.confirmations[position]) {
        addLog('confirm-duplicate', { seq: cue.seq, position });
        broadcastState();
        return;
      }
      const late = state.timedOut.includes(position);
      state.confirmations[position] = {
        clientId: meta.clientId,
        at: Date.now(),
        late,
      };
      if (late) state.timedOut = state.timedOut.filter((t) => t !== position);
      addLog(late ? 'confirm-late' : 'confirm', { seq: cue.seq, position });
      broadcastState();
      break;
    }

    case 'substitute': {
      // 临时替岗：控制端指定一名在线替补，接管当前口令某个未确认目标
      const meta = ws.meta || {};
      if (meta.role !== 'control') {
        return sendError(ws, '仅控制端可以指定临时替岗');
      }
      const cue = currentCue();
      if (!cue) return sendError(ws, '当前没有进行中的口令');
      const from = String(data.from || '').trim();
      const to = String(data.to || '').trim();
      if (!from || !to) return sendError(ws, '请指定被替岗的原岗位与替补岗位');
      if (!cue.targets.includes(from)) {
        return sendError(ws, `「${from}」不是本条口令的目标岗位`);
      }
      if (state.substitutions[from]) {
        return sendError(ws, `「${from}」的确认任务已被「${state.substitutions[from].substitute}」接管`);
      }
      if (state.confirmations[from]) {
        return sendError(ws, `「${from}」已确认，无需替岗`);
      }
      if (!onlinePositions.has(to)) {
        return sendError(ws, `替补岗位「${to}」不在线，无法接管`);
      }
      // 替补不能已是本条有效目标（含原目标、其他替补及 from 本身）
      if (effectiveTargets(cue).includes(to)) {
        return sendError(ws, `「${to}」已是本条口令的有效目标，不能作为替补`);
      }
      // 原子更新：仅当前口令的有效目标与确认权限，下一条口令不受影响
      state.substitutions[from] = { substitute: to, at: Date.now() };
      // 超时未确认标记随确认权一起移交给替补
      if (state.timedOut.includes(from)) {
        state.timedOut = state.timedOut.map((t) => (t === from ? to : t));
      }
      addLog('position-substituted', { seq: cue.seq, from, to });
      broadcastState();
      break;
    }

    default:
      sendError(ws, `未知消息类型：${data.type}`);
  }
}

wss.on('connection', (ws) => {
  ws.on('message', (msg) => handleMessage(ws, msg));
  ws.on('close', () => {
    const meta = ws.meta;
    if (meta && meta.role === 'position' && meta.position) {
      // 同一岗位可能有多个连接，全部断开才算离线
      const stillOnline = [...wss.clients].some(
        (c) => c !== ws && c.meta && c.meta.position === meta.position && c.readyState === c.OPEN
      );
      if (!stillOnline) {
        onlinePositions.delete(meta.position);
        addLog('position-offline', { position: meta.position });
        broadcastState();
      }
    }
  });
  ws.on('error', () => {});
});

server.listen(PORT, () => {
  console.log(`舞台口令同步系统已启动: http://localhost:${PORT}`);
  console.log(`  导演端:   http://localhost:${PORT}/director.html`);
  console.log(`  控制端:   http://localhost:${PORT}/control.html`);
  console.log(`  岗位端:   http://localhost:${PORT}/position.html`);
  console.log(`  事件日志: data/events-${state.sessionId}.jsonl`);
});

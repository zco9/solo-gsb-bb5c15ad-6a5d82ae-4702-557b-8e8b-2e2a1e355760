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
  confirmations: {}, // 目标槽位（原岗位）-> { clientId, at, late, by }（替岗时 by 为替补岗位）
  substitutions: {}, // 当前口令的临时替岗：原岗位 -> { substitute, at }，仅当前口令有效
  timedOut: [], // 超时未确认的岗位（按目标槽位记录）
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

function allConfirmed() {
  const cue = currentCue();
  if (!cue) return false;
  return cue.targets.every((t) => state.confirmations[t]);
}

// 当前口令的有效目标：应用临时替岗后的岗位名，与原 targets 槽位一一对应
function effectiveTargets(cue) {
  return cue.targets.map((t) => (state.substitutions[t] ? state.substitutions[t].substitute : t));
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
    substitutions: state.substitutions,
    timedOut: state.timedOut,
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
  // 关键：每条口令的确认/替岗/超时状态独立，绝不串入下一条（下一条仍使用导演编排的原目标）
  state.confirmations = {};
  state.substitutions = {};
  state.timedOut = [];
  state.cueStartedAt = Date.now();
  state.deadline = state.cueStartedAt + state.timeoutSec * 1000;

  const cue = currentCue();
  addLog('cue-started', { seq: cue.seq, text: cue.text, targets: cue.targets });

  timeoutTimer = setTimeout(() => {
    const cueNow = currentCue();
    if (!cueNow || cueNow.id !== cue.id) return; // 已推进，忽略旧定时器
    const missing = cueNow.targets.filter((t) => !state.confirmations[t]);
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
        const missing = cue.targets.filter((t) => !state.confirmations[t]);
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
      state.substitutions = {};
      state.timedOut = [];
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
      // 确认权属于“有效目标”：被替岗的槽位只能由替补岗位确认
      const slotIdx = effectiveTargets(cue).indexOf(position);
      if (slotIdx === -1) {
        const sub = state.substitutions[position];
        if (cue.targets.includes(position) && sub) {
          return sendError(ws, `岗位「${position}」已由「${sub.substitute}」替岗，无需确认`);
        }
        return sendError(ws, `本条口令不涉及岗位「${position}」`);
      }
      const slot = cue.targets[slotIdx];
      // 重复确认：实时标记进日志，不改变状态
      if (state.confirmations[slot]) {
        addLog('confirm-duplicate', { seq: cue.seq, position });
        broadcastState();
        return;
      }
      const late = state.timedOut.includes(slot);
      state.confirmations[slot] = {
        clientId: meta.clientId,
        at: Date.now(),
        late,
        by: position, // 实际确认岗位（替岗时与原目标不同）
      };
      if (late) state.timedOut = state.timedOut.filter((t) => t !== slot);
      const detail = { seq: cue.seq, position };
      if (slot !== position) detail.for = slot; // 替补代原岗位确认
      addLog(late ? 'confirm-late' : 'confirm', detail);
      broadcastState();
      break;
    }

    // 临时替岗：控制端将某个未确认目标的确认权移交给一名在线岗位。
    // 单条消息同步处理 + 单次状态变更，对本条口令而言是原子生效的。
    case 'substitute': {
      const cue = currentCue();
      if (!cue) return sendError(ws, '当前没有进行中的口令');
      const from = String(data.from || '').trim(); // 原岗位（未确认目标）
      const to = String(data.to || '').trim(); // 替补岗位（在线）
      if (!from || !to) return sendError(ws, '替岗需指定原岗位与替补岗位');
      if (!cue.targets.includes(from)) {
        return sendError(ws, `岗位「${from}」不是本条口令的目标岗位`);
      }
      if (state.confirmations[from]) {
        return sendError(ws, `岗位「${from}」已确认，无需替岗`);
      }
      if (state.substitutions[from]) {
        return sendError(ws, `岗位「${from}」已由「${state.substitutions[from].substitute}」替岗`);
      }
      if (to === from) return sendError(ws, '替补岗位不能与原岗位相同');
      // 替补不能已是本条口令的有效目标（含其他槽位的替补）
      if (effectiveTargets(cue).includes(to)) {
        return sendError(ws, `岗位「${to}」已是本条口令的有效目标，不能作为替补`);
      }
      if (!onlinePositions.has(to)) {
        return sendError(ws, `岗位「${to}」不在线，无法指定替岗`);
      }
      // 仅更新当前口令的有效目标与确认权限；推进/重置时随确认表一起清空
      state.substitutions[from] = { substitute: to, at: Date.now() };
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

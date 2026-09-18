/* 公共前端逻辑：带自动重连的 WebSocket 封装。
 * clientId 存于 localStorage，断线重连后服务端以下发的全量状态快照恢复页面。 */
'use strict';

const CueClient = (() => {
  function getClientId() {
    let id = localStorage.getItem('cueClientId');
    if (!id) {
      id = crypto.randomUUID();
      localStorage.setItem('cueClientId', id);
    }
    return id;
  }

  /**
   * @param {object} opts
   * @param {string} opts.role - 'director' | 'control' | 'position'
   * @param {() => (string|null)} [opts.getPosition] - 岗位端返回岗位名
   * @param {(state: object) => void} opts.onState
   * @param {(message: string) => void} [opts.onError]
   * @param {(connected: boolean) => void} [opts.onConnChange]
   */
  function connect(opts) {
    let ws = null;
    let closed = false;

    function hello() {
      const payload = { type: 'hello', role: opts.role, clientId: getClientId() };
      if (opts.role === 'position' && opts.getPosition) {
        payload.position = opts.getPosition();
      }
      send(payload);
    }

    function open() {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws';
      ws = new WebSocket(`${proto}://${location.host}/ws`);

      ws.onopen = () => {
        if (opts.onConnChange) opts.onConnChange(true);
        hello();
      };
      ws.onmessage = (ev) => {
        let data;
        try { data = JSON.parse(ev.data); } catch { return; }
        if (data.type === 'state') opts.onState(data.state);
        else if (data.type === 'error' && opts.onError) opts.onError(data.message);
      };
      ws.onclose = () => {
        if (opts.onConnChange) opts.onConnChange(false);
        if (!closed) setTimeout(open, 1000); // 断线自动重连
      };
      ws.onerror = () => ws.close();
    }

    function send(payload) {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(payload));
        return true;
      }
      return false;
    }

    open();
    return { send, rehello: hello, close: () => { closed = true; ws && ws.close(); } };
  }

  function fmtTime(isoOrMs) {
    const d = new Date(isoOrMs);
    return d.toLocaleTimeString('zh-CN', { hour12: false });
  }

  const EVENT_LABELS = {
    'sequence-set': '序列已保存',
    'session-start': '演出开始',
    'cue-started': '发出口令',
    'cue-completed': '口令完成',
    'cue-timeout': '确认超时',
    'confirm': '岗位确认',
    'confirm-late': '迟到确认',
    'confirm-duplicate': '重复确认',
    'position-substituted': '临时替岗',
    'sequence-finished': '全部口令完成',
    'session-reset': '场次重置',
    'position-online': '岗位上线',
    'position-offline': '岗位离线',
  };

  function renderLog(log) {
    return log.slice().reverse().map((e) => {
      const label = EVENT_LABELS[e.event] || e.event;
      const detail = Object.entries(e.detail || {})
        .map(([k, v]) => `${k}=${Array.isArray(v) ? v.join('/') : JSON.stringify(v)}`)
        .join(' ');
      const cls = ['cue-timeout', 'confirm-duplicate', 'confirm-late'].includes(e.event)
        ? 'log-warn' : '';
      return `<div class="log-row ${cls}"><span class="log-ts">${fmtTime(e.ts)}</span>` +
        `<span class="log-event">${label}</span><span class="log-detail">${escapeHtml(detail)}</span></div>`;
    }).join('');
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  return { connect, getClientId, fmtTime, renderLog, escapeHtml };
})();

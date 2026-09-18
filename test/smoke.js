// 端到端冒烟测试：模拟导演、控制端、两个岗位端走完整流程
'use strict';
const WebSocket = require('ws');

const URL = 'ws://localhost:3000/ws';
let passed = 0, failed = 0;

function check(name, cond) {
  if (cond) { passed++; console.log(`  ✔ ${name}`); }
  else { failed++; console.log(`  ✘ ${name}`); }
}

function client(role, position) {
  const ws = new WebSocket(URL);
  const c = {
    ws,
    state: null,
    errors: [],
    ready: new Promise((res) => {
      ws.on('open', () => {
        ws.send(JSON.stringify({ type: 'hello', role, position, clientId: 'test-' + role + '-' + (position || '') }));
        res();
      });
    }),
    send(obj) { ws.send(JSON.stringify(obj)); },
    waitState(pred, timeout = 3000) {
      return new Promise((res, rej) => {
        const t0 = Date.now();
        const iv = setInterval(() => {
          if (c.state && pred(c.state)) { clearInterval(iv); res(c.state); }
          else if (Date.now() - t0 > timeout) { clearInterval(iv); rej(new Error('waitState 超时')); }
        }, 20);
      });
    },
  };
  ws.on('message', (m) => {
    const d = JSON.parse(m);
    if (d.type === 'state') c.state = d.state;
    if (d.type === 'error') c.errors.push(d.message);
  });
  return c;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  const director = client('director');
  const control = client('control');
  const light = client('position', '灯光');
  const sound = client('position', '音响');
  const prop = client('position', '道具');
  await Promise.all([director.ready, control.ready, light.ready, sound.ready, prop.ready]);
  await sleep(200);

  console.log('0. 重置场次，确保干净起点');
  control.send({ type: 'reset' });
  await control.waitState((s) => s.phase === 'idle');

  console.log('1. 导演创建序列');
  director.send({ type: 'setSequence', timeoutSec: 3, cues: [
    { text: '灯光渐暗', targets: ['灯光'] },
    { text: '音乐起，道具就位', targets: ['音响', '道具'] },
  ]});
  let st = await control.waitState((s) => s.sequence.length === 2);
  check('序列已保存（2 条）', st.sequence.length === 2);
  check('序号自动编号', st.sequence[0].seq === 1 && st.sequence[1].seq === 2);

  console.log('2. 控制端开始演出');
  control.send({ type: 'start' });
  st = await control.waitState((s) => s.phase === 'active');
  check('第 1 条口令激活', st.currentCue.text === '灯光渐暗');

  console.log('3. 非目标岗位确认应被拒绝');
  sound.send({ type: 'confirm' });
  await sleep(200);
  check('音响确认被拒绝', sound.errors.some((e) => e.includes('不涉及')));
  check('确认状态未串入', Object.keys(st.confirmations || {}).length === 0);

  console.log('4. 目标岗位确认 + 重复确认标记');
  light.send({ type: 'confirm' });
  st = await control.waitState((s) => s.confirmations['灯光']);
  check('灯光确认成功', !!st.confirmations['灯光']);
  light.send({ type: 'confirm' });
  st = await control.waitState((s) => s.log.some((l) => l.event === 'confirm-duplicate'));
  check('重复确认被实时标记进日志', true);

  console.log('5. 全员确认后推进');
  control.send({ type: 'advance' });
  st = await control.waitState((s) => s.currentCue && s.currentCue.seq === 2);
  check('推进到第 2 条', st.currentCue.text.includes('音乐起'));
  check('上一条确认未串入下一条', Object.keys(st.confirmations).length === 0);

  console.log('6. 未全员确认禁止推进');
  sound.send({ type: 'confirm' });
  await control.waitState((s) => s.confirmations['音响']);
  control.send({ type: 'advance' });
  await sleep(200);
  check('推进被拒绝（道具未确认）', control.errors.some((e) => e.includes('禁止推进')));

  console.log('7. 超时标记 + 迟到确认');
  await control.waitState((s) => s.timedOut.includes('道具'), 8000);
  check('道具超时未确认被标记', true);
  prop.send({ type: 'confirm' });
  st = await control.waitState((s) => s.confirmations['道具']);
  check('迟到确认被接受且标记 late', st.confirmations['道具'].late === true);
  check('日志含 confirm-late', st.log.some((l) => l.event === 'confirm-late'));

  console.log('8. 断线重连恢复状态');
  light.ws.close();
  await sleep(300);
  const light2 = client('position', '灯光');
  await light2.ready;
  st = await light2.waitState((s) => s.phase === 'active' && s.currentCue.seq === 2);
  check('重连后恢复当前口令', st.currentCue.text.includes('音乐起'));
  check('重连后恢复确认状态', !!st.confirmations['音响'] && !!st.confirmations['道具']);

  console.log('9. 推进至完成');
  control.send({ type: 'advance' });
  st = await control.waitState((s) => s.phase === 'done');
  check('全部口令完成', st.log.some((l) => l.event === 'sequence-finished'));

  console.log('10. 重置后可重新编辑');
  control.send({ type: 'reset' });
  st = await director.waitState((s) => s.phase === 'idle');
  check('重置回 idle', true);
  check('事件日志保留', st.log.length > 5);

  console.log(`\n结果：${passed} 通过，${failed} 失败`);
  process.exit(failed ? 1 : 0);
})().catch((e) => { console.error('测试异常：', e.message); process.exit(1); });

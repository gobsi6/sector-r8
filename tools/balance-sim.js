// 섹터 R-8 작전 자동 시뮬레이터: 실제 server.js의 규칙 코드를 그대로 불러와 봇 요원들로 작전을 반복 실행한다.
// 사용: node tools/balance-sim.js server.js 300   (조합마다 300판, 결과는 JSON 한 줄)
// 한 판의 기록 보기: PEEK=R8-14,3,1 node tools/balance-sim.js server.js   (작전, 현장 인원, 관제 0/1)
// 관제 행동 빼고 보기: CTRL=none|nosupport|nofac|nostab
// 봇은 개체까지의 정확한 거리를 알고 판단하므로 실제 플레이보다 성공률이 높게 나오는 편이다.
const fs = require('fs'), path = require('path'), Module = require('module'), os = require('os');
const file = path.resolve(process.argv[2]), N = +(process.argv[3] || 200);
process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'r8sim-'));
process.env.PORT = '0';
const src = fs.readFileSync(file, 'utf8').replace(/server\.listen\([^\n]*\n/, '\n') + '\nmodule.exports = { get: n => { try { return eval(n); } catch (e) { return undefined; } } };\n';
const m = new Module(file, null); m.filename = file; m.paths = Module._nodeModulePaths(path.dirname(file));
const log0 = console.log; console.log = () => {}; m._compile(src, file); console.log = log0;
const G = n => m.exports.get(n);
const db = G('db'), Q = G('Q'), runs = G('runs'), lobbyOf = G('lobbyOf'), D = G('D'), posOf = G('posOf'), lineOf = G('lineOf'), posOk = G('posOk');
const opJoin = G('opJoin'), opReady = G('opReady'), opStart = G('opStart'), opCtl = G('opCtl');
const opPick = G('opPick'), resolveRound = G('resolveRound'), opAct = G('opAct'), opMove = G('opMove');
const ROUND = !!opPick, REACH_M = G('REACH_M') || 6, SKILLS = G('SKILLS');
// 봇 요원: 능력치 합 60, 기능 3개 등급 1 (캐릭터 생성과 같은 조건)
const BOTS = [
  { name: '근접', stats: [13, 11, 10, 9, 8, 9], sk: ['격투', '운반', '인내'] },
  { name: '사격', stats: [9, 10, 13, 10, 9, 9], sk: ['사격', '은신', '관찰'] },
  { name: '기술', stats: [9, 10, 10, 13, 9, 9], sk: ['장비 운용', '기록 해독', '응급처치'] },
  { name: '정찰', stats: [9, 10, 11, 9, 12, 9], sk: ['관찰', '운반', '격투'] },
  { name: '관제', stats: [8, 9, 9, 13, 10, 11], sk: ['통신', '기록 해독', '응급처치'] }
];
const now = () => Date.now();
BOTS.forEach((b, i) => {
  const uid = Number(Q.addUser.run('sim' + i, 'x', now()).lastInsertRowid); b.uid = uid;
  const skills = Object.fromEntries(b.sk.map(k => [k, 1]));
  Q.addAgent.run({ user_id: uid, num: 'S-' + i, name: b.name, role: 0, head: 0, photo: null, age: 30, stats: JSON.stringify(b.stats), trait: 1, skills: JSON.stringify(skills), res: JSON.stringify({ supply: 100, research: 0, pass: 0 }), created: now() });
  db.prepare('UPDATE agents SET traits = ? WHERE user_id = ?').run(JSON.stringify([]), uid);
});
const SKLV = Math.max(1, Math.min(5, +(process.env.SKLV || 1))); // 봇 기능 등급(SKLV=3이면 중견 요원)
// 판마다 봇을 처음 상태로 되돌린다. 기능 경험치도 지워야 뒤에 도는 판이 더 강해지지 않는다.
const resetBots = () => { db.prepare('UPDATE agents SET contamination = 0, regen_until = NULL, quar_until = NULL, traits = ?, career = 0, skill_xp = ?').run('[]', '{}');
  BOTS.forEach(b => db.prepare('UPDATE agents SET skills = ? WHERE user_id = ?').run(JSON.stringify(Object.fromEntries(b.sk.map(k => [k, SKLV]))), b.uid)); };
const dice = (b, statIdx, skill) => Math.max(1, Math.floor(b.stats[statIdx] / 2) - 1 + (b.sk.includes(skill) ? 1 : 0));
// 현장 봇의 판단: 쓰러진 동료 회수 → 교전(막기·공격) → 장면 진행(목표 선택지, 필요하면 전진)
function fieldPick(r, mem, taken) {
  const b = BOTS.find(x => String(x.uid) === mem.uid), o = D(r), sc = o.scenes[r.scene], p = posOf(r, mem.uid), L = lineOf(r);
  const down = r.members.find(x => x.role === 'field' && x.uid !== mem.uid && (x.dead || x.broken) && !x.recovered);
  if (sc.fork) { // 갈림길: 관제가 들여다본 문 중 위험 없는 쪽, 아니면 들여다본 위험 문의 반대쪽, 그것도 없으면 문 A
    const good = sc.doors.find(d => d.peeked && (d.risk === 'none' || d.risk === 'cache')), bad = sc.doors.find(d => d.peeked && !(d.risk === 'none' || d.risk === 'cache'));
    return ['act', 'fork-' + (good ? good.id : bad ? (bad.id === 'a' ? 'b' : 'a') : 'a')];
  }
  if (down && !taken.has('rec')) { taken.add('rec'); return ['act', 'rec-' + down.uid]; }
  for (const side of ['front', 'rear']) {
    const f = r.minions.find(x => x.side === side); if (!f) continue;
    const melee = p === 'solo' || p === side, ranged = p === 'mid' || (L.length === 2 && p !== side);
    if (melee && (f.intent === 'bite' || f.intent === 'scream') && mem.hp < mem.hpMax * 0.35) return ['act', 'guard'];
    if (melee || ranged) return ['act', 'atk-' + side];
  }
  const distOk = c => !(c.max && r.dist > c.max) && !(c.min && r.dist < c.min);
  const reachOk = c => !(c.reach && r.pos < sc.map.len - REACH_M);
  const adv = sc.choices.filter(c => c.adv !== false && posOk(r, mem.uid, c.pos));
  const can = adv.filter(c => reachOk(c) && distOk(c));
  const blockF = r.minions.some(x => x.side === 'front'), blockR = r.minions.some(x => x.side === 'rear');
  if (!can.length) {
    // 목표 지점까지 가야 하면 선두가 전진
    if (adv.some(c => !reachOk(c)) && (p === 'front' || p === 'solo') && !blockF) return ['move', 'fwd'];
    // 개체가 너무 가까우면 개체에서 멀어지는 쪽으로 이동
    if (adv.some(c => c.min && r.dist < c.min)) {
      if (sc.from === 'rear' && (p === 'front' || p === 'solo') && !blockF && r.pos < sc.map.len) return ['move', 'fwd'];
      if (sc.from === 'front' && (p === 'rear' || p === 'solo') && !blockR && r.pos > 0) return ['move', 'back'];
    }
    return ['move', 'wait'];
  }
  // 성공 가능성이 괜찮은 요원은 각자 목표 선택지를 시도한다(라운드제에서는 먼저 성공한 것만 남는다)
  const best = can.sort((a, b2) => dice(b, b2.stat, b2.skill) - b2.diff - (dice(b, a.stat, a.skill) - a.diff))[0];
  if (ROUND ? dice(b, best.stat, best.skill) < best.diff * 2 && taken.has('adv') : taken.has('adv')) return ['move', 'wait'];
  taken.add('adv');
  return ['act', best.id];
}
// 포지션 봇: 자기 포지션 행동이 필요하면 그것부터, 아니면 일반 판단(fieldPick)
// 기본 편성(CLS 환경 변수로 바꿀 수 있다. 예: CLS=atk,tech,med,def)
// 출발 장비 편성(CLS=weapon,radio,medkit 처럼). 장비와 상관없이 누구나 관제실 요청·치료를 할 수 있다.
const CLS_DEF = { 1: ['weapon'], 2: ['weapon', 'radio'], 3: ['weapon', 'medkit', 'radio'], 4: ['weapon', 'medkit', 'radio', 'detector'] };
// 봇 분담: 관제실 요청은 통신기 든 요원(없으면 선두가 아닌 첫 요원), 치료는 구급상자(없으면 응급처치 기능 있는 요원)
function duty(r) {
  const team = r.members.filter(x => x.role === 'field' && !x.dead && !x.broken), lead = x => ['front', 'solo'].includes(posOf(r, x.uid));
  const tech = team.find(x => x.cls === 'radio') || team.find(x => !lead(x)) || team[0];
  const med = team.find(x => x.cls === 'medkit') || team.find(x => x.sk && x.sk['응급처치']) || null;
  return { tech: tech && tech.uid, med: med && med.uid };
}
function classPick(r, mem, taken) {
  const sc = D(r).scenes[r.scene], facsOf = G('facsOf'), p = posOf(r, mem.uid);
  const team = r.members.filter(x => x.role === 'field' && !x.dead);
  const lead = p === 'front' || p === 'solo';
  const adv0 = sc.choices ? sc.choices.filter(c => c.adv !== false) : [], needLead = !sc.fork && adv0.length && adv0.every(c => c.reach && r.pos < sc.map.len - REACH_M) && !r.minions.some(x => x.side === 'front');
  // 관제실이 개체 접촉을 예고하면 대상자는 피한다(EVADE=0 이면 무시)
  if (r.contactWarn && process.env.EVADE !== '0' && r.contactWarn.uid === mem.uid) return ['act', 'evade'];
  // 관제실이 하수인 출현을 하달하면, 앞에 붙은 하수인이 없고 개체가 멀 때 숨는다(HIDE=0 이면 무시)
  if (r.ambush && process.env.HIDE !== '0' && !r.minions.length && r.dist > (sc.speed || 5) * 2) return ['act', 'hide'];
  const DU = duty(r);
  if (DU.tech === mem.uid) {
    if (r.noise && r.count <= 4) return ['act', mem.san > mem.sanMax * 0.5 ? 'decode' : 'jam'];
    if (sc.fork) { const d = sc.doors.find(x => !x.peeked); if (d && !sc.doors.some(x => x.peeked && (x.risk === 'none' || x.risk === 'cache'))) return ['act', 'peek-' + d.id]; return null; }
    if (r.minions.some(x => x.side === p || p === 'solo')) return null;
    if (lead && needLead) return null;
    const F = facsOf(r), f = id => F.find(x => x.id === id);
    if (f('beacon')) return ['act', 'fac-beacon'];
    if (!f('pwr').on) return ['act', 'fac-pwr'];
    if (!f('sp').on) return ['act', 'fac-sp'];
    if (!f('wall').on && f('wall').need && r.dist <= 15) return ['act', 'fac-wall'];
    if (r.support < 2 && !taken.has('sup')) { taken.add('sup'); return ['act', 'sup']; }
    return null;
  }
  if (DU.med === mem.uid) {
    const crit = lead && needLead ? 0.25 : 1;
    const lowS = team.filter(x => x.san < x.sanMax * 0.4 * crit).sort((a, b) => a.san / a.sanMax - b.san / b.sanMax)[0];
    if (lowS) return ['act', 'calm-' + lowS.uid];
    const lowH = team.filter(x => x.hp < x.hpMax * 0.5 * crit).sort((a, b) => a.hp / a.hpMax - b.hp / b.hpMax)[0];
    if (lowH) return ['act', 'heal-' + lowH.uid];
    return null;
  }
  return null;
}
function runOne(opId, nField, withCtrl) {
  resetBots(); runs.delete(opId); lobbyOf(opId);
  const cl0 = process.env.CLS ? process.env.CLS.split(',') : CLS_DEF[nField], BI = { weapon: 0, medkit: 1, radio: 2, detector: 3 };
  const used = new Set(), team = cl0.slice(0, nField).map(c => { let i = BI[c]; while (used.has(i)) i = (i + 1) % 4; used.add(i); return BOTS[i]; });
  team.forEach(b => opJoin(b.uid, opId, 'field'));
  team.forEach((b, i) => G('opCls')(b.uid, opId, cl0[i]));
  runs.get(opId).members.forEach(x => opReady(Number(x.uid), opId, true));
  const e = opStart(team[0].uid, opId); if (e) throw new Error('start ' + e);
  const r = runs.get(opId); let actions = 0, guard = 0;
  while (r.status === 'live' && guard++ < 400) {
    const field = lineOf(r).filter(x => !x.broken);
    const taken = new Set();
    if (ROUND) {
      if (!field.length) { resolveRound(r); continue; }
      for (const mem of field) { if (r.status !== 'live') break; const [k, v] = classPick(r, mem, taken) || fieldPick(r, mem, taken); actions++;
        if (opPick(mem.uid, opId, k, v)) opPick(mem.uid, opId, 'move', 'wait'); }
    } else {
      if (!field.length) { const any = r.members.find(x => x.role === 'field'); opMove(any.uid, opId, 'wait'); continue; } // 예전 방식: 아무도 못 움직이면 멈추므로 시간만 흐른다고 본다
      for (const mem of field) { if (r.status !== 'live' || mem.broken || mem.dead) continue; const [k, v] = fieldPick(r, mem, new Set()); actions++;
        const err = k === 'move' ? opMove(mem.uid, opId, v) : opAct(mem.uid, opId, v); if (err) opMove(mem.uid, opId, 'wait');
      }
    }
  }
  const f = r.members.filter(x => x.role === 'field');
  const why = r.result === 'success' ? 'success' : r.count <= 0 ? 'count' : f.every(x => x.dead) ? 'dead' : r.result === 'fail' ? 'broken' : r.result || 'stuck';
  return { result: r.result || 'stuck', why, turns: r.turn, actions, dead: f.filter(x => x.dead).length, left: f.filter(x => (x.dead || x.broken) && !x.recovered).length,
    hp: f.filter(x => !x.dead).reduce((a, x) => a + x.hp / x.hpMax, 0) / Math.max(1, f.filter(x => !x.dead).length), count: r.count };
}
if (process.env.PEEK) { const [op, nf, c] = process.env.PEEK.split(','); const res = runOne(op, +nf, c === '1'); const r = runs.get(op); console.log(JSON.stringify(res)); r.log.forEach(L => console.log('-', L.kind, L.label || '', L.tierName || '', (L.text || '').slice(0, 90), L.fx ? JSON.stringify(L.fx) : '')); process.exit(0); }
const out = [];
for (const opId of (process.env.OPS || 'R8-14,R8-P').split(',')) for (const nf of [1, 2, 3, 4]) for (const ctl of [false]) {
  const R = []; for (let i = 0; i < N; i++) R.push(runOne(opId, nf, ctl));
  const pct = k => Math.round(R.filter(x => x.why === k).length / N * 100);
  const avg = k => +(R.reduce((a, x) => a + x[k], 0) / N).toFixed(2);
  out.push({ op: opId, field: nf, ctrl: ctl ? 1 : 0, success: pct('success'), failCount: pct('count'), failDead: pct('dead'), failBroken: pct('broken'), stuck: pct('stuck'), turns: avg('turns'), actions: avg('actions'), deaths: avg('dead'), leftBehind: avg('left'), hpLeft: Math.round(avg('hp') * 100), countLeft: avg('count') });
}
process.stdout.write(JSON.stringify({ mode: ROUND ? 'round' : 'action', n: N, rows: out }) + '\n');
process.exit(0);

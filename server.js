// 섹터 R-8 게임 서버
// 계정, 캐릭터(요원), 접속 상태, 통신 채널을 맡는다.
// 데이터는 SQLite 파일 하나에 저장한다. Railway에 볼륨을 붙이면 그 경로(RAILWAY_VOLUME_MOUNT_PATH)를 자동으로 쓴다.
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');
const { DatabaseSync } = require('node:sqlite');
const bcrypt = require('bcryptjs');

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || process.env.RAILWAY_VOLUME_MOUNT_PATH || path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });

/* ───────── 데이터베이스 ───────── */
const db = new DatabaseSync(path.join(DATA_DIR, 'sector-r8.db'));
db.exec('PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;');
const transaction = fn => { db.exec('BEGIN IMMEDIATE'); try { const r = fn(); db.exec('COMMIT'); return r; } catch (e) { db.exec('ROLLBACK'); throw e; } };
db.exec(`
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  login TEXT NOT NULL UNIQUE COLLATE NOCASE,
  pw TEXT NOT NULL,
  created INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
  token TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS agents (
  user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  num TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  role INTEGER NOT NULL,
  head INTEGER NOT NULL,
  age INTEGER NOT NULL,
  stats TEXT NOT NULL,
  trait INTEGER NOT NULL,
  skills TEXT NOT NULL,
  contamination INTEGER NOT NULL DEFAULT 0,
  res TEXT NOT NULL,
  created INTEGER NOT NULL,
  last_seen INTEGER
);
CREATE TABLE IF NOT EXISTS sealed (
  user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS chat (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  num TEXT NOT NULL,
  name TEXT NOT NULL,
  role INTEGER NOT NULL,
  channel TEXT NOT NULL,
  text TEXT NOT NULL,
  ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS chat_ts ON chat(channel, ts);
CREATE TABLE IF NOT EXISTS op_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  op TEXT NOT NULL,
  title TEXT NOT NULL,
  result TEXT NOT NULL,
  started INTEGER NOT NULL,
  ended INTEGER NOT NULL,
  members TEXT NOT NULL,
  log TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sector_entities (
  id TEXT PRIMARY KEY,
  stability REAL NOT NULL,
  updated INTEGER NOT NULL,
  captured INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS dm (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_id INTEGER NOT NULL,
  to_id INTEGER NOT NULL,
  text TEXT NOT NULL,
  ts INTEGER NOT NULL,
  read INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS dm_pair ON dm (from_id, to_id);
CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  entity TEXT NOT NULL,
  kind TEXT NOT NULL,
  mins INTEGER NOT NULL,
  started INTEGER NOT NULL,
  ends INTEGER NOT NULL,
  status TEXT NOT NULL,
  result TEXT,
  acked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS sector_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at INTEGER NOT NULL,
  kind TEXT NOT NULL,
  entity TEXT,
  text TEXT NOT NULL,
  voice TEXT
);
CREATE TABLE IF NOT EXISTS sector_unlocks (
  entity TEXT NOT NULL,
  section TEXT NOT NULL,
  by_name TEXT NOT NULL,
  at INTEGER NOT NULL,
  PRIMARY KEY (entity, section)
);
CREATE TABLE IF NOT EXISTS drafts (
  user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  num TEXT NOT NULL UNIQUE,
  data TEXT NOT NULL,
  created INTEGER NOT NULL
);
`);

// 예전 데이터베이스에 없던 칸을 더한다
for (const [col, type] of [['traits', 'TEXT'], ['regen_until', 'INTEGER'], ['ops_done', 'INTEGER NOT NULL DEFAULT 0'], ['quar_until', 'INTEGER'], ['skill_xp', "TEXT NOT NULL DEFAULT '{}'"], ['career', 'INTEGER NOT NULL DEFAULT 0'], ['stat_pts', 'INTEGER NOT NULL DEFAULT 0'], ['inv', "TEXT NOT NULL DEFAULT '{}'"], ['ach', "TEXT NOT NULL DEFAULT '{}'"]]) {
  if (!db.prepare(`SELECT 1 FROM pragma_table_info('agents') WHERE name = '${col}'`).get()) db.exec(`ALTER TABLE agents ADD COLUMN ${col} ${type}`);
}
// 외관 사진 칸이 없던 예전 데이터베이스에 칸을 더한다
if (!db.prepare("SELECT 1 FROM pragma_table_info('tasks') WHERE name = 'signs'").get()) db.exec("ALTER TABLE tasks ADD COLUMN signs TEXT NOT NULL DEFAULT '[]'");
if (!db.prepare("SELECT 1 FROM pragma_table_info('sector_entities') WHERE name = 'obs'").get()) db.exec('ALTER TABLE sector_entities ADD COLUMN obs INTEGER NOT NULL DEFAULT 0');
if (!db.prepare("SELECT 1 FROM pragma_table_info('sector_entities') WHERE name = 'lock_until'").get()) db.exec('ALTER TABLE sector_entities ADD COLUMN lock_until INTEGER NOT NULL DEFAULT 0');
if (!db.prepare("SELECT 1 FROM pragma_table_info('agents') WHERE name = 'photo'").get()) db.exec('ALTER TABLE agents ADD COLUMN photo TEXT');
// 예전 버전(이메일 가입)으로 만든 데이터베이스면 칸 이름만 바꾼다
if (db.prepare("SELECT 1 FROM pragma_table_info('users') WHERE name = 'email'").get()) db.exec('ALTER TABLE users RENAME COLUMN email TO login');

// 관제팀은 NPC(벌처)가 맡게 되어, 예전에 관제 요원으로 만든 캐릭터는 모두 현장 요원이 된다
db.exec('UPDATE agents SET role = 0 WHERE role = 1');
const Q = {
  userByLogin: db.prepare('SELECT * FROM users WHERE login = ?'),
  addUser: db.prepare('INSERT INTO users (login, pw, created) VALUES (?, ?, ?)'),
  addSession: db.prepare('INSERT INTO sessions (token, user_id, created) VALUES (?, ?, ?)'),
  allUsers: db.prepare('SELECT id, login FROM users'),
  session: db.prepare('SELECT users.id, users.login FROM sessions JOIN users ON users.id = sessions.user_id WHERE token = ?'),
  dropSession: db.prepare('DELETE FROM sessions WHERE token = ?'),
  agent: db.prepare('SELECT * FROM agents WHERE user_id = ?'),
  agentByNum: db.prepare('SELECT user_id FROM agents WHERE num = ?'),
  agents: db.prepare('SELECT * FROM agents ORDER BY num'),
  addAgent: db.prepare(`INSERT INTO agents (user_id, num, name, role, head, photo, age, stats, trait, skills, contamination, res, created, last_seen)
    VALUES (@user_id, @num, @name, @role, @head, @photo, @age, @stats, @trait, @skills, 0, @res, @created, @created)`),
  setPhoto: db.prepare('UPDATE agents SET photo = ? WHERE user_id = ?'),
  afterOp: db.prepare('UPDATE agents SET contamination = @cont, traits = @traits, trait = @trait, res = @res, regen_until = @regen, quar_until = @quar, ops_done = @ops WHERE user_id = @uid'),
  addRun: db.prepare('INSERT INTO op_runs (op, title, result, started, ended, members, log) VALUES (@op, @title, @result, @started, @ended, @members, @log)'),
  runs: db.prepare('SELECT id, op, title, result, started, ended, members FROM op_runs ORDER BY id DESC LIMIT 40'),
  runLog: db.prepare('SELECT log FROM op_runs WHERE id = ?'),
  seen: db.prepare('UPDATE agents SET last_seen = ? WHERE user_id = ?'),
  sealed: db.prepare('SELECT data FROM sealed WHERE user_id = ?'),
  addSealed: db.prepare('INSERT INTO sealed (user_id, data) VALUES (?, ?)'),
  draft: db.prepare('SELECT * FROM drafts WHERE user_id = ?'),
  draftByNum: db.prepare('SELECT user_id FROM drafts WHERE num = ?'),
  addDraft: db.prepare('INSERT INTO drafts (user_id, num, data, created) VALUES (?, ?, ?, ?)'),
  setDraftNum: db.prepare('UPDATE drafts SET num = ? WHERE user_id = ?'),
  dropDraft: db.prepare('DELETE FROM drafts WHERE user_id = ?'),
  chatRecent: db.prepare('SELECT * FROM (SELECT * FROM chat ORDER BY id DESC LIMIT 300) ORDER BY id'),
  addChat: db.prepare('INSERT INTO chat (user_id, num, name, role, channel, text, ts) VALUES (?, ?, ?, ?, ?, ?, ?)')
};

/* ───────── 규칙 데이터 (설계서 기준, 검증용) ───────── */
const DEPT_SKILLS = [['장비 운용', '운반'], ['사격', '격투'], ['기록 해독', '관찰'], ['응급처치', '인내'], ['통신', '장비 운용'], ['기록 해독', '운반'], ['은신', '관찰'], ['운반', '응급처치'], ['관찰', '인내'], [],
  ['관찰', '기록 해독'], ['운반', '장비 운용'], ['응급처치', '장비 운용'], ['인내', '운반'], ['통신', '관찰'], ['사격', '은신'], ['응급처치', '관찰'], ['응급처치', '격투'], ['기록 해독', '은신'], ['장비 운용', '인내'],
  ['격투', '운반'], ['기록 해독', '인내'], ['통신', '사격'], ['은신', '인내'], ['통신', '기록 해독']];
const PAST_SIZE = 25, TRAIT_SIZE = 25;
const SKILLS = ['격투', '사격', '은신', '응급처치', '장비 운용', '기록 해독', '통신', '관찰', '인내', '운반'];
// 이름이 바뀐 기능(참기 → 인내)은 옮기고, 없어진 기능(오컬트, 교섭)을 가진 요원은 아직 없는 기능 하나로 바꿔 준다
const OLD_SKILL_SWAP = ['인내', '응급처치', '통신', '은신', '관찰', '기록 해독', '운반', '장비 운용', '격투', '사격'];
for (const a of db.prepare("SELECT user_id, skills FROM agents WHERE skills LIKE '%오컬트%' OR skills LIKE '%교섭%' OR skills LIKE '%참기%'").all()) {
  const sk = JSON.parse(a.skills);
  if (sk['참기']) { sk['인내'] = Math.max(sk['인내'] || 0, sk['참기']); delete sk['참기']; } // 참기 → 인내 (이름만 바뀜)
  for (const old of ['오컬트', '교섭']) if (sk[old]) { const lv = sk[old]; delete sk[old]; const to = OLD_SKILL_SWAP.find(k => !sk[k]); if (to) sk[to] = lv; }
  db.prepare('UPDATE agents SET skills = ? WHERE user_id = ?').run(JSON.stringify(sk), a.user_id);
}
const START_RES = { supply: 100, research: 0, pass: 1 };
const CHANNELS = ['all', 'field', 'ctrl'];

/* ───────── 도우미 ───────── */
const now = () => Date.now();
const isInt = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
// 외관 사진: 브라우저에서 224×272로 줄인 data URL만 받는다
const PHOTO_RE = /^data:image\/(webp|jpeg|png);base64,[A-Za-z0-9+/=]+$/;
const photoOf = v => v == null || v === '' ? null : (typeof v === 'string' && v.length <= 300000 && PHOTO_RE.test(v) ? v : undefined);
const str = (v, max) => typeof v === 'string' ? v.trim().slice(0, max) : '';
const online = new Map(); // user_id → 연결 수

/* ───────── 보급 상점 ───────── */
// kind: use(작전 소모품, 최대 3개) · gear(장비, slot마다 하나 장착) · frame(출입증 테두리). 칭호는 팔지 않고 업적으로 얻는다.
const SHOP = [
  { id: 'kit', kind: 'use', name: '구급 키트', price: 25, desc: '작전 중 가장 많이 다친 현장 요원의 HP를 30 회복한다.' },
  { id: 'calm', kind: 'use', name: '진정제', price: 25, desc: '작전 중 정신력이 가장 낮은 현장 요원의 정신력을 30 회복한다. 붕괴한 요원도 일으킨다.' },
  { id: 'flare', kind: 'use', name: '섬광탄', price: 40, desc: '작전 중 주변의 하수인을 모두 쫓아낸다.' },
  { id: 'vest', kind: 'gear', slot: 'armor', name: '경량 방호복', price: 120, desc: 'HP 최대치 +15.', fx: { hp: 15 } },
  { id: 'suit', kind: 'gear', slot: 'armor', name: '강화 방호복', price: 260, desc: 'HP 최대치 +30.', fx: { hp: 30 } },
  { id: 'shield', kind: 'gear', slot: 'armor', name: '차폐 외투', price: 200, desc: '오염 한계 +20. 검은 주사위가 늦게 섞인다.', fx: { lim: 20 } },
  { id: 'patch', kind: 'gear', slot: 'tool', name: '안정 패치', price: 150, desc: '정신력 최대치 +15.', fx: { san: 15 } },
  { id: 'radio', kind: 'gear', slot: 'tool', name: '고급 무전기', price: 180, desc: '통신 기능 +1.', fx: { skill: ['통신', 1] } },
  { id: 'lamp', kind: 'gear', slot: 'tool', name: '전술 손전등', price: 150, desc: '관찰 기능 +1.', fx: { skill: ['관찰', 1] } },
  { id: 'toolkit', kind: 'gear', slot: 'tool', name: '정비 공구 세트', price: 160, desc: '장비 운용 기능 +1.', fx: { skill: ['장비 운용', 1] } },
  { id: 'gold', kind: 'frame', name: '금빛 테두리', price: 120, desc: '출입증 테두리를 금빛으로 바꾼다.' },
  { id: 'redline', kind: 'frame', name: '붉은 경계선', price: 100, desc: '출입증 테두리를 붉은 경계선으로 바꾼다.' },
  { id: 'holo', kind: 'frame', name: '청록 홀로그램', price: 150, desc: '출입증 테두리를 청록 홀로그램으로 바꾼다.' }
];
const SHOP_BY = Object.fromEntries(SHOP.map(x => [x.id, x])), USE_MAX = 3, SLOTS = ['armor', 'tool'];
const invOf = row => { let v = {}; try { v = JSON.parse(row.inv || '{}'); } catch {} return { items: v.items || {}, own: v.own || [], eq: v.eq || {}, frame: v.frame || null, title: v.title || null, titles: v.titles || [] }; };
/* ───────── 업적과 칭호 ───────── */
// 칭호는 업적을 이루면 얻는다. c는 요원별 기록(작전 수, 재생 횟수 등)이다.
const TITLES = [
  { id: 'rookie', name: '신입 요원', desc: '작전을 처음으로 마친다.', key: 'ops', need: 1 },
  { id: 'veteran', name: '현장 베테랑', desc: '작전을 10번 성공한다.', key: 'success', need: 10 },
  { id: 'voice', name: '무전 너머의 목소리', desc: '기술 포지션으로 작전을 5번 마친다.', key: 'ctrlOps', need: 5 },
  { id: 'night', name: '야간 근무자', desc: '기지 관리 작업을 10번 마친다.', key: 'tasks', need: 10 },
  { id: 'steady', name: '침착한 손', desc: '관리 작업 중 이상 징후에 10번 대응한다.', key: 'signs', need: 10 },
  { id: 'tankreg', name: '수조 단골', desc: '재생 수조에서 3번 재생된다.', key: 'deaths', need: 3 },
  { id: 'survivor', name: '끝까지 버틴 자', desc: 'HP 20% 이하로 버티며 작전을 성공한다.', key: 'clutch', need: 1 },
  { id: 'officer', name: '고참', desc: '계급 5에 오른다.', key: 'rank', need: 5 },
  { id: 'first', name: '첫 출동', desc: '첫 출동 훈련을 마친다.', key: 'tutDone', need: 1 }
];
const TITLE_BY = Object.fromEntries(TITLES.map(x => [x.id, x]));
const achOf = row => { try { return JSON.parse(row.ach || '{}'); } catch { return {}; } };
// 기록을 더하고 새로 이룬 업적의 칭호를 준다
function bumpAch(uid, add) {
  const row = Q.agent.get(Number(uid)); if (!row) return [];
  const c = achOf(row); for (const [k, n] of Object.entries(add || {})) c[k] = (c[k] || 0) + n;
  c.rank = rankOf(row.career || 0).rank;
  const v = invOf(row), got = TITLES.filter(t => !v.titles.includes(t.id) && (c[t.key] || 0) >= t.need);
  got.forEach(t => { v.titles.push(t.id); if (!v.title) v.title = t.id; });
  db.prepare('UPDATE agents SET ach = ?, inv = ? WHERE user_id = ?').run(JSON.stringify(c), JSON.stringify(v), Number(uid));
  got.forEach(t => io.to('u:' + uid).emit('notice', { text: `칭호 「${t.name}」을(를) 얻었습니다. (${t.desc.replace(/\.$/, '')})`, level: 'ach', at: now() }));
  return got;
}
const gearFx = row => { const v = invOf(row), fx = { hp: 0, san: 0, lim: 0, skill: {} };
  SLOTS.forEach(sl => { const g = SHOP_BY[v.eq[sl]]; if (!g || !g.fx) return; fx.hp += g.fx.hp || 0; fx.san += g.fx.san || 0; fx.lim += g.fx.lim || 0; if (g.fx.skill) fx.skill[g.fx.skill[0]] = (fx.skill[g.fx.skill[0]] || 0) + g.fx.skill[1]; });
  return fx; };
function publicAgent(row) {
  if (!row) return null;
  return {
    uid: String(row.user_id), num: row.num, name: row.name, role: row.role, head: row.head, photo: row.photo || null, age: row.age,
    stats: JSON.parse(row.stats), trait: row.trait, traits: traitsOf(row), skills: JSON.parse(row.skills), contamination: row.contamination,
    task: typeof QT !== 'undefined' ? taskOut(QT.mine.get(row.user_id)) : null,
    skillXp: JSON.parse(row.skill_xp || '{}'), career: { ...rankOf(row.career || 0), total: row.career || 0 }, statPts: row.stat_pts || 0, statCap: STAT_CAP,
    regenUntil: row.regen_until || null, quarUntil: row.quar_until || null, contLimit: contLimit(row), opsDone: row.ops_done || 0,
    inv: invOf(row), ach: achOf(row), gear: gearFx(row), res: JSON.parse(row.res), createdAt: row.created, lastSeen: row.last_seen, online: (online.get(row.user_id) || 0) > 0
  };
}
function chatOut(r) { return { id: r.id, uid: String(r.user_id), num: r.num, name: r.name, role: r.role, channel: r.channel, text: r.text, ts: r.ts }; }
function newToken(userId) { const t = crypto.randomBytes(32).toString('hex'); Q.addSession.run(t, userId, now()); return t; }
function authUser(token) { return token ? Q.session.get(String(token)) : null; }
// GM 계정: 기본은 admin. 환경 변수 GM_LOGINS에 쉼표로 적으면 그 아이디들로 바뀐다 (대소문자 무시)
const GM_LOGINS = String(process.env.GM_LOGINS || 'admin').split(',').map(x => x.trim().toLowerCase()).filter(Boolean);
const isGM = u => !!(u && GM_LOGINS.includes(String(u.login).toLowerCase()));
const needGM = (req, res, next) => isGM(req.user) ? next() : fail(res, 'gm/forbidden', 403);

// 같은 주소에서 로그인 시도가 몰리면 잠시 막는다
const tries = new Map();
function throttled(ip) {
  const t = tries.get(ip) || { n: 0, at: now() };
  if (now() - t.at > 10 * 60 * 1000) { t.n = 0; t.at = now(); }
  t.n++; tries.set(ip, t);
  return t.n > 30;
}

/* ───────── HTTP ───────── */
const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '400kb' }));
app.use(express.static(path.join(__dirname, 'public')));

const fail = (res, code, status = 400) => res.status(status).json({ error: code });
const need = (req, res, next) => {
  const u = authUser((req.get('authorization') || '').replace(/^Bearer /, ''));
  if (!u) return fail(res, 'auth/required', 401);
  req.user = u; next();
};

app.get('/healthz', (req, res) => res.send('ok'));

app.post('/api/signup', (req, res) => {
  if (throttled(req.ip)) return fail(res, 'auth/too-many-requests', 429);
  const login = str(req.body.login, 40), pw = typeof req.body.pw === 'string' ? req.body.pw : '';
  if (!/^[A-Za-z0-9_]{4,20}$/.test(login)) return fail(res, 'auth/invalid-login');
  if (pw.length < 6) return fail(res, 'auth/weak-password');
  if (Q.userByLogin.get(login)) return fail(res, 'auth/login-taken');
  const info = Q.addUser.run(login, bcrypt.hashSync(pw, 10), now());
  res.json({ token: newToken(info.lastInsertRowid), user: { uid: String(info.lastInsertRowid), login } });
});

app.post('/api/login', (req, res) => {
  if (throttled(req.ip)) return fail(res, 'auth/too-many-requests', 429);
  const login = str(req.body.login, 40), pw = typeof req.body.pw === 'string' ? req.body.pw : '';
  if (!login) return fail(res, 'auth/missing-login');
  if (!pw) return fail(res, 'auth/missing-password');
  const u = Q.userByLogin.get(login);
  if (!u || !bcrypt.compareSync(pw, u.pw)) return fail(res, 'auth/invalid-credential');
  res.json({ token: newToken(u.id), user: { uid: String(u.id), login: u.login } });
});

app.post('/api/logout', (req, res) => {
  Q.dropSession.run((req.get('authorization') || '').replace(/^Bearer /, ''));
  res.json({ ok: true });
});

app.get('/api/me', need, (req, res) => {
  const s = Q.sealed.get(req.user.id);
  res.json({ user: { uid: String(req.user.id), login: req.user.login, gm: isGM(req.user) }, agent: publicAgent(Q.agent.get(req.user.id)), sealed: s ? JSON.parse(s.data) : null });
});

// ── 재생 절차 초안: 넘버와 과거·변질 후보를 서버가 정해 두고, 새로고침해도 바뀌지 않게 저장한다
const die = n => crypto.randomInt(1, n + 1);
function freeNumber() {
  for (let i = 0; i < 5000; i++) {
    const num = 'R8-' + String(crypto.randomInt(10, 1000)).padStart(3, '0');
    if (!Q.agentByNum.get(num) && !Q.draftByNum.get(num)) return num;
  }
  throw Object.assign(new Error('no number'), { code: 'number-full' });
}
function rollPasts() {
  const out = [], seen = new Set();
  while (out.length < 3) {
    const p = { dept: die(PAST_SIZE) - 1, death: die(PAST_SIZE) - 1, secret: die(PAST_SIZE) - 1 }, k = p.dept + '-' + p.death + '-' + p.secret;
    if (!seen.has(k)) { seen.add(k); out.push(p); }
  }
  return out;
}
function rollTraits() {
  const out = [], seen = new Set();
  while (out.length < 3) { const id = die(TRAIT_SIZE); if (!seen.has(id)) { seen.add(id); out.push({ id }); } }
  return out;
}
function draftOf(uid) {
  const row = Q.draft.get(uid);
  if (row) return { num: row.num, ...JSON.parse(row.data) };
  const d = { pasts: rollPasts(), traits: rollTraits() }, num = freeNumber();
  Q.addDraft.run(uid, num, JSON.stringify(d), now());
  return { num, ...d };
}
app.get('/api/draft', need, (req, res) => {
  if (Q.agent.get(req.user.id)) return fail(res, 'agent/exists');
  res.json({ draft: draftOf(req.user.id) });
});

// 출입증 발급: 초안에서 고른 후보와 플레이어 입력을 검증해 저장한다
app.post('/api/agent', need, (req, res) => {
  const uid = req.user.id, b = req.body || {};
  if (Q.agent.get(uid)) return fail(res, 'agent/exists');
  const row = Q.draft.get(uid);
  if (!row) return fail(res, 'agent/no-draft');
  const d = JSON.parse(row.data);
  const name = str(b.name, 12), realName = str(b.realName, 20);
  if (!name) return fail(res, 'agent/bad-name');
  if (!realName) return fail(res, 'agent/bad-realname');
  const photo = photoOf(b.photo);
  if (photo === undefined) return fail(res, 'agent/bad-photo');
  b.role = 0;
  if (!isInt(b.role, 0, 1) || !isInt(b.age, 18, 70) || !isInt(b.pastPick, 0, 2) || !isInt(b.traitPick, 0, 2)) return fail(res, 'agent/bad-field');
  const stats = b.stats;
  if (!Array.isArray(stats) || stats.length !== 6 || !stats.every(v => isInt(v, STAT_MIN0, STAT_MAX0)) || stats.reduce((a, c) => a + c, 0) !== STAT_SUM) return fail(res, 'agent/bad-stats');
  const p = d.pasts[b.pastPick], t = d.traits[b.traitPick], trait = t.id || (t.d1 + t.d2);
  if (!isInt(b.free, 0, SKILLS.length - 1)) return fail(res, 'agent/bad-skill');
  const skills = {};
  DEPT_SKILLS[p.dept].forEach(k => { skills[k] = 1; });
  if (skills[SKILLS[b.free]]) return fail(res, 'agent/bad-skill');
  skills[SKILLS[b.free]] = 1;
  const realAge = b.age + 5 * (1 + ((p.dept + p.death + p.secret) % 10));
  const sealed = { mode: 'roll', realName, dept: p.dept, death: p.death, secret: p.secret, cross: str(b.cross, 8).toUpperCase(), realAge };
  let num = row.num;
  try {
    transaction(() => {
      if (Q.agentByNum.get(num)) { num = freeNumber(); }
      Q.addAgent.run({ user_id: uid, num, name, role: 0, head: 0, photo, age: b.age, stats: JSON.stringify(stats), trait, skills: JSON.stringify(skills), res: JSON.stringify(START_RES), created: now() });
      db.prepare('UPDATE agents SET traits = ? WHERE user_id = ?').run(JSON.stringify([{ id: trait, lv: 1 }]), uid);
      Q.addSealed.run(uid, JSON.stringify(sealed));
      Q.dropDraft.run(uid);
    });
  } catch (e) { console.error(e); return fail(res, 'server', 500); }
  const agent = publicAgent(Q.agent.get(uid));
  io.emit('agent', agent);
  res.json({ agent, sealed });
});

// 외관 사진 바꾸기 / 지우기
app.post('/api/agent/photo', need, (req, res) => {
  if (!Q.agent.get(req.user.id)) return fail(res, 'agent/none');
  const photo = photoOf(req.body && req.body.photo);
  if (photo === undefined) return fail(res, 'agent/bad-photo');
  Q.setPhoto.run(photo, req.user.id);
  const agent = publicAgent(Q.agent.get(req.user.id));
  io.emit('agent', agent);
  res.json({ agent });
});

/* ───────── 작전 (샘플: AO-212 「계수자」 재격리) ───────── */
// 판정: d6 주사위 풀. 5·6이 성공 1개. 오염 주사위는 6이면 성공 2개, 1이면 이상 반응. 지원 주사위는 관제팀이 보탠다.
const SN = ['근력', '체력', '민첩', '지능', '육감', '의지'];
const REGEN_MS = Number(process.env.REGEN_HOURS || 6) * 3600 * 1000;
const MAX_TRAITS = 5, TRAIT_MAX_LV = 3;
// 변질 특성 효과 (서버 계산용): 능력치 보정 [능력치 번호, +n], HP·정신력 보정
// 변질 특성이 주는 기능 보너스 (두 번째 그림자 은신 +1, 겹친 목소리 통신 +1, 굳은 관절 은신 −1, 개체의 표식 은신 +2)
const TRAIT_SKILL = { 1: ['은신', 1], 15: ['통신', 1], 17: ['은신', -1], 25: ['은신', 2] };
/* 능력치 1~20 체계: 평균 10. 생성 때 합계 60, 하나당 3~15. 훈련 상한 20, 넘는 길은 변질뿐. */
const STAT_SUM = 60, STAT_MIN0 = 3, STAT_MAX0 = 15, STAT_CAP = 20, DICE_MAX = 14;
const TRAIT_STAT = { 2: [1, 4], 3: [5, 4], 5: [2, 2], 10: [3, 2], 11: [4, 4], 14: [1, 2], 17: [0, 2], 18: [4, 2], 21: [3, 2], 22: [5, 2] };
const TRAIT_HP = { 6: 25 }, TRAIT_SAN = { 16: 30 }; // 최종 수치에 더한다
// 예전 1~7 체계(합계 24)로 만든 요원은 ×2+2로 옮긴다
for (const a of db.prepare('SELECT user_id, stats FROM agents').all()) { const st = JSON.parse(a.stats); if (st.reduce((x, y) => x + y, 0) < 40) db.prepare('UPDATE agents SET stats = ? WHERE user_id = ?').run(JSON.stringify(st.map(v => v * 2 + 2)), a.user_id); }
const DIFF_NAME = { 1: '쉬움', 2: '보통', 3: '어려움', 4: '극한' };

// 거리: 개체와 대열 사이 거리(m). from은 개체가 있는 쪽(front 앞, rear 뒤). speed는 한 턴에 개체가 다가오는 거리.
// 선택지: pos는 대열 위치(front 선두, mid 중열, rear 후미, 없으면 누구나), max/min은 필요한 거리, adv:false면 장면이 넘어가지 않는다.
const RADAR_RANGE = 50, STEP_M = 10, RECOIL_M = 8, COUNT_EVERY = 4, MINION_CAP = 4;
// 형상: 거리가 가까울수록 개체의 몸이 드러난다 (25m 이상, 11~24m, 6~10m, 5m 이하)
const sightStage = d => d >= 25 ? 0 : d > 10 ? 1 : d > 5 ? 2 : 3;
const OPS = [{
  id: 'R8-14', title: 'AO-212 「계수자」 재격리', threat: 'Ⅲ', mins: 40, field: 4, ctrl: 0, level: 3, rec: { field: [3, 4], ctrl: [0, 0] }, count: 8, countEvery: 4, entity: 'AO-212',
  countLabel: '계수자의 셈', tickText: '복도 벽의 숫자가 하나 줄었다. 셈은 기다려 주지 않는다.',
  endText: 'AO-212가 셈을 마쳤다. 복도의 모든 소리가 한꺼번에 사라졌다.', endHp: -4, failText: '격리동 B-3을 봉쇄한다.', reward: { supply: 40, research: 2 },
  brief: '격리동 B-3에서 AO-212의 소리 차단 장치가 꺼졌습니다. 개체가 셈을 마치기 전에 장치를 다시 설치하고 귀환하십시오.',
  contact: { text: '어둠 속에서 손가락이 뻗어 나왔다. {name} 요원의 몸 어딘가가 하나 세어졌다.', hp: -2, san: -1, count: -1 },
  forms: ['어둠 너머에서 셈 소리만 들린다. 형체는 보이지 않는다.', '복도 끝 어둠 속에 사람보다 머리 하나는 큰 윤곽이 서 있다.', '무릎까지 늘어진 두 팔이 보인다. 손마다 손가락이 너무 많고, 하나씩 접히고 있다.', '그것이 고개를 든다. 접힌 손가락 마디마다 숫자가 새겨져 있다.'],
  minion: { name: '세어진 것', hp: 3, text: '이마에 숫자가 새겨진 사람 형체. 셈에 붙들린 이전 직원들의 모습을 하고 있다.',
    spawn: { front: '대열 앞의 어둠에서 세어진 것이 걸어 나왔다. 이마의 숫자가 깜빡인다.', rear: '대열 뒤에서 세어진 것이 일어섰다. 방금 지나온 바닥에 누워 있던 것이다.' },
    hit: '세어진 것이 {pos} {name} 요원에게 달려들었다.', kill: '{name} 요원이 세어진 것을 쓰러뜨렸다. 이마의 숫자가 꺼졌다.' },
  scenes: [
    { title: 'B-3 복도 진입', from: 'front', dist: 40, speed: 5,
      map: { len: 60, start: 5, marks: [{ at: 0, label: '진입구', kind: 'door' }, { at: 14, label: '숫자가 적힌 벽' }, { at: 28, label: '깨진 형광등' }, { at: 42, label: '배전반' }, { at: 58, label: '격리실 B-3 문', kind: 'door' }] },
      text: '형광등이 깜빡일 때마다 벽에 적힌 숫자가 하나씩 줄어든다. 복도 끝에서 누군가 낮게 수를 세는 소리가 들린다.',
      choices: [
        { id: 'sneak', label: '격리실 문까지 숨어서 접근한다', pos: 'front', max: 15, stat: 2, skill: '은신', diff: 2,
          crit: '발소리 하나 내지 않고 문 앞에 닿았다. 셈 소리가 잠시 멎었다.', ok: '숨을 죽인 채 문 앞에 닿았다.', cost: { text: '바닥의 유리 조각을 밟았다. 발바닥이 찢어졌다.', hp: -1 }, fail: { text: '발소리가 울렸다. 셈 소리가 빨라진다.', san: -1, count: -1 } },
        { id: 'read', label: '벽의 숫자를 해독한다', pos: 'mid', stat: 3, skill: '기록 해독', diff: 2, adv: false,
          crit: { text: '숫자는 남은 사람의 수였다. 셈의 규칙을 알아내 셈을 늦췄다.', count: 2 }, ok: { text: '숫자가 줄어드는 간격을 기록했다. 셈이 하나 늦어졌다.', count: 1 }, cost: { text: '숫자를 오래 들여다본 탓에 머릿속에서 셈이 멈추지 않는다.', san: -1 }, fail: { text: '숫자를 소리 내어 읽고 말았다. 벽의 숫자가 둘 줄었다.', count: -2 } },
        { id: 'light', label: '뒤에서 조명을 확보한다', pos: 'rear', stat: 3, skill: '장비 운용', diff: 1, adv: false,
          crit: { text: '복도 전체에 조명이 들어왔다. 다음 판정이 수월해진다.', sup: 2 }, ok: { text: '손전등 각도를 맞췄다. 다음 판정이 수월해진다.', sup: 1 }, cost: { text: '조명은 켰지만 깜빡임이 셈과 박자를 맞추기 시작했다.', san: -1, sup: 1 }, fail: { text: '전구가 터졌다. 소리에 셈이 빨라진다.', count: -1 } }
      ] },
    { title: '계수자와 조우', from: 'front', dist: 15, speed: 5, spawn: { front: 1 },
      map: { len: 30, start: 3, marks: [{ at: 0, label: '격리실 문', kind: 'door' }, { at: 8, label: '관찰창' }, { at: 16, label: '쓰러진 의자' }, { at: 28, label: '격리실 안쪽 벽', kind: 'wall' }] },
      text: '격리실 문틈으로 AO-212가 보인다. 그것은 손가락을 접으며 수를 센다. 셈 소리는 요원들 중 누군가의 목소리를 닮아 간다.',
      choices: [
        { id: 'force', label: '힘으로 제압한다', pos: 'front', max: 5, stat: 0, skill: '격투', diff: 3,
          crit: '개체를 벽에 밀어붙였다. 셈이 끊겼다.', ok: '개체를 구석으로 몰아넣었다.', cost: { text: '몰아넣었지만 개체의 손가락이 팔을 셌다.', hp: -3 }, fail: { text: '닿는 순간 팔의 감각이 하나씩 세어져 사라졌다.', hp: -4, cont: 5 } },
        { id: 'sense', label: '진짜 위치를 짚어 낸다', pos: 'front', max: 10, stat: 4, skill: '관찰', diff: 2,
          crit: '목소리는 미끼였다. 개체의 실제 위치를 정확히 짚었다.', ok: '개체가 서 있는 위치를 찾아냈다.', cost: { text: '찾아냈지만 개체와 눈이 마주쳤다.', cont: 5 }, fail: { text: '목소리를 쫓다 엉뚱한 벽을 짚었다.', hp: -2, count: -1 } },
        { id: 'count', label: '대신 수를 세어 주의를 끈다', pos: 'mid', max: 15, stat: 5, skill: '인내', diff: 3,
          crit: '개체가 요원의 셈을 따라 하기 시작했다. 셈이 거꾸로 돌아간다.', ok: '개체의 시선이 요원에게 고정됐다. 틈이 생겼다.', cost: { text: '셈을 맞추는 동안 자기 이름이 기억나지 않았다.', san: -2 }, fail: { text: '셈이 엇갈렸다. 개체가 요원의 숫자를 지웠다.', san: -2, count: -1, cont: 5 } },
        { id: 'flare', label: '섬광탄으로 밀어낸다', pos: 'rear', stat: 2, skill: '사격', diff: 2, adv: false,
          crit: { text: '섬광이 터졌다. 개체가 복도 끝까지 물러났다.', push: 15 }, ok: { text: '섬광에 개체가 뒤로 물러났다.', push: 10 }, cost: { text: '개체는 물러났지만 눈앞에 숫자가 잔상으로 남았다.', push: 10, san: -1 }, fail: { text: '섬광탄이 불발됐다.', count: -1 } }
      ] },
    { title: '소리 차단 장치 설치', from: 'front', dist: 10, speed: 5, spawn: { rear: 1 },
      map: { len: 24, start: 4, marks: [{ at: 0, label: '격리실 문', kind: 'door' }, { at: 5, label: '1번 모서리 장치' }, { at: 10, label: '2번 모서리 장치' }, { at: 15, label: '3번 모서리 장치' }, { at: 20, label: '4번 모서리 장치' }, { at: 24, label: '안쪽 벽', kind: 'wall' }] },
      text: '장치를 격리실 네 모서리에 다시 붙여야 한다. 개체는 아직 셈을 멈추지 않았다.',
      choices: [
        { id: 'install', label: '장치를 설치한다', pos: 'mid', max: 10, stat: 3, skill: '장비 운용', diff: 2,
          crit: '네 모서리가 동시에 켜졌다. 격리실이 완전히 조용해졌다.', ok: '장치가 하나씩 켜졌다. 셈 소리가 멀어진다.', cost: { text: '마지막 장치를 붙이는 사이 손등이 숫자로 뒤덮였다.', hp: -1, cont: 5 }, fail: { text: '장치가 튀었다. 소음에 개체가 셈을 서두른다.', count: -2 } },
        { id: 'hold', label: '몸으로 막아 시간을 번다', pos: 'front', max: 5, stat: 1, skill: '인내', diff: 2,
          crit: '개체의 셈을 끝까지 버텨 냈다. 동료가 장치를 붙였다.', ok: '버티는 사이 장치가 연결됐다.', cost: { text: '버텼지만 몸 곳곳에 숫자 자국이 남았다.', hp: -2 }, fail: { text: '버티지 못하고 쓰러졌다.', hp: -3, count: -1 } },
        { id: 'cover', label: '엄호 사격으로 묶어 둔다', pos: 'rear', stat: 2, skill: '사격', diff: 2, adv: false,
          crit: { text: '탄이 개체의 손가락을 정확히 맞혔다. 개체가 크게 물러났다.', push: 15 }, ok: { text: '사격에 개체가 움찔하며 물러났다.', push: 10 }, cost: { text: '개체는 물러났지만 탄피 소리를 셈이 따라 센다.', push: 5, count: -1 }, fail: { text: '탄이 빗나갔다.', count: -1 } }
      ] },
    { title: '귀환', from: 'rear', dist: 12, speed: 10, spawn: { rear: 1 },
      map: { len: 60, start: 15, marks: [{ at: 0, label: '격리실 B-3', kind: 'door' }, { at: 22, label: '방화문', kind: 'door' }, { at: 38, label: '숫자가 적힌 벽' }, { at: 60, label: '출구', kind: 'door' }] },
      text: '격리실 문이 닫히기 시작한다. 등 뒤에서 셈 소리가 따라온다. 복도의 숫자가 빠르게 0을 향해 간다.',
      choices: [
        { id: 'run', label: '출구까지 전력으로 이탈한다', pos: 'front', min: 20, stat: 2, skill: '운반', diff: 2,
          crit: '모두가 문이 닫히기 전에 빠져나왔다.', ok: '숨을 몰아쉬며 복도를 벗어났다.', cost: { text: '빠져나왔지만 문틈에 발이 끼었다.', hp: -2 }, fail: { text: '넘어졌다. 등 뒤에서 셈 소리가 다가온다.', hp: -2, count: -2 } },
        { id: 'guide', label: '숫자가 줄지 않는 길을 찾는다', pos: 'mid', min: 10, stat: 4, skill: '관찰', diff: 2,
          crit: '숫자가 줄지 않는 길을 찾았다. 아무도 다치지 않았다.', ok: '숫자가 덜 줄어드는 쪽을 골랐다.', cost: { text: '길을 찾는 사이 숫자 몇 개를 들었다.', san: -1 }, fail: { text: '막다른 길이었다.', count: -2 } },
        { id: 'door', label: '방화문을 닫아 막는다', pos: 'rear', max: 15, stat: 0, skill: '운반', diff: 2, adv: false,
          crit: { text: '방화문이 개체 코앞에서 닫혔다.', push: 20 }, ok: { text: '방화문을 끌어 닫았다. 셈 소리가 멀어졌다.', push: 15 }, cost: { text: '문은 닫혔지만 손가락 하나가 문틈에 끼어 들어왔다.', push: 10, hp: -1 }, fail: { text: '방화문이 녹슬어 꿈쩍하지 않는다.', count: -1 } }
      ] }
  ]
}];

/* ───────── 하층 순찰 (들어갈 때마다 무작위로 만들어지는 작전) ───────── */
// 하층: 심연의 영향으로 뒤틀린 기지 아래층. 구역 3곳을 지나 상층 승강기로 돌아오면 성공이다.
const PATROL_SECTIONS = 3, REACH_M = 6, PATROL_STAB = 15;
OPS.push({ id: 'R8-P', patrol: true, title: '하층 정기 순찰', threat: 'Ⅱ', mins: 30, field: 4, ctrl: 0, level: 2, rec: { field: [2, 4], ctrl: [0, 0] }, count: 8, countEvery: 4, recoil: 12,
  brief: '하층 B3 구역 정기 순찰입니다. 구역 문을 지나 상층 승강기까지 돌아오십시오. 하층의 구조와 배회 개체는 진입할 때마다 바뀝니다.',
  sub: '도면과 배회 개체는 진입할 때마다 바뀝니다. 성공하면 수용 개체의 격리 안정도가 모두 15% 오릅니다.',
  countLabel: '귀환 신호', tickText: '귀환 신호가 약해졌다. 하층의 구조가 또 바뀌고 있다.',
  endText: '귀환 신호가 끊겼다. 대열은 반복되는 복도 속에서 길을 잃었다.', endHp: -3, failText: '순찰 기록을 폐기한다.', reward: { supply: 25, research: 1 }, scenes: [] });
const pick = a => a[die(a.length) - 1];
// 조사: 받침 있으면 a, 없으면 b
const jo = (w, a, b) => { const c = w.charCodeAt(w.length - 1); return w + ((c >= 0xAC00 && c <= 0xD7A3 && (c - 0xAC00) % 28) ? a : b); };
const shuffle = a => { const b = a.slice(); for (let i = b.length - 1; i > 0; i--) { const j = die(i + 1) - 1; [b[i], b[j]] = [b[j], b[i]]; } return b; };
// 배회 개체: 레이더에 잡히는 하층의 존재
const WANDERERS = [
  { name: '배회자', forms: ['발소리가 하나 더 들린다. 형체는 보이지 않는다.', '복도 끝에 제복 차림의 형체가 등을 돌리고 서 있다.', '형체가 걸어온다. 걸음이 대열의 발소리와 정확히 맞는다.', '얼굴이 보인다. 대열 중 누군가와 같은 얼굴이다.'],
    contact: { text: '차가운 손이 {name} 요원의 어깨를 붙잡았다. 그 손에도 같은 넘버가 적혀 있다.', hp: -1, san: -1 } },
  { name: '늘어진 것', forms: ['천장 환풍구에서 무언가 끌리는 소리가 난다.', '천장 패널 사이로 지나치게 긴 팔 하나가 늘어져 있다.', '팔과 다리가 천장에서 거꾸로 늘어져 흔들린다. 관절이 너무 많다.', '그것이 천장에서 떨어져 내린다. 몸통이 없다.'],
    contact: { text: '천장에서 늘어진 팔이 {name} 요원의 목을 감았다.', hp: -2 } },
  { name: '소리 없는 것', forms: ['복도의 소음이 한쪽부터 사라지고 있다.', '어둠 속에서 소리가 빨려 들어가는 지점이 있다.', '형광등 아래에 사람 크기의 빈자리가 있다. 그 안으로 소리가 사라진다.', '자기 숨소리가 들리지 않는다. 그것이 바로 앞에 있다.'],
    contact: { text: '{name} 요원의 목소리가 사라졌다. 입은 움직이는데 아무 소리도 나지 않는다.', san: -2 } }
];
// 하층 존재(하수인): 싸워서 쓰러뜨리는 일반 개체
const CRAWLERS = [
  { name: '뒤틀린 직원', hp: 3, spawn: { front: '앞쪽 사물함 문이 열리고, 관절이 반대로 꺾인 직원이 기어 나왔다.', rear: '대열 뒤에서 관절이 반대로 꺾인 직원이 일어섰다.' }, kill: '{name} 요원이 뒤틀린 직원을 쓰러뜨렸다. 명찰의 이름은 읽을 수 없었다.' },
  { name: '형광 벌레 떼', hp: 2, spawn: { front: '깨진 형광등에서 빛나는 벌레 떼가 쏟아져 나와 앞을 막았다.', rear: '뒤쪽 형광등이 터지며 빛나는 벌레 떼가 쏟아졌다.' }, kill: '{name} 요원이 벌레 떼를 흩어 놓았다. 형광등이 다시 켜졌다.' },
  { name: '기어 다니는 것', hp: 2, spawn: { front: '바닥 배수구에서 손바닥만 한 것들이 기어 나와 길을 막았다.', rear: '지나온 배수구에서 무언가 기어 나와 뒤를 쫓는다.' }, kill: '{name} 요원이 기어 다니는 것을 짓밟았다.' }
];
const ODDITIES = ['자기 넘버가 붙은 사물함', '벽을 가리키는 비상구 표지', 'B3 층 표시', '고인 물에 비친 다른 복도', '꺼진 형광등 구간', '사망자 출입 기록 단말', '안쪽에서 잠긴 격리실', '같은 날짜의 근무 일지', '거꾸로 매달린 소화기', '벽에 긁힌 까마귀 그림', '물이 새는 천장', '멈춘 벽시계'];
// 구역 틀: 장면 설명과 목표 행동
const SECTIONS_T = [
  { title: '반복 복도', bg: 'P-repeat', door: '문틈으로 방금 지나온 것과 같은 복도가 보인다', text: '같은 복도가 세 번째 반복된다. 벽의 층 표시는 계속 B3다.',
    goal: { label: '반복 구간의 끝 문을 연다', stat: 4, skill: '관찰', diff: 2, crit: '반복의 틈을 찾았다. 문 너머로 새 구역이 이어진다.', ok: '구역 문이 열렸다.', cost: { text: '문은 열렸지만 같은 복도를 한 번 더 걸은 기억이 남았다.', san: -1 }, fail: { text: '문 너머도 같은 복도였다.', count: -1 } },
    side: { label: '반복 패턴을 기록한다', pos: 'mid', stat: 3, skill: '기록 해독', diff: 2, crit: { text: '반복의 주기를 알아냈다. 귀환 신호를 보정했다.', count: 2 }, ok: { text: '반복 패턴을 기록했다. 귀환 신호가 조금 맑아졌다.', count: 1 }, cost: { text: '기록하는 사이 같은 문장을 두 번 적었다.', san: -1 }, fail: { text: '기록이 뒤죽박죽이 됐다.', count: -1 } } },
  { title: '사물함 구역', bg: 'P-locker', door: '안쪽에서 금속을 두드리는 소리가 난다', text: '양쪽 벽을 따라 사물함이 끝없이 늘어서 있다. 몇 개는 안쪽에서 두드리는 소리가 난다.',
    goal: { label: '구역 끝 보안문을 해제한다', stat: 3, skill: '장비 운용', diff: 2, crit: '보안문이 소리 없이 열렸다.', ok: '보안문이 열렸다.', cost: { text: '문은 열렸지만 경보음이 울렸다.', count: -1 }, fail: { text: '보안 코드가 맞지 않는다. 사물함 두드리는 소리가 커졌다.', san: -1 } },
    side: { label: '두드리는 사물함을 연다', pos: 'mid', stat: 5, skill: '인내', diff: 2, crit: { text: '안에는 섹터 보급 상자가 있었다. 다음 판정이 수월해진다.', sup: 2 }, ok: { text: '안에서 쓸 만한 장비를 찾았다.', sup: 1 }, cost: { text: '안에는 자기 넘버가 적힌 제복이 걸려 있었다.', san: -2, sup: 1 }, fail: { text: '사물함 안에서 무언가 손을 뻗었다.', hp: -2 } } },
  { title: '침수 통로', bg: 'P-flood', door: '문 아래로 차가운 물이 스며 나온다', text: '발목까지 물이 차 있다. 물에 비친 복도는 실제와 다른 방향으로 뻗어 있다.',
    goal: { label: '물에 비친 길이 아니라 실제 길로 건넌다', stat: 2, skill: '운반', diff: 2, crit: '흔들림 없이 통로를 건넜다.', ok: '통로를 건넜다.', cost: { text: '건넜지만 물속의 무언가가 발목을 스쳤다.', hp: -1 }, fail: { text: '물에 비친 길을 따라가 버렸다.', count: -1, san: -1 } },
    side: { label: '배수 밸브를 연다', pos: 'mid', stat: 0, skill: '운반', diff: 2, crit: { text: '물이 빠지며 바닥의 길이 드러났다. 귀환 신호가 맑아졌다.', count: 1, sup: 1 }, ok: { text: '물이 조금 빠졌다. 다음 판정이 수월해진다.', sup: 1 }, cost: { text: '밸브는 열렸지만 손이 미끄러져 다쳤다.', hp: -1, sup: 1 }, fail: { text: '밸브가 꿈쩍하지 않는다.', count: -1 } } },
  { title: '정전 구역', bg: 'P-dark', door: '문틈이 완전히 어둡다. 빛이 새어 나오지 않는다', text: '조명이 모두 꺼져 있다. 손전등 빛이 닿는 곳만 존재하는 것처럼 보인다.',
    goal: { label: '예비 전원을 켜고 문을 연다', stat: 3, skill: '장비 운용', diff: 2, crit: '구역 전체에 불이 들어왔다. 문이 열렸다.', ok: '예비 전원이 들어왔다. 문이 열렸다.', cost: { text: '전원은 들어왔지만 감전으로 손이 저렸다.', hp: -1 }, fail: { text: '배전반에서 불꽃이 튀었다.', hp: -1, count: -1 } },
    side: { label: '어둠 속 기척을 살핀다', pos: 'mid', stat: 4, skill: '관찰', diff: 2, crit: { text: '기척의 위치를 정확히 짚었다. 개체가 물러났다.', push: 15 }, ok: { text: '기척 쪽으로 빛을 비췄다. 개체가 물러났다.', push: 10 }, cost: { text: '빛을 비춘 곳에 얼굴이 있었다.', san: -2, push: 10 }, fail: { text: '아무것도 찾지 못했다.', count: -1 } } },
  { title: '출입 기록실', bg: 'P-record', door: '문 너머에서 단말 작동음이 웅웅거린다', text: '벽 가득한 단말에 출입 기록이 흐른다. 사망한 직원들의 넘버가 지금도 출입하고 있다.',
    goal: { label: '기록을 위조해 구역 문을 통과한다', stat: 3, skill: '기록 해독', diff: 2, crit: '단말이 대열을 직원으로 인식했다. 문이 열렸다.', ok: '문이 열렸다.', cost: { text: '문은 열렸지만 단말에 자기 사망 일시가 떴다.', san: -1 }, fail: { text: '단말이 경보를 울렸다.', count: -1 } },
    side: { label: '자기 넘버의 기록을 찾아본다', pos: 'mid', stat: 5, skill: '기록 해독', diff: 3, crit: { text: '기록 속에서 잃어버린 기억 한 조각을 찾았다.', san: 2, sup: 1 }, ok: { text: '자기 넘버의 출입 기록을 찾았다. 마지막 출입은 오늘이다.', sup: 1 }, cost: { text: '찾지 말았어야 했다.', san: -2 }, fail: { text: '기록이 빠르게 지워졌다.', count: -1 } } }
];
const REAR_T = [
  { label: '뒤를 막고 시간을 번다', stat: 1, skill: '인내', diff: 2, crit: { text: '추적을 완전히 끊었다.', push: 20 }, ok: { text: '뒤쪽을 막아 시간을 벌었다.', push: 15 }, cost: { text: '막았지만 몸으로 받아 냈다.', push: 10, hp: -1 }, fail: { text: '막아서지 못했다.', count: -1 } },
  { label: '섬광탄으로 밀어낸다', stat: 2, skill: '사격', diff: 2, crit: { text: '섬광에 개체가 크게 물러났다.', push: 20 }, ok: { text: '섬광에 개체가 물러났다.', push: 15 }, cost: { text: '물러났지만 눈앞이 하얗게 번졌다.', push: 10, san: -1 }, fail: { text: '섬광탄이 불발됐다.', count: -1 } }
];
function genMap(len, endLabel) {
  const marks = [{ at: 0, label: '진입문', kind: 'door' }];
  const odd = shuffle(ODDITIES).slice(0, 3), gap = len / 4;
  odd.forEach((t, i) => marks.push({ at: Math.round(gap * (i + 1) + (die(5) - 3)), label: t }));
  marks.push({ at: len, label: endLabel, kind: 'door' });
  return { len, start: 2, marks };
}
/* ───────── 갈림길: 구역 사이마다 도면에 없던 문 두 개가 나타난다 ─────────
   현장팀은 투표로 문을 고른다(동률이면 먼저 고른 쪽, 아무도 고르지 않으면 무작위).
   관제는 관제 행동 하나로 한쪽 문 너머의 감시 카메라를 들여다볼 수 있다. */
const DOOR_RISK = {
  none: { name: '특이 사항 없음', text: '문 너머는 조용하다. 도면과 크게 다르지 않다.' },
  loop: { name: '루프 위험', text: '같은 구간이 되풀이되는 흔적이 있다. 들어가면 절반 확률로 귀환 신호 −1.' },
  nest: { name: '하수인 둥지', text: '바닥과 천장에 무언가 웅크린 형체가 여럿 보인다. 들어가자마자 하수인 2마리.' },
  close: { name: '개체 근접', text: '배회 개체가 문 바로 너머에 있다. 개체가 10m 더 가까이에서 시작한다.' },
  cache: { name: '보급 상자', text: '쓰러진 선반 아래 섹터 보급 상자가 보인다. 들어가면 지원 주사위 +1.' }
};
const RISK_POOL = ['none', 'none', 'loop', 'nest', 'close', 'cache'];
function genSection(t, i) {
  const from = die(10) <= 6 ? 'rear' : 'front', len = 30 + 5 * (die(5) - 1);
  return { title: `${t.title} (하층 B3-${i + 1})`, text: t.text, from, dist: from === 'rear' ? 15 + 5 * die(3) : len + 5 * die(3), speed: 5,
    spawn: die(10) <= 5 ? { [die(2) === 1 ? 'front' : 'rear']: 1 } : null, map: genMap(len, '구역 문'), shiftable: true, bg: t.bg,
    choices: [{ id: 'goal', pos: 'front', reach: true, ...t.goal }, { id: 'side', adv: false, ...t.side }, { id: 'rear', pos: 'rear', adv: false, ...pick(REAR_T) }] };
}
function genFork(pool, i) {
  const ts = shuffle(pool).slice(0, 2), rk = shuffle(RISK_POOL);
  const r2 = rk[0] === rk[1] ? rk.find(x => x !== rk[0]) : rk[1];
  const doors = ts.map((t, k) => ({ id: k ? 'b' : 'a', name: k ? '문 B' : '문 A', tpl: t.title, hint: t.door, risk: k ? r2 : rk[0], scene: genSection(t, i), peeked: false }));
  return { title: '갈림길', fork: true, bg: 'P-fork', text: '복도가 둘로 갈라졌다. 도면에는 없던 문이다. 어느 쪽이든 아까와 같은 곳으로 이어진다는 보장은 없다.', from: 'rear', dist: 60, speed: 0,
    map: { len: 10, start: 5, marks: [{ at: 0, label: '지나온 구역 문', kind: 'door' }, { at: 10, label: '문 A와 문 B', kind: 'door' }] }, choices: [], doors };
}
function genPatrol(base) {
  const w = pick(WANDERERS), cr = pick(CRAWLERS), order = shuffle(SECTIONS_T), first = order[0];
  const scenes = [genSection(first, 0)];
  let used = [first];
  for (let i = 1; i < PATROL_SECTIONS; i++) {
    if (process.env.R8_FORK === '0') { const t = order[i]; used.push(t); scenes.push(genSection(t, i)); continue; }
    const pool = SECTIONS_T.filter(t => !used.includes(t)), f = genFork(pool.length >= 2 ? pool : SECTIONS_T, i);
    used = used.concat(f.doors.map(d => SECTIONS_T.find(t => t.title === d.tpl)));
    scenes.push(f, f.doors[0].scene); // 고르기 전까지 다음 칸은 문 A로 채워 둔다(GM 장면 넘기기용)
  }
  scenes.push({ title: '상층 승강기', bg: 'P-lift', text: '승강기 표시등이 깜빡인다. 층 표시는 B3과 B4 사이를 오간다. 뒤에서 무언가 따라온다.', from: 'rear', dist: 20, speed: 10,
    spawn: { rear: 1 }, map: genMap(40, '상층 승강기'),
    choices: [{ id: 'goal', label: '승강기를 호출해 탑승한다', pos: 'front', reach: true, stat: 2, skill: '운반', diff: 2, crit: '모두가 승강기에 올랐다. 문이 닫혔다.', ok: '승강기 문이 닫혔다. 위로 올라간다.', cost: { text: '문이 닫히기 직전 무언가 손을 들이밀었다.', hp: -2 }, fail: { text: '승강기가 오지 않는다. 표시등이 B4를 가리킨다.', count: -1 } },
      { id: 'side', label: '수동으로 승강기를 끌어올린다', pos: 'mid', stat: 0, skill: '장비 운용', diff: 2, adv: false, crit: { text: '승강기가 빠르게 내려온다.', count: 1, sup: 1 }, ok: { text: '승강기가 내려오고 있다.', sup: 1 }, cost: { text: '손잡이가 손을 할퀴었다.', hp: -1, sup: 1 }, fail: { text: '줄이 끊어질 듯 삐걱댄다.', count: -1 } },
      { id: 'rear', pos: 'rear', adv: false, ...pick(REAR_T) }] });
  return { ...base, entity: w.name, forms: w.forms, contact: w.contact, minion: { name: cr.name, hp: cr.hp, spawn: cr.spawn, kill: cr.kill }, scenes,
    title: `하층 정기 순찰: ${first.title}부터` };
}
// 갈림길 투표 결과로 다음 구역을 정하고 들어간다
function forkResolve(r) {
  const o = D(r), sc = o.scenes[r.scene]; if (!sc || !sc.fork) return;
  const votes = Object.values(r.picks || {}).filter(p => /^fork-[ab]$/.test(p.v)).sort((a, b) => a.at - b.at);
  const n = { a: 0, b: 0 }; votes.forEach(p => { n[p.v.slice(5)]++; });
  let id, why;
  // 아무도 투표하지 않은 첫 라운드는 갈림길에서 기다린다(문 확인 결과를 보고 고를 수 있게). 두 번째부터는 아무 문이나.
  if (!votes.length && !r.forkIdle) { r.forkIdle = 1; say(r, '대열은 갈림길 앞에서 잠시 숨을 골랐다. 다음 라운드에는 어느 쪽이든 들어서야 한다.', { kind: 'move' }); return; }
  if (!votes.length) { id = die(2) === 1 ? 'a' : 'b'; why = '아무도 고르지 않아 대열은 먼저 눈에 띈 문으로 들어섰다.'; }
  else if (n.a === n.b) { id = votes[0].v.slice(5); why = `표가 갈렸다(${n.a}:${n.b}). 먼저 손을 든 쪽을 따른다.`; }
  else { id = n.a > n.b ? 'a' : 'b'; why = `투표 ${n.a}:${n.b}.`; }
  const d = sc.doors.find(x => x.id === id);
  say(r, `${why} 대열은 ${jo(d.name, "을", "를")} 열었다.`, { kind: 'move' });
  const nx = { ...d.scene };
  if (d.risk === 'close') nx.dist = Math.max(8, nx.dist - 10);
  o.scenes[r.scene + 1] = nx;
  r.scene++; r.dist = nx.dist; r.pos = nx.map.start;
  say(r, nx.text, { kind: 'scene', title: nx.title });
  sceneEnter(r);
  if (d.risk === 'loop') { if (Math.random() < 0.5) { r.count = Math.max(0, r.count - 1); say(r, `문을 지나자 방금 지나온 복도가 다시 나타났다. 같은 구간을 한 번 더 걸었다. ${o.countLabel} −1.`, { kind: 'warn', fx: { count: -1 } }); } else say(r, '같은 구간이 되풀이되려다 멈췄다. 운이 좋았다.', { kind: 'sys' }); }
  else if (d.risk === 'nest') { spawnMinion(r, 'front', 1); spawnMinion(r, 'rear', 1); }
  else if (d.risk === 'close') say(r, '문을 열자마자 레이더가 울렸다. 개체가 가까이에 있다.', { kind: 'warn' });
  else if (d.risk === 'cache') { r.support = Math.min(SUPPORT_MAX, r.support + 1); say(r, '쓰러진 선반 아래에서 보급 상자를 찾았다. 다음 판정에 지원 주사위 1개.', { kind: 'sys' }); }
  r._sceneChanged = true;
}

const traitsOf = row => {
  if (row.traits) { try { const t = JSON.parse(row.traits); if (Array.isArray(t)) return t; } catch {} }
  return row.trait ? [{ id: row.trait, lv: 1 }] : [];
};
function modStats(row) {
  const s = JSON.parse(row.stats).slice();
  traitsOf(row).forEach(t => { const b = TRAIT_STAT[t.id]; if (b) s[b[0]] += b[1]; });
  return s;
}
// 수치 배율: 평균 능력치(각 4)일 때 HP 100, 정신력 100이 되도록 곱한다
const HP_MUL = 100 / 12, SAN_MUL = 100 / 7;
function maxes(row) {
  const s = modStats(row), tr = traitsOf(row);
  // 최대치는 5 단위로 끊는다
  const o = s.map(v => (v - 2) / 2); // 1~20 → 예전 척도. 평균 10 → 4
  const hp = Math.round((o[0] + o[1] + 4) * HP_MUL / 5) * 5 + tr.reduce((a, t) => a + (TRAIT_HP[t.id] || 0), 0);
  const san = Math.round((o[5] + 3) * SAN_MUL / 5) * 5 + tr.reduce((a, t) => a + (TRAIT_SAN[t.id] || 0), 0);
  const g = gearFx(row);
  return { hp: hp + g.hp, san: san + g.san };
}
// 오염 한계치: 체력과 의지가 버티는 만큼. 평균(각 4)이면 100, 5 단위.
const contLimit = row => { const s = modStats(row).map(v => (v - 2) / 2); return Math.max(30, Math.round((s[1] + s[5] + 4) * 100 / 12 / 5) * 5) + gearFx(row).lim; };
const QUAR_MS = Number(process.env.QUARANTINE_HOURS || 4) * 3600000;
const VULTURE_OVER = ['"폭주 직전에 잡았다. 운이 좋은 줄 알아."', '"한 번만 더 넘기면 그땐 격리실 쪽에 눕힌다."', '"변이는 기록해 뒀다. 다음엔 표본으로 보게 될지도 모르지."'];
// 피해·회복을 배율에 맞춰 키우고, 매번 ±30% 흔들어 긴박하게
const spread = (base, mul) => base ? Math.sign(base) * Math.max(1, Math.round(Math.abs(base) * mul * (0.7 + (die(61) - 1) / 100))) : 0;
const hpD = b => spread(b, HP_MUL), sanD = b => spread(b, SAN_MUL);
const scaleFx = fx => fx ? { ...fx, hp: hpD(fx.hp || 0) || undefined, san: sanD(fx.san || 0) || undefined } : fx;
// 주사위 풀 굴리기
const skLv = (row, k) => Math.max(0, (JSON.parse(row.skills)[k] || 0) + (gearFx(row).skill[k] || 0) + traitsOf(row).reduce((a, t) => a + (TRAIT_SKILL[t.id] && TRAIT_SKILL[t.id][0] === k ? TRAIT_SKILL[t.id][1] : 0), 0));
const skAll = row => Object.fromEntries(SKILLS.map(k => [k, skLv(row, k)]));
function rollPool(row, statIdx, skill, support, pen) {
  const s = modStats(row), sk = skLv(row, skill);
  // 주사위: 능력치 ÷ 2 − 1 (평균 10이면 4개로 예전과 같다. 2 오를 때마다 하나씩 늘어 20이면 9개) + 기능 등급
  const base = Math.max(1, Math.min(DICE_MAX, Math.floor(s[statIdx] / 2) - 1 + sk - (pen || 0)));
  const black = Math.min(base, Math.floor((row.contamination || 0) / (contLimit(row) / 5)));
  const sup = Math.min(DICE_MAX - base, support || 0);
  const w = Array.from({ length: base - black }, () => die(6)), b = Array.from({ length: black }, () => die(6)), sp = Array.from({ length: sup }, () => die(6));
  const succ = w.filter(v => v >= 5).length + b.reduce((a, v) => a + (v === 6 ? 2 : v === 5 ? 1 : 0), 0) + sp.filter(v => v >= 5).length;
  const anomalies = b.filter(v => v === 1).length;
  return { dice: { w, b, s: sp }, succ, anomalies, stat: SN[statIdx], statVal: s[statIdx], skill, skillLv: sk };
}
const tierOf = (succ, diff) => succ >= diff + 2 ? 'crit' : succ >= diff ? 'ok' : succ === diff - 1 ? 'cost' : 'fail';
const TIER_NAME = { crit: '대성공', ok: '성공', cost: '대가 있는 성공', fail: '실패' };

// 진행 중인 작전 (작전 하나당 하나)
const runs = new Map();
let runSeq = 0;
function lobbyOf(opId) {
  if (!runs.has(opId)) runs.set(opId, { id: ++runSeq, op: opId, status: 'lobby', members: [], order: [], minions: [], mseq: 0, scene: 0, dist: 0, pos: 0, turn: 0, count: 0, support: 0, log: [], result: null, started: null, ended: null });
  return runs.get(opId);
}
OPS.forEach(o => lobbyOf(o.id));
const opDef = id => OPS.find(o => o.id === id);
// 진행 중인 작전의 정의: 무작위 생성 작전(순찰)은 시작할 때 만든 정의를 쓴다
const D = r => r.gen || opDef(r.op);
// 대열: 살아 있는 현장 요원만 민첩 순서로 선다
const lineOf = r => r.order.map(u => r.members.find(m => m.uid === u)).filter(m => m && !m.dead);
function posOf(r, uid) {
  const L = lineOf(r), i = L.findIndex(m => m.uid === String(uid));
  if (i < 0) return null;
  if (L.length === 1) return 'solo';
  return i === 0 ? 'front' : i === L.length - 1 ? 'rear' : 'mid';
}
// 위치 조건: 혼자면 모든 자리, 중열이 없으면(2명 이하) 중열 행동은 누구나
function posOk(r, uid, need) {
  if (!need) return true;
  const p = posOf(r, uid); if (!p) return false;
  if (p === 'solo') return true;
  if (need === 'mid' && lineOf(r).length < 3) return true;
  return p === need;
}
// 관제팀만 레이더의 정확한 값을 받는다. 현장팀과 참관자는 감지 여부만 받는다.
function runOut(r, viewer, gm) {
  const o = D(r), live = r.status === 'live', sc = live ? o.scenes[r.scene] : null;
  const vm = r.members.find(m => m.uid === String(viewer));
  const ctl = !!gm;
  // 관제 NPC가 거리를 늘 알려 준다. 잡음이 끼면 끊긴다(GM은 계속 본다).
  const radar = live ? (r.noise && !gm ? { exact: true, noise: true, dist: null, from: sc.from, speed: sc.speed, range: RADAR_RANGE, detected: true }
    : { exact: true, dist: r.dist, from: sc.from, speed: sc.speed, range: RADAR_RANGE, detected: r.dist <= RADAR_RANGE }) : null;
  return { id: r.id, op: { id: o.id, title: o.title, threat: o.threat, mins: o.mins, field: o.field, ctrl: o.ctrl, count: o.count, brief: o.brief, scenes: o.patrol && !r.gen ? PATROL_SECTIONS * 2 : o.scenes.length,
      patrol: !!o.patrol, level: o.level || 2, rec: o.rec || null, countLabel: o.countLabel, entity: o.entity, reward: o.reward, sub: o.sub || null },
    steps: live ? o.scenes.map((x, i) => o.patrol ? (x.fork ? '갈림길' : i === o.scenes.length - 1 ? '승강기' : i <= r.scene ? x.title.replace(/ \(하층.*$/, '') : '구역') : x.title) : null,
    eta: live ? (() => { const ev = Math.max(1.5, (o.countEvery || COUNT_EVERY) * scaleOf(r).every); return Math.max(0, Math.round(r.count * ev - (r.turn % ev))); })() : null,
    ent: live ? (() => { const st = sightStage(r.dist), c = o.contact || {}; return { name: o.entity, threat: o.threat, stage: st, obs: o.forms.slice(0, st + 1), total: o.forms.length, contact: { hp: c.hp || 0, san: c.san || 0, count: c.count || 0 } }; })() : null,
    status: r.status, scene: live ? { idx: r.scene, bg: sc.bg || null, title: sc.title, text: sc.text, from: sc.from,
      choices: sc.choices.map(c => ({ id: c.id, label: c.label, pos: c.pos || null, max: c.max || null, min: c.min || null, reach: !!c.reach, adv: c.adv !== false, stat: SN[c.stat], skill: c.skill, diff: c.adv !== false && r.fac && r.fac.ease ? Math.max(1, c.diff - 1) : c.diff, diffName: DIFF_NAME[c.adv !== false && r.fac && r.fac.ease ? Math.max(1, c.diff - 1) : c.diff], eased: c.adv !== false && !!(r.fac && r.fac.ease) })) } : null,
    members: r.members.map(m => ({ ...m, pos: m.role === 'field' ? posOf(r, m.uid) : null })), order: r.order, radar, turn: r.turn,
    r1: r.r1 || null,
    picks: live ? Object.entries(r.picks || {}).map(([u, p]) => ({ uid: u, kind: p.kind, v: p.v, label: p.label })) : [], deadline: live ? r.deadline || null : null, roundMs: ROUND_MS, now: now(),
    noise: live && r.noise ? { shown: r.noise.shown, turns: r.noise.turns, left: Math.max(1, 3 - r.noise.turns) } : null,
    shift: live && r.shift ? { kind: r.shift.kind, name: SHIFT_KINDS[r.shift.kind].name, warn: SHIFT_KINDS[r.shift.kind].warn } : null,
    shiftFx: live && r.shiftFx && r.shiftFx.turn === r.turn ? r.shiftFx : null,
    beacon: live && r.beaconUntil > r.turn ? r.beaconUntil - r.turn : 0,
    fork: live && sc.fork ? sc.doors.map(d => ({ id: d.id, name: d.name, hint: d.hint, peeked: d.peeked, tpl: d.peeked ? d.tpl : null, risk: d.peeked ? d.risk : null, riskName: d.peeked ? DOOR_RISK[d.risk].name : null, riskText: d.peeked ? DOOR_RISK[d.risk].text : null, from: d.peeked ? d.scene.from : null })) : null,
    facs: live ? facsOf(r) : [], dark: live ? isDark(r) : false, ease: live ? !!(r.fac && r.fac.ease) : false,
    sight: live ? { stage: sightStage(r.dist), text: o.forms[sightStage(r.dist)], from: sc.from } : null,
    map: live ? { len: sc.map.len, marks: sc.map.marks, pos: r.pos } : null,
    minions: live ? r.minions.map(x => ({ id: x.id, side: x.side, hp: x.hp, hpMax: x.hpMax, name: o.minion.name, intent: isDark(r) && !ctl ? 'unknown' : x.intent || 'bite', intentName: isDark(r) && !ctl ? '?' : INTENT[x.intent || 'bite'], braced: !!x.braced, combo: x.hitTurn === r.turn - 1 ? x.hitBy : null })) : [],
    heal: live ? r.members.filter(canHeal).map(m => ({ id: 'heal-' + m.uid, uid: m.uid, name: m.name, hp: m.hp, hpMax: m.hpMax })) : [],
    recover: live ? r.members.filter(needsRecovery).map(m => ({ id: 'rec-' + m.uid, uid: m.uid, name: m.name, why: m.dead ? 'dead' : 'broken' })) : [],
    attacks: live ? ['front', 'rear'].filter(sd => r.minions.some(x => x.side === sd)).map(sd => ({ id: 'atk-' + sd, side: sd, label: sd === 'front' ? `앞을 막은 ${jo(o.minion.name, '과', '와')} 싸운다` : `뒤에서 덮치는 ${jo(o.minion.name, '과', '와')} 싸운다` })) : [],
    count: r.count, support: r.support, log: r.log.slice(-60), chat: vm ? (r.chat || []).slice(-60) : [], result: r.result, started: r.started, ended: r.ended,
    sealedUntil: entLocked(ENTITIES.find(x => x.op === r.op)) };
}
const emitRun = r => { for (const s of io.sockets.sockets.values()) s.emit('op', runOut(r, s.user && s.user.id, isGM(s.user))); };
const busyIn = uid => { for (const r of runs.values()) if (r.status !== 'done' && r.members.some(m => m.uid === String(uid))) return r; return null; };
/* ───────── 요원 경력 ───────── */
// 출동과 관리 작업으로 경력이 쌓이면 계급이 오르고, 계급마다 능력치 포인트 2를 받는다.
const CAREER = { opSuccess: 20, opOther: 10, emergency: 1, task: { 30: 1, 60: 3, 120: 5, 180: 8 } }, RANK_PTS = 2;
const rankNeed = rk => 60 + 30 * (rk - 1); // 계급 rk → rk+1 (60, 90, 120…)
function rankOf(career) { let rk = 1, c = career; while (c >= rankNeed(rk)) { c -= rankNeed(rk); rk++; } return { rank: rk, cur: c, need: rankNeed(rk) }; }
const setCareer = db.prepare('UPDATE agents SET career = ?, stat_pts = ? WHERE user_id = ?');
function gainCareer(uid, n) {
  const row = Q.agent.get(uid); if (!row) return null;
  const before = rankOf(row.career || 0).rank, after = rankOf((row.career || 0) + n).rank, ups = after - before;
  setCareer.run((row.career || 0) + n, (row.stat_pts || 0) + ups * RANK_PTS, uid);
  return { n, rank: after, ups };
}
/* ───────── 기능 경험치 ───────── */
// 판정에 쓴 기능에 경험치가 꾸준히 쌓인다. 다 차면 등급이 오른다. 등급 0인 기능도 쓰다 보면 익힌다.
const SKILL_MAX = 5, XP_NEED = [20, 50, 90, 140, 200]; // 등급 n → n+1에 필요한 양
const XP = { roll: 2, ctl: 1, emergency: 1, task: { 30: 2, 60: 4, 120: 7, 180: 10 } };
const setSkillXp = db.prepare('UPDATE agents SET skills = ?, skill_xp = ? WHERE user_id = ?');
function gainXp(uid, skill, n) {
  const row = Q.agent.get(uid); if (!row || !SKILLS.includes(skill)) return null;
  const sk = JSON.parse(row.skills), xp = JSON.parse(row.skill_xp || '{}');
  let lv = sk[skill] || 0; if (lv >= SKILL_MAX) return { skill, n: 0, lv, ups: [] };
  xp[skill] = (xp[skill] || 0) + n; const ups = [];
  while (lv < SKILL_MAX && xp[skill] >= XP_NEED[lv]) { xp[skill] -= XP_NEED[lv]; lv++; ups.push(lv); }
  if (lv >= SKILL_MAX) xp[skill] = 0;
  if (lv) sk[skill] = lv;
  setSkillXp.run(JSON.stringify(sk), JSON.stringify(xp), uid);
  return { skill, n, lv, ups };
}
const say = (r, text, extra) => {
  r.log.push({ t: now(), text, ...(extra || {}) });
  // 작전 판정이면 그 기능에 경험치. 등급이 오르면 대열 정보도 새로 고친다.
  if (extra && extra.kind === 'roll' && extra.uid && extra.skill) {
    const g = gainXp(Number(extra.uid), extra.skill, extra.ctl ? XP.ctl : XP.roll), m = r.members.find(x => x.uid === String(extra.uid));
    if (g && g.ups.length && m) {
      m.sk = skAll(Q.agent.get(Number(extra.uid)));
      r.log.push({ t: now(), kind: 'grow', uid: m.uid, text: `${m.name} 요원의 ${g.skill} 기능이 ${g.lv}등급이 되었다.` });
      io.emit('agent', publicAgent(Q.agent.get(Number(extra.uid))));
    }
  }
};

function opJoin(uid, opId, role) {
  const row = Q.agent.get(uid); if (!row) return 'agent/none';
  if (row.regen_until && row.regen_until > now()) return 'op/regen';
  if (row.quar_until && row.quar_until > now()) return 'op/quarantine';
  const r = runs.get(opId); if (!r) return 'op/none';
  if (r.status !== 'lobby') return 'op/started';
  if (entLocked(ENTITIES.find(x => x.op === opId))) return 'op/sealed';
  const other = busyIn(uid); if (other && other !== r) return 'op/busy';
  role = 'field';
  const o = opDef(opId), cap = o.field, prev = r.members.find(m => m.uid === String(uid));
  r.members = r.members.filter(m => m.uid !== String(uid));
  if (r.members.filter(m => m.role === role).length >= cap) return 'op/full';
  const mx = maxes(row);
  r.members.push({ uid: String(uid), name: row.name, num: row.num, role, hp: mx.hp, hpMax: mx.hp, san: mx.san, sanMax: mx.san, cont: 0, sk: skAll(row), cls: (prev && prev.cls) || suggestCls(row), dead: false, broken: false, acted: false, ready: false });
  return null;
}
function opLeave(uid, opId) {
  const r = runs.get(opId); if (!r || r.status !== 'lobby') return 'op/started';
  r.members = r.members.filter(m => m.uid !== String(uid)); return null;
}
// 대기실 준비 완료: 참가자 전원이 준비 완료를 눌러야 작전을 개시할 수 있다.
function opReady(uid, opId, on) {
  const r = runs.get(opId); if (!r || r.status !== 'lobby') return 'op/started';
  const m = r.members.find(x => x.uid === String(uid)); if (!m) return 'op/not-member';
  m.ready = !!on; return null;
}
/* ───────── 라운드: 현장 요원은 라운드마다 행동을 하나씩 고른다 ─────────
   모두 고르거나 제한 시간이 지나면 고른 순서대로 실행하고(고르지 못한 요원은 대기), 그 뒤에 한 턴이 흐른다
   (하수인 행동 → 개체 접근 → 귀환 신호). 관제 행동과 소모품은 고르는 즉시 일어난다. */
/* ───────── 포지션: 대기실에서 작전마다 고른다 ─────────
   돌격: 공격 판정 주사위 +1, 맞히면 피해 +1
   방어: 막기가 대열 전원을 지키고, 그 라운드의 개체 접촉 피해도 절반
   치유: 응급 처치(HP)와 정신 안정(정신력)을 할 수 있다
   기술: 관제(NPC 벌처)에 시설 조작·비컨·문 확인·지원 연결·잡음 해독을 요청한다 */
const CLS = { atk: '돌격', def: '방어', med: '치유', tech: '기술' };
function suggestCls(row) {
  const sk = skAll(row) || {}, sc = { atk: (sk['격투'] || 0) + (sk['사격'] || 0), def: (sk['인내'] || 0) + (sk['운반'] || 0), med: (sk['응급처치'] || 0) * 2, tech: (sk['장비 운용'] || 0) + (sk['통신'] || 0) + (sk['기록 해독'] || 0) };
  return Object.entries(sc).sort((a, b) => b[1] - a[1])[0][1] ? Object.entries(sc).sort((a, b) => b[1] - a[1])[0][0] : 'atk';
}
function opCls(uid, opId, cls) {
  const r = runs.get(opId); if (!r || r.status !== 'lobby') return 'op/started';
  const m = r.members.find(x => x.uid === String(uid)); if (!m) return 'op/not-member';
  if (!CLS[cls]) return 'op/bad-choice';
  m.cls = cls; return null;
}
function classAct(v) {
  let x;
  if (v === 'sup') return { k: 'support', cls: 'tech' };
  if ((x = /^fac-(\w+)$/.exec(v))) return { k: 'fac', t: x[1], cls: 'tech' };
  if ((x = /^peek-([ab])$/.exec(v))) return { k: 'peek', t: x[1], cls: 'tech' };
  if (v === 'decode' || v === 'jam') return { k: v, cls: 'tech' };
  if ((x = /^calm-(.+)$/.exec(v))) return { k: 'stabilize', t: x[1], cls: 'med' };
  if (v === 'raven1') return { k: 'raven1' };
  return null;
}
// 고를 때 미리 확인한다(실행은 라운드 끝에)
function classPre(r, m, A) {
  if (A.cls && m.cls !== A.cls) return ['op/cls'];
  const sc = D(r).scenes[r.scene];
  if (A.k === 'support') return [null, '관제 지원 연결'];
  if (A.k === 'raven1') return r.r1 ? ['op/raven1-used'] : [null, 'RAVEN-1 지원 요청'];
  if (A.k === 'decode' || A.k === 'jam') return r.noise ? [null, A.k === 'decode' ? '신호 해독' : '회선 차단'] : ['op/no-noise'];
  if (A.k === 'peek') { const d = sc.fork && sc.doors.find(x => x.id === A.t); if (!d) return ['op/bad-choice']; if (d.peeked) return ['op/used']; if (r.noise) return ['op/noise']; return [null, `${d.name} 확인 요청`]; }
  if (A.k === 'stabilize') { const t = r.members.find(x => x.uid === A.t && x.role === 'field' && !x.dead); return !t || t.san >= t.sanMax ? ['op/no-target'] : [null, `${t.name} 정신 안정`]; }
  if (A.k === 'fac') { const F = facsOf(r).find(x => x.id === A.t); if (!F) return ['op/bad-choice']; if (F.on) return ['op/used']; if (F.id === 'wall' && !F.need) return ['op/no-target']; return [null, `${F.name} 조작 요청`]; }
  return ['op/bad-choice'];
}
const ROUND_MS = Math.max(15, Number(process.env.ROUND_SECONDS || 60)) * 1000;
const canPick = m => m.role === 'field' && !m.dead && !m.broken;
function pickCheck(r, uid, kind, v) {
  const m = fieldMember(r, uid), o = D(r), sc = o.scenes[r.scene], p = posOf(r, uid), L = lineOf(r);
  if (sc.fork) {
    const fm = /^fork-([ab])$/.exec(v);
    if (kind === 'act' && fm) return [null, `${sc.doors.find(d => d.id === fm[1]).name} 쪽에 한 표`];
    if (kind === 'move' && (v === 'fwd' || v === 'back')) return ['op/fork'];
    if (kind === 'act' && !/^(rec-|heal-|calm-|peek-|sup$|raven1$|decode$|jam$)/.test(v)) return ['op/fork'];
  }
  if (kind === 'move') {
    if (v === 'wait') return [null, '대기'];
    if (v === 'swap') return L.findIndex(x => x.uid === m.uid) <= 0 ? ['op/no-ahead'] : [null, '자리 바꾸기'];
    if (v === 'fwd' || v === 'back') {
      if (v === 'fwd' && !(p === 'front' || p === 'solo')) return ['op/pos'];
      if (v === 'back' && !(p === 'rear' || p === 'solo')) return ['op/pos'];
      if (r.minions.some(x => x.side === (v === 'fwd' ? 'front' : 'rear'))) return [v === 'fwd' ? 'op/blocked-front' : 'op/blocked-rear'];
      return [null, v === 'fwd' ? '전진' : '후퇴'];
    }
    return ['op/bad-choice'];
  }
  const CA = classAct(v); if (CA) return classPre(r, m, CA);
  let mt;
  if ((mt = /^(atk|shv)-(front|rear)$/.exec(v))) {
    const side = mt[2], foe = r.minions.find(x => x.side === side); if (!foe) return ['op/no-foe'];
    const melee = p === 'solo' || p === side, ranged = p === 'mid' || (L.length === 2 && p !== side);
    if (mt[1] === 'shv') return melee ? [null, `밀쳐 내기 (${side === 'front' ? '앞' : '뒤'})`] : ['op/pos'];
    return melee || ranged ? [null, `${melee ? '공격' : '사격'} (${side === 'front' ? '앞' : '뒤'})`] : ['op/pos'];
  }
  if (v === 'guard') return m.cls === 'def' ? (r.minions.length || r.dist <= 10 ? [null, '엄호'] : ['op/no-foe']) : r.minions.length ? [null, '막기'] : ['op/no-foe'];
  if (v.startsWith('rec-')) { const t = r.members.find(x => x.uid === v.slice(4)); return !t || !needsRecovery(t) || t.uid === m.uid ? ['op/no-target'] : [null, `${t.name} 회수`]; }
  if (v.startsWith('heal-')) { const t = r.members.find(x => x.uid === v.slice(5)); if (m.cls !== 'med') return ['op/cls']; return !t || !canHeal(t) ? ['op/no-target'] : [null, t.uid === m.uid ? '자가 응급 처치' : `${t.name} 응급 처치`]; }
  const c = sc.choices.find(x => x.id === v); if (!c) return ['op/bad-choice'];
  if (!posOk(r, uid, c.pos)) return ['op/pos'];
  if (c.reach && r.pos < sc.map.len - REACH_M) return ['op/not-there'];
  return [null, c.label];
}
function opPick(uid, opId, kind, v) {
  const r = runs.get(opId), err = canField(r, uid); if (err) return err;
  if (r.batch) return 'op/resolving';
  const [e, label] = pickCheck(r, uid, kind, String(v || '')); if (e) return e;
  r.picks = r.picks || {};
  if (r.picks[String(uid)] && r.picks[String(uid)].v === 'peek-done') return 'op/used';
  // 문 너머 확인은 고르는 즉시 일어난다: 그래야 동료가 보고 투표할 수 있다. 이번 라운드의 행동으로 친다.
  if (kind === 'act' && /^peek-[ab]$/.test(String(v))) {
    const e = classExec(r, fieldMember(r, uid), 'peek', String(v).slice(5)); if (e) return e;
    r.picks[String(uid)] = { kind, v: 'peek-done', label, at: now(), scene: r.scene };
    if (roundReady(r)) resolveRound(r);
    return null;
  }
  r.picks[String(uid)] = { kind, v: String(v), label, at: now(), scene: r.scene };
  if (roundReady(r)) resolveRound(r);
  return null;
}
// 라운드는 현장 전원이 고르고, 관제 전원이 관제 행동을 하거나 넘기면 끝난다. 시간이 다 되면 그대로 진행한다.
function roundReady(r) {
  const P = r.picks || {};
  return r.members.filter(canPick).every(m => P[m.uid]);
}
function opUnpick(uid, opId) {
  const r = runs.get(opId); if (!r || r.status !== 'live') return 'op/not-live';
  if (r.batch) return 'op/resolving';
  if (r.picks && r.picks[String(uid)] && r.picks[String(uid)].v === 'peek-done') return 'op/used';
  if (r.picks) delete r.picks[String(uid)];
  return null;
}
function startRound(r) {
  clearTimeout(r._timer); r.picks = {};
  if (r.status !== 'live') return;
  r.deadline = now() + ROUND_MS;
  r._timer = setTimeout(() => { if (r.status === 'live') { resolveRound(r); emitRun(r); } }, ROUND_MS + 250);
}
function resolveRound(r) {
  if (r.status !== 'live' || r.batch) return;
  clearTimeout(r._timer);
  const picks = r.picks || {}, sc0 = r.scene;
  const late = r.members.filter(m => canPick(m) && !picks[m.uid]);
  if (late.length) say(r, `시간이 다 됐다. 고르지 못한 ${late.map(m => m.name).join(', ')} 요원은 대기한다.`, { kind: 'move' });
  r.batch = true; r._sceneChanged = false;
  for (const [uid, p] of Object.entries(picks).sort((a, b) => a[1].at - b[1].at)) {
    if (r.status !== 'live') break;
    const m = r.members.find(x => x.uid === uid); if (!m) continue;
    if (r.scene !== sc0) { say(r, `장면이 바뀌어 ${m.name} 요원의 「${p.label}」은(는) 취소됐다.`, { kind: 'move', uid }); continue; }
    if (/^fork-[ab]$/.test(p.v) || p.v === 'peek-done') continue;
    if (p.kind === 'move' && p.v === 'wait') { say(r, `${m.name} 요원은 자리를 지키며 기다렸다.`, { kind: 'move', uid }); continue; }
    const e = p.kind === 'move' ? moveExec(r, uid, p.v) : actExec(r, uid, p.v);
    if (e && r.status === 'live') say(r, `${m.name} 요원의 「${p.label}」은(는) 상황이 바뀌어 할 수 없었다.`, { kind: 'move', uid });
  }
  if (r.status === 'live' && r.scene === sc0 && D(r).scenes[r.scene].fork) forkResolve(r);
  r.batch = false; r.picks = {};
  if (r.status === 'live') endTurn(r, r._sceneChanged ? false : undefined);
  if (r.status === 'live') startRound(r);
}
function opStart(uid, opId) {
  const r = runs.get(opId); if (!r || r.status !== 'lobby') return 'op/started';
  if (!r.members.some(m => m.uid === String(uid))) return 'op/not-member';
  if (!r.members.some(m => m.role === 'field')) return 'op/no-field';
  if (r.members.some(m => !m.ready)) return 'op/not-ready';
  if (entLocked(ENTITIES.find(x => x.op === opId))) return 'op/sealed';
  const base = opDef(opId);
  r.gen = base.patrol ? genPatrol(base) : null;
  const o = D(r);
  // 인원 보정 기준: 현장 인원
  r.nf = r.members.filter(m => m.role === 'field').length; r.nt = r.nf;
  r.status = 'live'; r.started = now(); r.noise = null; r.shift = null; r.shiftDone = false; r.shiftFx = null; r.beaconUntil = -1; r.sceneT0 = 0; r.ctlUsed = {}; r.doorUsed = false; r.fac = {}; r.count = o.count; r.scene = 0; r.support = 0; r.log = []; r.turn = 0; r.dist = o.scenes[0].dist; r.pos = o.scenes[0].map.start;
  const agi = m => { const row = Q.agent.get(Number(m.uid)); return row ? modStats(row)[2] : 0; };
  r.order = r.members.filter(m => m.role === 'field').map((m, i) => ({ uid: m.uid, a: agi(m), i })).sort((a, b) => b.a - a.a || a.i - b.i).map(x => x.uid);
  say(r, `작전 ${o.id} 개시. 현장팀 ${r.nf}명 (${r.members.map(m => `${m.name} ${CLS[m.cls] || ''}`).join(', ')}). 관제는 RAVEN-3 벌처가 무전으로 맡는다.`, { kind: 'sys' });
  r.band = 0;
  say(r, `대열 편성: ${lineOf(r).map(m => m.name).join(' → ')} (민첩이 높은 요원이 선두에 선다)`, { kind: 'sys' });
  r.minions = []; r.mseq = 0;
  say(r, o.scenes[0].text, { kind: 'scene', title: o.scenes[0].title });
  sceneEnter(r);
  startRound(r);
  return null;
}
/* 기능의 상시 효과 */
const STEALTH_DODGE = 0.15, GRIT_CUT = 0.15, COMMS_ESC = 0.15; // 등급당. 5등급이면 75%
const lvOf = (m, k) => (m && m.sk && m.sk[k]) || 0;
// 인내: 정신력 피해를 등급당 15% 줄인다 (5등급 75%)
const grit = (m, v) => v < 0 ? Math.min(-1, Math.round(v * (1 - Math.min(0.75, GRIT_CUT * lvOf(m, '인내'))))) : v;
function applyFx(r, m, fx) {
  if (!fx) return;
  if (fx.hp) m.hp = Math.max(0, Math.min(m.hpMax, m.hp + fx.hp));
  if (fx.san) m.san = Math.max(0, Math.min(m.sanMax, m.san + fx.san));
  if (fx.cont) m.cont += fx.cont;
  if (fx.count) r.count = Math.max(0, Math.min(D(r).count, r.count + fx.count));
  if (fx.sup) r.support = Math.min(4, r.support + fx.sup);
  if (fx.push) r.dist = Math.min(60, r.dist + fx.push);
}
function checkBodies(r) {
  r.members.forEach(m => {
    if (!m.dead && m.hp <= 0) { m.dead = true; m.recovered = false; say(r, `${m.name} 요원의 생체 신호가 끊겼다. 동료가 회수하지 못하면 RAVEN-4가 출동한다.`, { kind: 'death', uid: m.uid }); }
    if (!m.dead && !m.broken && m.san <= 0) { m.broken = true; m.recovered = false; say(r, `${m.name} 요원의 정신이 붕괴했다. 치유 요원이 정신을 안정시키거나 동료가 회수해야 한다.`, { kind: 'warn', uid: m.uid }); }
  });
}
const fieldMember = (r, uid) => r.members.find(x => x.uid === String(uid) && x.role === 'field');
function canField(r, uid) {
  if (!r || r.status !== 'live') return 'op/not-live';
  const m = fieldMember(r, uid);
  if (!m) return 'op/not-field';
  if (m.dead) return 'op/dead';
  if (m.broken) return 'op/broken';
  return null;
}
// 하수인: 대열 앞(front)이나 뒤(rear)에 붙어 길을 막고, 턴마다 그쪽 끝 요원을 공격한다
/* 인원 보정: 인원이 많을수록 압박이 커지고, 적을수록 완화된다(작전 시작 때 인원 기준). 시뮬레이터(봇 수천 판)로 맞춘 값이다.
   관제를 NPC로 바꾸면서(현장 요원이 시설 조작에 차례를 쓴다) 3~4인 값을 다시 맞췄다.
   시간 압박은 현장 인원으로 정한다 — every: 귀환 신호 감소 주기 배수 · speed: 개체 접근 속도 배수
   하수인은 현장 인원으로 정한다 — hp: 하수인 체력 + · spawn: 장면·시간 경과 때 더 나오는 수 · cap: 시간 경과로 나올 때 상한
   R8_SCALE 환경 변수(JSON, {"time":{…},"foe":{…}})로 덮어쓸 수 있다. */
const SCALE_ENV = (() => { try { return JSON.parse(process.env.R8_SCALE || '{}'); } catch { return {}; } })();
const SCALE_T = Object.assign({ 1: { every: 1.3, speed: 0.75 }, 2: { every: 1.1, speed: 0.9 }, 3: { every: 1.2, speed: 0.9 }, 4: { every: 1.1, speed: 1 } }, SCALE_ENV.time || {});
const SCALE_F = Object.assign({ 1: { hp: 0, spawn: 0, cap: 2 }, 2: { hp: 0, spawn: 0, cap: 2 }, 3: { hp: 0, spawn: 0, cap: 2 }, 4: { hp: 1, spawn: 0, cap: 3 } }, SCALE_ENV.foe || {});
const scaleOf = r => ({ ...SCALE_T[Math.min(4, Math.max(1, r.nt || 1))], ...SCALE_F[Math.min(4, Math.max(1, r.nf || 1))] });
function spawnMinion(r, side, n) {
  const o = D(r);
  for (let i = 0; i < n && r.minions.length < MINION_CAP; i++) {
    const hp = o.minion.hp + scaleOf(r).hp;
    r.minions.push({ id: ++r.mseq, side, hp, hpMax: hp, intent: pickIntent(r) });
    say(r, o.minion.spawn[side], { kind: 'spawn' });
  }
}
/* ───────── 관제 시설 (기지의 손) ─────────
   장면마다 시설이 세 가지 있다. 관제는 라운드마다 관제 행동 하나로 시설 하나를 조작한다(지원 주사위·바이탈 안정과 같은 몫).
   · 구역 전원(쉬움): 켜지 않으면 구역은 비상등 — 현장은 하수인의 다음 행동을 「?」로 보고, 장면 판정 주사위가 하나 줄어든다.
     켜는 순간 빛에 하수인이 비틀거린다. 관제가 없는 작전은 끝까지 비상등이다.
   · 장면 시설(보통): 장면마다 하나. 목표 판정을 한 단계 쉽게 하거나(ease), 지름길로 대열을 앞당긴다(short).
   · 격벽(어려움): 작전당 한 번. 개체를 8m(대성공 12m) 밀어낸다. */
const FAC_SP = [
  { name: '배수 펌프', kind: '설비', type: 'ease', eff: '목표 판정이 한 단계 쉬워진다' },
  { name: '방화문', kind: '문', type: 'short', eff: '지름길이 열려 대열이 10m 앞당겨진다' },
  { name: '출입 단말', kind: '단말', type: 'ease', eff: '잠금이 풀려 목표 판정이 한 단계 쉬워진다' },
  { name: '환기 장치', kind: '설비', type: 'ease', eff: '시야를 가리던 안개가 빠져 목표 판정이 한 단계 쉬워진다' }
];
function facsOf(r) {
  if (!r.fac) r.fac = {};
  if (D(r).scenes[r.scene].fork) return [];
  const o = D(r), last = r.scene === o.scenes.length - 1, sp = last ? { name: '화물 승강기', kind: '이동', type: 'ease', eff: '승강기를 미리 불러 귀환 판정이 한 단계 쉬워진다' } : FAC_SP[r.scene % FAC_SP.length];
  const from = o.scenes[r.scene].from;
  return [
    { id: 'pwr', name: '구역 전원', kind: '전력', diff: 1, eff: '조명이 돌아와 현장 시야가 트인다(하수인 예고 공개, 판정 주사위 −1 해제)', on: !!r.fac.pwr, state: r.fac.pwr ? '정상 전력' : '비상등', x: 20 },
    { id: 'sp', name: sp.name, kind: sp.kind, type: sp.type, diff: 2, eff: sp.eff, on: !!r.fac.sp, state: r.fac.sp ? '작동 중' : sp.type === 'short' ? '잠김' : '정지', x: 62 },
    { id: 'wall', name: '격벽', kind: '격벽', diff: 3, eff: '개체가 8m 물러난다 (작전당 한 번)', on: !!r.doorUsed, state: r.doorUsed ? '사용함' : '열림', x: from === 'rear' ? 6 : 94, need: r.dist <= RADAR_RANGE }
  ].concat(r.shift ? [{ id: 'beacon', name: '기준점 비컨', kind: '비컨', diff: 2, skill: '통신', eff: `원래 도면 좌표를 송출해 이번 변형(${SHIFT_KINDS[r.shift.kind].name})을 막는다. 심연도 이 신호를 듣는다: ${BEACON_TURNS}라운드 동안 잡음 확률 2배`, on: false, state: '대기', x: 50 }] : []);
}
const isDark = r => !(r.fac && r.fac.pwr);
/* ───────── 하층 구조 변형 ─────────
   하층은 기지 구조가 꼬여 있고 계속 확장·변형된다. 구역마다 한 번, 관제 계기판에 변형이 먼저 잡히고
   다음 턴이 끝날 때 구조가 바뀐다. 관제는 그 사이에 기준점 비컨(지능 + 통신, 보통)으로 변형을 막을 수 있다.
   비컨은 원래 도면 좌표를 송출하는 장치라 심연도 그 신호를 듣는다: 켜면 3라운드 동안 잡음 확률이 두 배가 된다. */
const SHIFT_CHANCE = Number(process.env.R8_SHIFT ?? 0.35), BEACON_TURNS = 3;
const SHIFT_KINDS = {
  extend: { name: '복도 연장', warn: '구역 끝까지의 복도가 12m 늘어난다' },
  marks: { name: '표지물 이동', warn: '지나온 표지물이 앞쪽으로 옮겨진다. 대열이 8m 되돌아간다' },
  power: { name: '전원 차단', warn: '구역 회선이 끊겨 비상등으로 돌아간다' },
  flip: { name: '방향 반전', warn: '복도의 앞뒤가 뒤집혀 개체와 하수인이 반대쪽에서 나타난다' }
};
function shiftTick(r) {
  const o = D(r), sc = o.scenes[r.scene];
  if (r.shift && r.turn >= r.shift.at) return shiftApply(r);
  if (!o.patrol || !sc.shiftable || r.shift || r.shiftDone || r.turn - (r.sceneT0 || 0) < 2) return;
  if (Math.random() >= SHIFT_CHANCE) return;
  const kinds = ['extend', 'marks', 'flip'].concat(r.fac && r.fac.pwr ? ['power', 'power'] : []);
  r.shift = { kind: pick(kinds), at: r.turn + 1 };
  say(r, `관제 무전: 도면 좌표가 흔들린다. 다음 턴이 끝나면 구조가 바뀐다(${SHIFT_KINDS[r.shift.kind].name}).${r.members.some(m => m.cls === 'tech' && !m.dead) ? ' 기술 요원, 비컨 띄울 수 있나?' : ' 비컨을 띄울 기술 요원이 없다. 대비해.'}`, { kind: 'radio', name: '관제 벌처' });
}
function shiftApply(r) {
  const o = D(r), sc = o.scenes[r.scene], k = r.shift.kind; let text, fx = null;
  r.shift = null; r.shiftDone = true;
  if (k === 'extend') {
    const before = Math.max(0, sc.map.len - r.pos);
    sc.map = { ...sc.map, len: sc.map.len + 12, marks: sc.map.marks.map(x => x.at > r.pos ? { ...x, at: x.at + 12 } : x) };
    if (sc.from === 'front') r.dist = Math.min(60, r.dist + 12);
    text = '벽이 숨을 쉬듯 밀려났다. 구역 끝까지의 복도가 12m 늘어났다.'; fx = { before, after: before + 12, unit: '구역 끝까지' };
  } else if (k === 'marks') {
    const np = Math.max(0, r.pos - 8), moved = np - r.pos, before = Math.max(0, sc.map.len - r.pos);
    r.pos = np; r.dist = Math.max(1, Math.min(60, r.dist + (sc.from === 'front' ? -moved : moved)));
    const odd = sc.map.marks.filter(x => !x.kind), labels = shuffle(odd.map(x => x.label));
    sc.map = { ...sc.map, marks: sc.map.marks.map(x => x.kind ? x : { ...x, label: labels.shift() }) };
    text = '방금 지나친 표지가 다시 앞에 있다. 대열은 어느새 8m 뒤로 되돌아와 있었다.'; fx = { before, after: before - moved, unit: '구역 끝까지' };
  } else if (k === 'power') {
    if (r.fac) { r.fac.pwr = false; }
    text = '배선이 벽 속으로 빨려 들어갔다. 구역이 다시 비상등으로 돌아갔다.';
  } else {
    sc.from = sc.from === 'rear' ? 'front' : 'rear';
    r.minions.forEach(x => { x.side = x.side === 'front' ? 'rear' : 'front'; });
    text = `복도의 앞과 뒤가 뒤바뀌었다. 개체의 기척이 이제 대열 ${sc.from === 'rear' ? '뒤' : '앞'}에서 들린다.`;
  }
  r.shiftFx = { kind: k, name: SHIFT_KINDS[k].name, text, turn: r.turn, ...(fx || {}) };
  say(r, text, { kind: 'shift', title: SHIFT_KINDS[k].name });
}
function sceneEnter(r) {
  r.ctlUsed = {}; r.fac = {}; r.band = 0; r.forkIdle = 0; r.shift = null; r.shiftDone = false; r.shiftFx = null; r.sceneT0 = r.turn;
  const sc0 = D(r).scenes[r.scene];
  if (sc0.fork) { r.minions = []; return; }
  const sp = sc0.spawn;
  if (sp) for (const sd of ['front', 'rear']) if (sp[sd]) spawnMinion(r, sd, sp[sd] + scaleOf(r).spawn);
}
// 하수인의 다음 행동. 칩에 미리 보여서 요원이 막을지 칠지 고를 수 있다.
// bite 물기(HP) · scream 비명(정신력) · call 동료 부르기 · brace 웅크리기(다음 피해 절반)
const INTENT = { bite: '물기', scream: '비명', call: '부르기', brace: '웅크리기', stun: '비틀거림' };
function pickIntent(r) {
  const x = Math.random(), full = r.minions.length >= MINION_CAP;
  return x < 0.5 ? 'bite' : x < 0.7 ? 'scream' : x < 0.85 && !full ? 'call' : 'brace';
}
function minionsAttack(r) {
  const o = D(r), L = lineOf(r); if (!L.length) return;
  const nm = o.minion.name;
  for (const sd of ['front', 'rear']) {
    // 좁은 복도라 한쪽에서 한 번에 하나만 움직인다
    const f = r.minions.find(x => x.side === sd); if (!f) continue;
    const t = sd === 'front' ? L[0] : L[L.length - 1];
    const p = L.length === 1 ? '' : sd === 'front' ? '선두' : '후미';
    const act = f.intent || 'bite';
    f.braced = false;
    if (act === 'stun') { say(r, `${jo(nm, '이', '가')} 비틀거리며 자세를 다시 잡는다.`, { kind: 'contact' }); }
    else if (act === 'brace') { f.braced = true; say(r, `${jo(nm, '이', '가')} 몸을 웅크렸다. 다음 공격을 절반만 받는다.`, { kind: 'contact' }); }
    else if (act === 'call') { say(r, `${jo(nm, '이', '가')} 길게 울었다. 어둠 속에서 무언가 대답한다.`, { kind: 'contact' }); spawnMinion(r, sd, 1); }
    else if (Math.random() < STEALTH_DODGE * lvOf(t, '은신')) { say(r, `${jo(nm, '이', '가')} ${p} ${t.name} 요원에게 달려들었지만, 요원은 이미 그 자리에 없었다. (은신)`.replace('  ', ' '), { kind: 'contact', uid: t.uid }); }
    else if (act === 'scream') {
      let d = grit(t, sanD(-1)); if (t.guard) d = Math.min(-1, Math.round(d / 2));
      say(r, `${jo(nm, '이', '가')} ${p} ${t.name} 요원의 귀에 대고 비명을 질렀다.${t.guard ? ' (막기로 절반)' : ''}`.replace('  ', ' '), { kind: 'contact', uid: t.uid, fx: { san: d } });
      t.san = Math.max(0, t.san + d);
    } else {
      let d = hpD(-1); if (t.guard) d = Math.min(-1, Math.round(d / 2));
      say(r, `${jo(nm, '이', '가')} ${p} ${t.name} 요원에게 달려들었다.${t.guard ? ' (막기로 절반)' : ''}`.replace('  ', ' '), { kind: 'contact', uid: t.uid, fx: { hp: d } });
      t.hp = Math.max(0, t.hp + d);
    }
    f.intent = pickIntent(r);
  }
  r.members.forEach(x => { x.guard = false; });
}
// 개체가 움직인다. 거리가 0이 되면 개체 쪽 끝에 선 요원이 접촉당한다.
function entityStep(r) {
  const o = D(r), sc = o.scenes[r.scene];
  // 소수 속도는 누적해서 반영한다(예: 5.25m면 네 턴에 한 번 6m)
  r._sp = (r._sp || 0) + sc.speed * scaleOf(r).speed; const step = Math.floor(r._sp); r._sp -= step;
  r.dist = Math.max(0, r.dist - step);
  if (r.dist > 0) return;
  const L = lineOf(r); if (!L.length) return;
  const t = sc.from === 'rear' ? L[L.length - 1] : L[0];
  const c = scaleFx(o.contact); if (c.san) c.san = grit(t, c.san);
  if (r.cover === r.turn - 1) { if (c.hp) c.hp = Math.min(-1, Math.round(c.hp / 2)); if (c.san) c.san = Math.min(-1, Math.round(c.san / 2)); c.text += ' (엄호로 절반)'; }
  say(r, c.text.replace('{name}', t.name), { kind: 'contact', ent: true, uid: t.uid, fx: { hp: c.hp, san: c.san, count: c.count } });
  applyFx(r, t, c);
  r.dist = o.recoil || RECOIL_M;
}
// 턴 끝: 개체 이동 → 셈 감소 → 신체 확인 → 실패 판정
/* ───────── 관제 간섭: 화면에 잡음이 끼고 알 수 없는 신호가 흘러든다 ─────────
   잡음이 있는 동안 관제는 정확한 거리를 읽을 수 없고, 턴마다 정신력이 조금씩 깎인다.
   해독(지능+기록 해독)하면 귀환 신호를 되찾지만 읽는 것만으로도 정신력이 깎인다. 차단하면 안전하지만 얻는 것이 없다. */
const NOISE_TEXT = [
  '셋. 넷. 다섯. 관제실에 있는 사람도 세어졌다. 여섯.',
  'B3 아래에는 층이 없다. 그런데 너희는 지금 B4에 있다.',
  '수조 3번에서 나온 것은 처음 들어간 것과 같지 않다.',
  '레이더의 점은 하나가 아니다. 하나로 겹쳐 있을 뿐이다.',
  '뒤를 돌아보지 마라. 관제실 유리에 비친 건 네가 아니다.',
  '귀환 신호는 우리가 먹고 있다. 돌려받고 싶으면 읽어라.',
  '문은 열려 있었다. 처음부터. 너희가 닫혀 있었다.'
];
const NOISE_CHANCE = 0.2;
const SUPPORT_MAX = 2; // 관제 지원 주사위는 2개까지 쌓인다(시뮬레이터로 맞춘 값)
function garble(t) { return [...t].map(ch => ch === ' ' || ch === '.' ? ch : Math.random() < 0.55 ? '▒█▓░'[Math.floor(Math.random() * 4)] : ch).join(''); }
function noiseStart(r) {
  if (r.noise) return;
  const text = NOISE_TEXT[Math.floor(Math.random() * NOISE_TEXT.length)];
  r.noise = { text, shown: garble(text), turns: 0 };
  say(r, '무전에 잡음이 끼었다. 관제 보고가 끊기고, 알 수 없는 신호가 흘러든다.', { kind: 'noise' });
}
function noiseTick(r) {
  if (!r.noise) { if (r.turn >= 2 && Math.random() < NOISE_CHANCE * (r.beaconUntil >= r.turn ? 2 : 1)) noiseStart(r); return; }
  r.noise.turns++; r.noise.shown = garble(r.noise.text);
  if (r.noise.turns >= 3) { r.noise = null; say(r, '관제 무전: 회선 복구했다. 다시 들리나?', { kind: 'radio', name: '관제 벌처' }); }
}
// 관제 NPC: 개체가 가까워질 때마다 무전으로 거리를 알린다(잡음이 끼면 끊긴다)
function npcRadio(r) {
  const sc = D(r).scenes[r.scene], d = r.dist, band = d <= 5 ? 4 : d <= 10 ? 3 : d <= 20 ? 2 : d <= RADAR_RANGE ? 1 : 0;
  if (r.noise) return;
  if (band > (r.band || 0)) say(r, `관제 무전: 개체, 대열 ${sc.from === 'rear' ? '뒤' : '앞'} ${d}m. ${band >= 4 ? '접촉 직전이다. 막을 준비해.' : band >= 3 ? '곧 붙는다.' : band >= 2 ? '빠르게 다가온다.' : '레이더에 잡혔다.'}`, { kind: 'radio', name: '관제 벌처' });
  r.band = band;
}
function endTurn(r, moved) {
  if (r.batch) { if (moved === false) r._sceneChanged = true; return null; }
  const o = D(r);
  r.turn++;
  if (moved !== false) { minionsAttack(r); entityStep(r); }
  const SC = scaleOf(r);
  const ev = Math.max(1.5, (o.countEvery || COUNT_EVERY) * SC.every); // 소수 주기: 평균적으로 ev 턴마다 한 칸
  if (Math.floor(r.turn / ev) > Math.floor((r.turn - 1) / ev)) {
    r.count = Math.max(0, r.count - 1); say(r, o.tickText, { kind: 'warn' });
    if (o.tickSpawn !== false && r.minions.length < SC.cap) spawnMinion(r, o.scenes[r.scene].from, 1 + (SC.spawn && Math.random() < 0.5 ? SC.spawn : 0));
  }
  noiseTick(r);
  if (moved !== false) shiftTick(r);
  npcRadio(r);
  r.members.forEach(x => { x.acted = false; x.radioed = false; x.passed = false; });
  checkBodies(r);
  if (r.count <= 0) {
    say(r, o.endText, { kind: 'death' });
    r.members.filter(x => x.role === 'field' && !x.dead).forEach(x => { x.hp = Math.max(0, x.hp + hpD(o.endHp)); });
    checkBodies(r);
    return opFinish(r, 'fail');
  }
  if (!r.members.some(x => x.role === 'field' && !x.dead)) return opFinish(r, 'fail');
  // 관제팀이 없는데 현장팀이 모두 붕괴하면 더 진행할 수 없다
  if (!r.members.some(x => x.role === 'field' && !x.dead && !x.broken)) { say(r, '현장팀 전원의 정신이 붕괴했다. 응답할 요원이 없다.', { kind: 'death' }); return opFinish(r, 'fail'); }
  return null;
}
const POS_NAME = { front: '선두', mid: '중열', rear: '후미' };
function actExec(r, uid, choiceId) {
  const err = canField(r, uid); if (err) return err;
  const m = fieldMember(r, uid);
  const o = D(r), sc = o.scenes[r.scene];
  if (/^atk-(front|rear)$/.test(choiceId)) return opFight(r, m, uid, choiceId.slice(4));
  if (/^shv-(front|rear)$/.test(choiceId)) return opShove(r, m, uid, choiceId.slice(4));
  if (choiceId === 'guard') return opGuard(r, m, uid);
  const CA = classAct(choiceId);
  if (CA) { const [pe] = classPre(r, m, CA); if (pe) return pe; const e = classExec(r, m, CA.k, CA.t); if (e) return e; checkBodies(r); return endTurn(r); }
  if (choiceId.startsWith('rec-')) return opRecover(r, m, uid, choiceId.slice(4));
  if (choiceId.startsWith('heal-')) return opHeal(r, m, uid, choiceId.slice(5));
  const c = sc.choices.find(x => x.id === choiceId);
  if (!c) return 'op/bad-choice';
  if (!posOk(r, uid, c.pos)) return 'op/pos';
  if (c.reach && r.pos < sc.map.len - REACH_M) return 'op/not-there';
  // 거리가 맞지 않으면 판정 없이 턴만 흐른다 (현장팀은 정확한 거리를 모른다)
  if ((c.max && r.dist > c.max) || (c.min && r.dist < c.min)) {
    say(r, `${m.name} 요원이 「${c.label}」을(를) 시도했지만 ${c.max && r.dist > c.max ? '개체가 아직 멀었다' : '개체가 너무 가까웠다'}. 아무 일도 일어나지 않았다.`, { kind: 'miss', uid: m.uid });
    return endTurn(r);
  }
  const row = Q.agent.get(uid);
  const sup = r.support; r.support = 0;
  const ease = c.adv !== false && r.fac && r.fac.ease ? 1 : 0, diff = Math.max(1, c.diff - ease);
  const res = rollPool({ ...row, contamination: (row.contamination || 0) + m.cont }, c.stat, c.skill, sup, isDark(r) ? 1 : 0);
  const tier = tierOf(res.succ, diff);
  const raw = c[tier], out = scaleFx(typeof raw === 'string' ? { text: raw } : raw); if (out.san) out.san = grit(m, out.san);
  const fx = ['hp', 'san', 'cont', 'count', 'sup', 'push'].some(k => out[k]) ? { hp: out.hp, san: out.san, cont: out.cont, count: out.count, sup: out.sup, push: out.push } : null;
  say(r, out.text, { kind: 'roll', uid: m.uid, name: m.name, pos: POS_NAME[posOf(r, uid)] || '', label: c.label, stat: res.stat, statVal: res.statVal, skill: c.skill, skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff, tier, tierName: TIER_NAME[tier], fx });
  applyFx(r, m, out);
  if (tier === 'crit' && c.adv !== false) r.count = Math.min(o.count, r.count + 1);
  for (let i = 0; i < res.anomalies; i++) { const d = grit(m, sanD(-1)); m.san = Math.max(0, m.san + d); say(r, `오염 주사위에서 1이 나왔다. ${m.name} 요원의 귀에 이상한 소리가 하나 더 들린다.`, { kind: 'warn', uid: m.uid, fx: { san: d } }); }
  if (tier !== 'fail' && c.adv !== false) {
    r.scene++;
    if (r.scene >= o.scenes.length) { checkBodies(r); return opFinish(r, 'success'); }
    r.dist = o.scenes[r.scene].dist; r.pos = o.scenes[r.scene].map.start;
    say(r, o.scenes[r.scene].text, { kind: 'scene', title: o.scenes[r.scene].title });
    sceneEnter(r);
    return endTurn(r, false);
  }
  return endTurn(r);
}
// 교전: 끝자리는 근접(근력 + 격투), 중열은 사격(민첩 + 사격). 성공 개수만큼 피해를 준다.
function opFight(r, m, uid, side) {
  const o = D(r), foe = r.minions.find(x => x.side === side);
  if (!foe) return 'op/no-foe';
  const p = posOf(r, uid), L = lineOf(r);
  const melee = p === 'solo' || (side === 'front' ? p === 'front' : p === 'rear');
  const ranged = p === 'mid' || (L.length === 2 && p !== (side === 'front' ? 'front' : 'rear'));
  if (!melee && !ranged) return 'op/pos';
  const stat = melee ? 0 : 2, skill = melee ? '격투' : '사격', diff = 2;
  const row = Q.agent.get(uid), sup = r.support; r.support = 0;
  const atk = m.cls === 'atk';
  const res = rollPool({ ...row, contamination: (row.contamination || 0) + m.cont }, stat, skill, sup + (atk ? 1 : 0));
  const tier = tierOf(res.succ, diff);
  let dmg = tier === 'crit' ? res.succ + 1 : tier === 'ok' ? res.succ : tier === 'cost' ? 1 : 0;
  if (atk && dmg > 0) dmg += 1;
  // 협공: 바로 앞 차례에 다른 요원이 친 하수인을 이어 치면 +2
  const combo = dmg > 0 && foe.hitTurn >= r.turn - 1 && foe.hitBy && foe.hitBy !== m.uid;
  if (combo) dmg += 2;
  const braced = dmg > 0 && foe.braced; if (braced) dmg = Math.ceil(dmg / 2);
  if (dmg > 0) { foe.hitTurn = r.turn; foe.hitBy = m.uid; }
  const hurt = tier === 'cost' || tier === 'fail' ? hpD(-1) : 0;
  foe.hp = Math.max(0, foe.hp - dmg);
  const nm = o.minion.name, txt = { crit: `정확히 급소를 노렸다. ${jo(nm, '이', '가')} 크게 휘청인다.`, ok: `${jo(nm, '을', '를')} 밀어붙였다.`, cost: `맞히긴 했지만 ${nm}의 반격이 스쳤다.`, fail: `빗나갔다. ${jo(nm, '이', '가')} 틈을 파고들었다.` }[tier];
  say(r, txt, { kind: 'roll', uid: m.uid, name: m.name, pos: POS_NAME[p] || '', label: (melee ? '근접 공격' : '사격') + (side === 'front' ? ' (앞)' : ' (뒤)'), stat: res.stat, statVal: res.statVal, skill, skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff, tier, tierName: TIER_NAME[tier], fx: { foe: dmg, hp: hurt || undefined } });
  if (combo) say(r, `협공. ${jo(o.minion.name, '이', '가')} 연달아 맞아 피해 +2.`, { kind: 'kill', uid: m.uid });
  if (braced) say(r, `${jo(o.minion.name, '이', '가')} 웅크리고 있어 피해가 절반으로 줄었다.`, { kind: 'warn' });
  if (hurt) m.hp = Math.max(0, m.hp + hurt);
  for (let i = 0; i < res.anomalies; i++) { const d = grit(m, sanD(-1)); m.san = Math.max(0, m.san + d); say(r, `오염 주사위에서 1이 나왔다. ${m.name} 요원의 귀에 이상한 소리가 하나 더 들린다.`, { kind: 'warn', uid: m.uid, fx: { san: d } }); }
  if (foe.hp <= 0) { r.minions = r.minions.filter(x => x !== foe); say(r, o.minion.kill.replace('{name}', m.name), { kind: 'kill', uid: m.uid }); }
  return endTurn(r);
}
// 밀쳐내기: 끝자리 요원이 근력 + 격투, 쉬움(1). 성공하면 하수인이 다음 행동을 놓친다.
function opShove(r, m, uid, side) {
  const o = D(r), foe = r.minions.find(x => x.side === side); if (!foe) return 'op/no-foe';
  const p = posOf(r, uid); if (!(p === 'solo' || p === side)) return 'op/pos';
  const row = Q.agent.get(uid), sup = r.support; r.support = 0;
  const res = rollPool({ ...row, contamination: (row.contamination || 0) + m.cont }, 0, '격투', sup);
  const tier = tierOf(res.succ, 1), nm = o.minion.name;
  if (tier !== 'fail') { foe.intent = 'stun'; foe.braced = false; if (tier === 'crit') { foe.hp = Math.max(0, foe.hp - 1); } }
  const txt = { crit: `${jo(nm, '을', '를')} 벽으로 세게 밀쳐 냈다. 다음 행동을 놓치고 피해 1.`, ok: `${jo(nm, '을', '를')} 밀쳐 냈다. 다음 행동을 놓친다.`, cost: `${jo(nm, '을', '를')} 겨우 밀쳐 냈다. 다음 행동을 놓친다.`, fail: `밀쳐 내지 못했다. ${jo(nm, '이', '가')} 버틴다.` }[tier];
  say(r, txt, { kind: 'roll', uid: m.uid, name: m.name, pos: POS_NAME[p] || '', label: '밀쳐내기' + (side === 'front' ? ' (앞)' : ' (뒤)'), stat: res.stat, statVal: res.statVal, skill: '격투', skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff: 1, tier, tierName: TIER_NAME[tier] });
  if (foe.hp <= 0) { r.minions = r.minions.filter(x => x !== foe); say(r, o.minion.kill.replace('{name}', m.name), { kind: 'kill', uid: m.uid }); }
  return endTurn(r);
}
// 막기: 판정 없이 이번 차례 하수인에게 받는 피해(물기·비명)를 절반으로 줄인다.
function opGuard(r, m, uid) {
  if (m.cls === 'def') {
    if (!r.minions.length && r.dist > 10) return 'op/no-foe';
    r.members.filter(x => x.role === 'field' && !x.dead).forEach(x => { x.guard = true; }); r.cover = r.turn;
    say(r, `${m.name} 요원이 방패를 세우고 대열을 엄호했다. 이번 라운드에 받는 피해가 절반으로 준다.`, { kind: 'sys', uid: m.uid });
    return endTurn(r);
  }
  if (!r.minions.length) return 'op/no-foe';
  m.guard = true;
  say(r, `${m.name} 요원이 자세를 낮추고 막을 준비를 했다.`, { kind: 'sys', uid: m.uid });
  return endTurn(r);
}
// 회수: 쓰러진(사망·붕괴) 동료를 데리고 간다. 체력 + 운반, 쉬움(1). 회수하지 못한 채 작전이 끝나면 RAVEN-4가 출동한다.
const needsRecovery = m => m.role === 'field' && !m.recovered && (m.dead || m.broken);
// 응급처치: 기능이 있는 현장 요원이 다친 동료(자기 자신 포함)의 체력을 회복한다. 지능 + 응급처치, 보통(2).
const canHeal = t => t.role === 'field' && !t.dead && t.hp < t.hpMax;
function opHeal(r, m, uid, target) {
  if (m.cls !== 'med') return 'op/cls';
  const t = r.members.find(x => x.uid === String(target));
  if (!t || !canHeal(t)) return 'op/no-target';
  const row = Q.agent.get(uid), sup = r.support; r.support = 0;
  const res = rollPool({ ...row, contamination: (row.contamination || 0) + m.cont }, 3, '응급처치', sup);
  const tier = tierOf(res.succ, 2), heal = { crit: hpD(3), ok: hpD(2), cost: hpD(1), fail: 0 }[tier], who = t === m ? '자기' : `${t.name} 요원의`;
  const txt = { crit: `${who} 상처를 빠르게 봉합했다. 출혈이 멎었다.`, ok: `${who} 상처를 지혈했다.`, cost: `${who} 상처를 감았지만 붕대가 금세 젖는다.`, fail: `손이 떨려 ${who} 상처를 제대로 감지 못했다.` }[tier];
  say(r, txt, { kind: 'roll', uid: m.uid, name: m.name, pos: POS_NAME[posOf(r, uid)] || '', label: t === m ? '자가 응급 처치' : `${t.name} 응급 처치`, stat: res.stat, statVal: res.statVal, skill: '응급처치', skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff: 2, tier, tierName: TIER_NAME[tier], fx: heal ? { hp: heal } : null });
  if (heal) t.hp = Math.min(t.hpMax, t.hp + heal);
  return endTurn(r);
}
function opRecover(r, m, uid, target) {
  const t = r.members.find(x => x.uid === String(target));
  if (!t || !needsRecovery(t)) return 'op/no-target';
  if (t.uid === m.uid) return 'op/bad-target';
  const row = Q.agent.get(uid), sup = r.support; r.support = 0;
  const res = rollPool({ ...row, contamination: (row.contamination || 0) + m.cont }, 1, '운반', sup);
  const tier = tierOf(res.succ, 1), hurt = tier === 'cost' ? hpD(-1) : 0;
  const txt = { crit: `${t.name} 요원을 둘러메고 대열에 합류했다. 흔들림이 없다.`, ok: `${t.name} 요원을 회수했다.`, cost: `${t.name} 요원을 회수했지만 무게에 어깨가 꺾였다.`, fail: `${t.name} 요원에게 손이 닿지 않았다.` }[tier];
  say(r, txt, { kind: 'roll', uid: m.uid, name: m.name, pos: POS_NAME[posOf(r, uid)] || '', label: `${t.name} 회수`, stat: res.stat, statVal: res.statVal, skill: '운반', skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff: 1, tier, tierName: TIER_NAME[tier], fx: tier === 'cost' ? { hp: hurt } : null });
  if (tier !== 'fail') t.recovered = true;
  if (tier === 'cost') m.hp = Math.max(0, m.hp + hurt);
  return endTurn(r);
}
// 이동: 전진은 선두, 후퇴는 후미, 대기와 자리 바꾸기는 누구나
function moveExec(r, uid, kind) {
  const err = canField(r, uid); if (err) return err;
  const m = fieldMember(r, uid), sc = D(r).scenes[r.scene], p = posOf(r, uid);
  if (kind === 'fwd' || kind === 'back') {
    if (kind === 'fwd' && !(p === 'front' || p === 'solo')) return 'op/pos';
    if (kind === 'back' && !(p === 'rear' || p === 'solo')) return 'op/pos';
    if (r.minions.some(x => x.side === (kind === 'fwd' ? 'front' : 'rear'))) return kind === 'fwd' ? 'op/blocked-front' : 'op/blocked-rear';
    // 대열은 지형 위에서 움직인다. 진행 방향(fwd)은 지형의 끝 쪽.
    const np = Math.max(0, Math.min(sc.map.len, r.pos + (kind === 'fwd' ? STEP_M : -STEP_M))), moved = np - r.pos;
    if (!moved) return kind === 'fwd' ? 'op/map-end' : 'op/map-start';
    r.pos = np;
    r.dist = Math.max(0, Math.min(60, r.dist + (sc.from === 'front' ? -moved : moved)));
    say(r, kind === 'fwd' ? `선두 ${m.name} 요원이 대열을 이끌고 앞으로 나아갔다.` : `후미 ${m.name} 요원의 신호에 대열이 뒤로 물러났다.`, { kind: 'move', uid: m.uid });
  } else if (kind === 'wait') {
    say(r, `${m.name} 요원의 신호로 대열이 제자리에서 숨을 죽였다.`, { kind: 'move', uid: m.uid });
  } else if (kind === 'swap') {
    const L = lineOf(r), i = L.findIndex(x => x.uid === m.uid);
    if (i <= 0) return 'op/no-ahead';
    const a = L[i - 1], ia = r.order.indexOf(a.uid), ib = r.order.indexOf(m.uid);
    r.order[ia] = m.uid; r.order[ib] = a.uid;
    say(r, `${m.name} 요원이 ${a.name} 요원과 자리를 바꿨다. 대열: ${lineOf(r).map(x => x.name).join(' → ')}`, { kind: 'move', uid: m.uid });
  } else return 'op/bad-choice';
  return endTurn(r);
}
// 작전 중 소모품: 현장 요원만, 차례마다 한 번. 차례를 쓰지 않는다.
function opItem(uid, opId, id) {
  const r = runs.get(opId); if (!r || r.status !== 'live') return 'op/not-live';
  const m = r.members.find(x => x.uid === String(uid) && x.role === 'field'); if (!m) return 'op/not-field';
  if (m.dead) return 'op/dead'; if (m.broken) return 'op/broken';
  if (m.itemTurn === r.turn) return 'op/item-wait';
  const row = Q.agent.get(uid), v = invOf(row), it = SHOP_BY[id];
  if (!it || it.kind !== 'use' || !(v.items[id] > 0)) return 'shop/none';
  const team = r.members.filter(x => x.role === 'field' && !x.dead);
  if (id === 'kit') { const t = team.filter(x => x.hp < x.hpMax).sort((a, b) => a.hp / a.hpMax - b.hp / b.hpMax)[0]; if (!t) return 'op/no-need';
    const h = Math.min(30, t.hpMax - t.hp); t.hp += h; say(r, `${m.name} 요원이 구급 키트를 썼다. ${t.name} 요원 HP +${h}.`, { kind: 'sys' }); }
  else if (id === 'calm') { const t = team.filter(x => x.san < x.sanMax).sort((a, b) => a.san / a.sanMax - b.san / b.sanMax)[0]; if (!t) return 'op/no-need';
    const h = Math.min(30, t.sanMax - t.san); t.san += h; if (t.broken && t.san > 0) t.broken = false; say(r, `${m.name} 요원이 진정제를 썼다. ${t.name} 요원 정신력 +${h}.`, { kind: 'sys' }); }
  else if (id === 'flare') { if (!r.minions.length) return 'op/no-need'; const n = r.minions.length; r.minions = []; say(r, `${m.name} 요원이 섬광탄을 던졌다. 하수인 ${n}마리가 빛을 피해 흩어졌다.`, { kind: 'sys' }); }
  v.items[id]--; saveInv(uid, v); m.itemTurn = r.turn; m.items = v.items;
  pushAgent(uid);
  return null;
}
// 포지션 행동: 기술(관제 요청) · 치유(정신 안정) · 누구나(RAVEN-1). 관제실은 NPC 벌처가 맡고, 현장 요원의 요청을 받아 움직인다.
function classExec(r, m, kind, target) {
  const o2 = D(r), row = Q.agent.get(Number(m.uid));
  if (kind === 'support') {
    const res = rollPool(row, 3, '통신', 0), tier = tierOf(res.succ, 1), add = tier === 'crit' ? 2 : tier === 'fail' ? 0 : 1;
    r.support = Math.min(SUPPORT_MAX, r.support + add); 
    say(r, add ? `관제 회선 연결. 다음 판정에 지원 주사위 ${add}개를 보탠다.` : '지원 신호가 잡음에 묻혔다.', { kind: 'roll', uid: m.uid, name: m.name, label: '관제 지원 연결', stat: res.stat, statVal: res.statVal, skill: '통신', skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff: 1, tier, tierName: TIER_NAME[tier] });
  } else if (kind === 'stabilize') {
    const t = r.members.find(x => x.uid === String(target) && !x.dead && (x.role === 'field'));
    if (!t) return 'op/bad-target';
    if (t.san >= t.sanMax) return 'op/no-need';
    const res = rollPool(row, 5, '응급처치', 0), tier = tierOf(res.succ, 2), heal = tier === 'crit' ? sanD(3) : tier === 'fail' ? 0 : sanD(2);
    t.san = Math.min(t.sanMax, t.san + heal); if (t.broken && t.san > 0) t.broken = false; 
    say(r, heal ? `${t.name} 요원의 호흡이 가라앉았다. (정신력 +${heal})` : `${t.name} 요원은 아직 눈의 초점이 돌아오지 않는다.`, { kind: 'roll', uid: m.uid, name: m.name, label: `${t.name} 정신 안정`, stat: res.stat, statVal: res.statVal, skill: '응급처치', skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff: 2, tier, tierName: TIER_NAME[tier] });
  } else if (kind === 'raven1') {
    // RAVEN-1 지원: 작전당 한 번. 하수인 소탕, 개체 밀어냄, 셈·귀환 신호 회복. 대가는 작전이 끝날 때 치른다.
    if (r.r1) return 'op/raven1-used';
    const o = D(r);
    r.r1 = { by: m.name, at: now() };
    const n = r.minions.length; r.minions = [];
    r.dist = Math.min(60, r.dist + 25);
    r.count = Math.min(o.count, r.count + 2);
    
    say(r, `${m.name} 요원이 RAVEN-1 지원을 요청했다. RAVEN-1이 투입됐다.${n ? ` ${jo(o.minion.name, '이', '가')} 모두 제거됐다.` : ''} 개체는 크게 물러났다. ${o.countLabel} +2.`, { kind: 'raven', uid: m.uid });
    // 고위험 개체라면 RAVEN-1이 관찰 기록을 남긴다
    const e = ENTITIES.find(x => x.op === r.op);
    if (e && e.observe && !QS.unlocks.all().some(u => u.entity === e.id && u.section === 'observe')) {
      QS.unlock.run(e.id, 'observe', 'RAVEN-1', now());
      say(r, `RAVEN-1이 ${e.id}의 반응을 기록해 갔다. 개체 문서의 관찰 기록이 열렸다.`, { kind: 'sys' });
      emitSector();
    }
    return null;
  } else if (kind === 'decode' || kind === 'jam') {
    if (!r.noise) return 'op/no-noise';
    
    if (kind === 'jam') { r.noise = null; say(r, `${m.name} 요원이 잡음 회선을 차단했다. 관제 무전이 돌아왔다.`, { kind: 'sys', uid: m.uid }); return null; }
    const read = grit(m, sanD(-0.5)); m.san = Math.max(0, m.san + read);
    const res = rollPool(row, 3, '기록 해독', 0), tier = tierOf(res.succ, 2);
    let txt, extra = 0;
    if (tier === 'fail') { extra = grit(m, sanD(-0.6)); txt = '읽어 내지 못했다. 글자들이 눈 안쪽에서 계속 움직인다.'; }
    else {
      txt = `신호를 해독했다: 「${r.noise.text}」 잡음이 걷히며 귀환 신호가 돌아온다. ${o2.countLabel} +1.`;
      r.count = Math.min(o2.count, r.count + 1);
      if (tier === 'crit') { r.support = Math.min(SUPPORT_MAX, r.support + 1); txt += ' 해독한 주파수로 지원 주사위 1개를 보탠다.'; }
      if (tier === 'cost') extra = grit(m, sanD(-0.4));
      r.noise = null;
    }
    m.san = Math.max(0, m.san + extra);
    say(r, txt, { kind: 'roll', uid: m.uid, name: m.name, label: '신호 해독', stat: res.stat, statVal: res.statVal, skill: '기록 해독', skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff: 2, tier, tierName: TIER_NAME[tier], fx: { san: read + extra } });
    checkBodies(r);
  } else if (kind === 'peek') {
    const sc = o2.scenes[r.scene], d = sc.fork && sc.doors.find(x => x.id === target);
    if (!d) return 'op/bad-choice';
    if (d.peeked) return 'op/used';
    if (r.noise) return 'op/noise';
    d.peeked = true; 
    const R = DOOR_RISK[d.risk], ns = d.scene;
    say(r, `${m.name} 요원의 요청으로 관제가 ${d.name} 너머의 감시 카메라를 확인했다. ${d.tpl}. ${R.name}: ${R.text} 개체는 대열 ${ns.from === 'rear' ? '뒤' : '앞'}쪽에서 접근한다.`, { kind: 'radio', uid: m.uid, name: '관제 벌처', peek: d.id });
    return null;
  } else if (kind === 'fac') {
    const F = facsOf(r).find(x => x.id === target); if (!F) return 'op/bad-choice';
    if (F.id === 'beacon') {
      r.beaconUntil = r.turn + BEACON_TURNS;
      const res = rollPool(row, 3, '통신', 0), tier = tierOf(res.succ, F.diff);
      let txt;
      if (tier === 'fail') txt = '비컨 신호가 하층의 잡음에 묻혔다. 좌표를 붙들지 못했다. 심연은 신호를 들었다.';
      else { const k = SHIFT_KINDS[r.shift.kind].name; r.shift = null; r.shiftDone = true; txt = `기준점 비컨이 원래 도면 좌표를 송출했다. 구조가 제자리에 붙들렸다(${k} 차단). 심연도 그 신호를 들었다.`; }
      say(r, txt, { kind: 'roll', uid: m.uid, name: m.name, label: '기준점 비컨', stat: res.stat, statVal: res.statVal, skill: '통신', skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff: F.diff, tier, tierName: TIER_NAME[tier] });
      return null;
    }
    if (F.on) return 'op/used';
    if (F.id === 'wall' && !F.need) return 'op/no-target';
    
    const res = rollPool(row, 3, '장비 운용', 0), tier = tierOf(res.succ, F.diff);
    let txt;
    if (F.id === 'pwr') {
      if (tier === 'fail') { txt = '구역 전원을 올리다 회로가 튀었다. 비상등만 깜빡이고, 관제 화면에 잡음이 번진다.'; noiseStart(r); }
      else { r.fac.pwr = true; r.minions.forEach(x => { x.intent = 'stun'; x.braced = false; }); txt = `구역 전원을 올렸다. 조명이 돌아왔다.${r.minions.length ? ` ${jo(o2.minion.name, '이', '가')} 빛에 눈이 멀어 비틀거린다.` : ''}`; }
    } else if (F.id === 'sp') {
      if (tier === 'fail') txt = `${F.name}이(가) 반응하지 않는다.`;
      else { r.fac.sp = true; if (F.type === 'short') { const sc = o2.scenes[r.scene]; r.pos = Math.min(sc.map.len, r.pos + 10); txt = `${F.name}을(를) 원격으로 열었다. 대열이 지름길로 10m 앞당겨졌다.`; } else { r.fac.ease = true; txt = `${F.name}을(를) 원격으로 가동했다. ${F.eff}.`; } }
    } else {
      r.doorUsed = true;
      if (tier === 'fail') txt = '격벽이 반쯤 내려오다 멈췄다. 개체는 틈으로 그대로 다가온다.';
      else { const push = tier === 'crit' ? 12 : 8; r.dist = Math.min(60, r.dist + push); txt = `원격으로 격벽을 내렸다. 개체가 돌아가는 길을 찾느라 ${push}m 멀어졌다.`; }
    }
    say(r, txt, { kind: 'roll', uid: m.uid, name: m.name, label: F.name + ' 조작', stat: res.stat, statVal: res.statVal, skill: '장비 운용', skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff: F.diff, tier, tierName: TIER_NAME[tier] });
  } else return 'op/bad-choice';
  return null;
}
function opAbort(uid, opId) {
  const r = runs.get(opId); if (!r || r.status !== 'live') return 'op/not-live';
  if (!r.members.some(m => m.uid === String(uid))) return 'op/not-member';
  const m = r.members.find(x => x.uid === String(uid));
  say(r, `${m.name} 요원이 철수를 요청했다. 작전을 중단한다.`, { kind: 'sys' });
  return opFinish(r, 'abort');
}
function addTrait(traits) {
  const id = die(25), has = traits.find(t => t.id === id);
  if (has) { has.lv = Math.min(TRAIT_MAX_LV, (has.lv || 1) + 1); return { id, deepen: true }; }
  if (traits.length < MAX_TRAITS) { traits.push({ id, lv: 1 }); return { id, deepen: false }; }
  const pick = traits[die(traits.length) - 1]; pick.lv = Math.min(TRAIT_MAX_LV, (pick.lv || 1) + 1); return { id: pick.id, deepen: true };
}
const R1_COST = 20, R4_COST = 15, R4_CONT = 10, R4_DELAY_MS = Number(process.env.RAVEN4_DELAY_HOURS || 2) * 3600000;
function opFinish(r, result) {
  const o = D(r); clearTimeout(r._timer); r.picks = {}; r.deadline = null;
  r.status = 'done'; r.result = result; r.ended = now();
  const reward = result === 'success' ? (r.r1 ? { supply: Math.floor(o.reward.supply / 2), research: Math.floor(o.reward.research / 2) } : o.reward) : { supply: 0, research: 0 };
  // 회수하지 못한 요원 → RAVEN-4 출동. 남겨진 요원은 재생이 늦어지고 오염이 깊어지며, 참가자 전원이 회수 비용을 낸다.
  const left = r.members.filter(m => m.role === 'field' && (m.dead || m.broken) && !m.recovered);
  transaction(() => {
    r.members.forEach(m => {
      const row = Q.agent.get(Number(m.uid)); if (!row) return;
      let cont = (row.contamination || 0) + m.cont;
      const traits = traitsOf(row), res = JSON.parse(row.res);
      let regen = row.regen_until || null;
      const abandoned = left.includes(m);
      if (m.dead) {
        cont = Math.min(100, cont + 10 + (abandoned ? R4_CONT : 0)); regen = now() + REGEN_MS + (abandoned ? R4_DELAY_MS : 0);
        const t = addTrait(traits); m.newTrait = t;
        say(r, `${m.name} 요원 재생 절차 예약. 오염도 +${10 + (abandoned ? R4_CONT : 0)}%, 변질 ${t.deepen ? '심화' : '발현'}${abandoned ? `, RAVEN-4 회수 지연으로 재생 ${R4_DELAY_MS / 3600000}시간 추가` : ''}.`, { kind: 'sys', uid: m.uid });
      } else {
        if (abandoned) { cont = Math.min(100, cont + R4_CONT); say(r, `${m.name} 요원은 붕괴한 채 RAVEN-4에 회수됐다. 오염도 +${R4_CONT}%.`, { kind: 'sys', uid: m.uid }); }
        if (result === 'success') { res.supply += reward.supply; res.research += reward.research; }
      }
      if (left.length) res.supply = Math.max(0, res.supply - R4_COST);
      if (r.r1) res.supply = Math.max(0, res.supply - R1_COST);
      // 오염 한계 초과 → 폭주 징후. RAVEN-3이 회수해 격리 관찰, 변질이 진행되고 오염도는 한계의 절반으로 정화된다.
      let quar = row.quar_until || null;
      const lim = contLimit(row);
      if (cont >= lim) {
        const t = addTrait(traits); cont = Math.round(lim / 2 / 5) * 5; quar = Math.max(now(), regen || 0) + QUAR_MS;
        say(r, `${m.name} 요원의 오염도가 한계(${lim})에 이르렀다. 폭주 징후. RAVEN-3이 회수해 격리 관찰한다. 변질 ${t.deepen ? '심화' : '발현'}, 오염도 ${cont}로 정화.`, { kind: 'death', uid: m.uid });
        QS.addEvent.run(now(), 'overflow', null, `${m.num} ${m.name} 요원 오염 한계 초과. RAVEN-3 회수, ${Math.round(QUAR_MS / 3600000)}시간 격리 관찰.`, '벌처: ' + VULTURE_OVER[die(VULTURE_OVER.length) - 1]);
      }
      Q.afterOp.run({ uid: Number(m.uid), cont, traits: JSON.stringify(traits), trait: traits[0] ? traits[0].id : row.trait, res: JSON.stringify(res), regen, quar, ops: (row.ops_done || 0) + 1 });
    });
    if (r.r1) QS.addEvent.run(now(), 'assault', ENTITIES.find(x => x.op === r.op)?.id || null, `작전 ${o.id}에 RAVEN-1 지원 투입(요청: ${r.r1.by}). 작전 참가자 전원 보급품 −${R1_COST}${result === 'success' ? ', 성공 보상 절반' : ''}.`, null);
    if (left.length) QS.addEvent.run(now(), 'retrieval', ENTITIES.find(x => x.op === r.op)?.id || null, `작전 ${o.id}에서 회수되지 못한 요원 ${left.map(m => m.name).join(', ')}. RAVEN-4 출동, 회수 완료. 작전 참가자 전원 보급품 −${R4_COST}.`, null);
    Q.addRun.run({ op: r.op, title: o.title, result, started: r.started || now(), ended: r.ended, members: JSON.stringify(r.members.map(m => ({ uid: m.uid, name: m.name, num: m.num, role: m.role, dead: m.dead }))), log: JSON.stringify(r.log) });
  });
  if (r.r1) say(r, `RAVEN-1 지원 비용 정산. 작전 참가자 전원 보급품 −${R1_COST}${result === 'success' ? ', 성공 보상 절반' : ''}.`, { kind: 'warn' });
  if (left.length) say(r, `RAVEN-4 출동. 회수되지 못한 요원 ${left.map(m => m.name).join(', ')}을(를) 회수했다. 작전 참가자 전원 보급품 −${R4_COST}.`, { kind: 'warn' });
  say(r, result === 'success' ? `작전 ${o.id} 완료. 생존 요원 보상: 보급품 ${reward.supply}, 연구 자료 ${reward.research}.${D(r).patrol ? ` 수용 개체 격리 안정도 +${PATROL_STAB}%.` : ''}` : result === 'abort' ? `작전 ${o.id} 중단. 보상은 지급되지 않는다.` : `작전 ${o.id} 실패. ${o.failText}`, { kind: 'end' });
  sectorOnOp(r.op, result, r.members.map(m => m.name).join(', '));
  for (const m of r.members) { const g = gainCareer(Number(m.uid), result === 'success' ? CAREER.opSuccess : CAREER.opOther); if (g && g.ups) say(r, `${m.name} 요원 계급 ${g.rank} 승급. 능력치 포인트 +${g.ups * RANK_PTS}.`, { kind: 'grow', uid: m.uid }); }
  r.members.forEach(m => bumpAch(m.uid, { ops: 1, ctrlOps: m.cls === 'tech' ? 1 : 0, success: result === 'success' ? 1 : 0, deaths: m.dead ? 1 : 0, clutch: result === 'success' && m.role === 'field' && !m.dead && m.hp <= m.hpMax * 0.2 ? 1 : 0 }));
  r.members.forEach(m => { const row = Q.agent.get(Number(m.uid)); if (row) io.emit('agent', publicAgent(row)); });
  emitSector();
  return null;
}
function opClose(uid, opId) {
  const r = runs.get(opId); if (!r || r.status !== 'done') return 'op/not-done';
  runs.delete(opId); lobbyOf(opId); return null;
}


/* ───────── 기지와 개체 문서 (섹터 전체가 함께 쓰는 상태) ───────── */
// 격리 안정도는 시간마다 떨어진다. 요원이 보급품으로 점검하면 오르고, 작전에 성공하면 100%가 된다.
const DECAY_PER_H = Number(process.env.DECAY_PER_HOUR || 2);
const COST = { maintain: { supply: 10 }, purge: { research: 2 }, hasten: { supply: 30 }, observe: { research: 3 }, sealed: { pass: 1 } };
const ENTITIES = [
  { id: 'AO-212', name: '계수자', icon: 'hand', threat: 'Ⅲ', cell: '격리동 B-3', op: 'R8-14',
    summary: '인간형 개체. 손가락을 하나씩 접으며 수를 센다. 셈이 끝나면 셈의 대상이 된 공간에서 모든 소리가 사라진다.',
    procedure: ['격리실 네 모서리에 소리 차단 장치를 1기씩 설치한다. 하나라도 꺼지면 즉시 재격리 작전을 편성한다.', '출입 인원은 격리실 안팎에서 숫자를 소리 내어 읽지 않는다.', '복도 벽의 숫자는 매 근무 교대마다 기록한다. 숫자가 줄어들면 관제실에 보고한다.'],
    observe: ['셈의 시작 값은 격리실 반경 30m 안에 있는 인원 수와 일치한다.', '개체는 가장 최근에 들은 목소리를 흉내 내어 수를 센다. 흉내 낸 목소리의 주인은 그날 밤 숫자가 나오는 꿈을 꾼다.', '요원이 개체보다 먼저 수를 세면 개체가 그 셈을 따라 한다. 이때 셈은 거꾸로 흐를 수 있다.'],
    sealed: 'AO-212의 첫 발견 보고서에는 당시 격리동 B-3 근무자가 4명이었다고 적혀 있다. 같은 날의 출입 기록에는 3명만 남아 있다. 나머지 한 명의 넘버는 현재 재생 수조 명부에 올라 있다.' },
  { id: 'AO-118', name: '습도', icon: 'droplets', threat: 'Ⅳ', cell: '격리동 D-1', op: null,
    summary: '형체가 없는 개체로 추정된다. 개체가 머무는 구역은 습도가 100%로 고정되고, 종이 기록이 스스로 젖어 번진다.' },
  { id: 'AO-047', name: '젖은 손님', icon: 'door-open', threat: 'Ⅱ', cell: '격리동 A-2', op: null,
    summary: '비 오는 밤에만 섹터 정문 앞에 나타나는 인간형 개체. 들여보내 달라고 정중히 부탁하며, 이름을 물으면 마지막으로 문을 열어 준 직원의 이름을 댄다.' },
  { id: 'AO-019', name: '잠든 동료', icon: 'bed', threat: 'Ⅰ', cell: '격리동 A-1', op: null,
    summary: '섹터 직원 제복을 입고 간이침대에서 잠든 인간형 개체. 깨어난 기록이 없다. 곁에서 1시간 넘게 지켜본 사람은 그날 밤 개체의 꿈을 함께 꾸며, 꿈속 복도는 지금의 섹터 R-8과 구조가 같다.' },
  { id: 'AO-033', name: '빈 액자', icon: 'frame', threat: 'Ⅱ', cell: '격리동 A-3', op: null,
    summary: '금박을 두른 액자. 들여다보면 지금 서 있는 방이 비치지만, 그 방에는 보는 사람이 없다. 오래 들여다본 직원은 이후 거울에 자기 모습이 반 박자 늦게 비친다고 보고한다.' },
  { id: 'AO-089', name: '세 번째 계단', icon: 'footprints', threat: 'Ⅱ', cell: '격리동 C-2', op: null,
    summary: '섹터 동쪽 비상계단. 자정부터 새벽 4시 사이에는 2층과 3층 사이 계단이 한 칸 늘어난다. 늘어난 칸을 밟은 사람은 계단을 끝까지 올라도 3층에 도착하지 않는다.' },
  { id: 'AO-156', name: '교환원', icon: 'phone-call', threat: 'Ⅲ', cell: '격리동 C-4', op: null,
    summary: '수동식 전화 교환대. 연결된 회선이 없는데도 섹터 내선으로 전화를 건다. 수화기 너머의 목소리는 이미 사망한 직원의 것이며, 받는 사람의 넘버를 정확히 알고 있다.' },
  { id: 'AO-201', name: '등불지기', icon: 'lamp', threat: 'Ⅲ', cell: '격리동 D-3', op: null,
    summary: '꺼지지 않는 석유등. 등불이 비추는 범위 안의 복도는 도면에 없는 방향으로 이어진다. 등을 들고 나간 직원이 돌아오지 못한 기록이 2건 있다.' },
  { id: 'AO-264', name: '낱장', icon: 'scroll-text', threat: 'Ⅳ', cell: '격리동 B-1', op: null,
    summary: '누렇게 바랜 종이 한 장. 같은 방에 보관된 문서에 하룻밤마다 한 줄씩 문장을 덧붙인다. 덧붙은 문장은 언제나 아직 일어나지 않은 일을 과거형으로 적고 있다.' },
  { id: 'AO-307', name: '깊은 우물', icon: 'waves-horizontal', threat: 'Ⅴ', cell: '격리동 E-1, 지하 7층', op: null,
    summary: '섹터 지하 7층 바닥에 뚫린 원형 수직굴. 바닥은 확인되지 않았다. 굴에 대고 말하면 메아리가 돌아오는데, 돌아오는 말은 말한 사람이 아직 하지 않은 말이다.' },
  { id: 'AO-330', name: '이름 없는 별', icon: 'sparkle', threat: 'Ⅳ', cell: '관측동 옥상', op: null,
    summary: '관측동 옥상에서만 보이는 별. 어느 성도에도 실려 있지 않다. 관측 기록상 이 별은 해마다 조금씩 가까워지고 있으며, 관측자들은 하나같이 별이 이쪽을 내려다본다고 적는다.' }
];
const NOTES = {"AO-019": ["개체의 호흡 주기는 섹터 경보 시험 주기와 일치한다.","개체의 명찰은 볼 때마다 다른 이름을 표시한다. 기록된 이름은 모두 재생 수조 명부에 있다.","개체가 뒤척인 날에는 재생 수조 하나가 예정보다 일찍 열린다."],"AO-033": ["액자 속 방은 관찰자가 마지막으로 잠든 방이다.","두 사람이 함께 들여다보면 액자 속 방에는 한 사람만 비친다. 비치지 않은 쪽은 다음 날 결근한다.","액자 뒷면의 제작 일자는 섹터 설립보다 앞선다."],"AO-047": ["개체가 나타난 밤의 강수량은 언제나 0이다.","개체가 대는 이름은 문을 열어 준 직원이 아니라, 다음에 문을 열 직원의 것이다.","정문 감시 기록 속 개체는 우산을 들고 있지만, 우산은 한 번도 젖어 있지 않다."],"AO-089": ["늘어난 칸의 높이는 정확히 17cm다. 다른 칸과 1mm도 다르지 않다.","늘어난 칸을 밟고 돌아온 직원은 없다. 다만 3층 출입 기록에는 그들의 출입이 계속 남는다.","계단 손잡이에 새 손자국이 매일 하나씩 늘어난다."],"AO-118": ["젖어 번진 문서는 마르고 나면 다른 내용이 되어 있다.","구역 안에서 직원의 숨이 하얗게 보인다. 실내 온도는 22도다.","구역 배수구에서 바닷물 성분이 검출됐다. 섹터는 내륙에 있다."],"AO-156": ["통화는 언제나 3분 12초에 끊긴다.","수화기 너머의 직원에게 물으면, 그는 자기가 언제 죽었는지 모른다고 답한다.","교환대 플러그 하나가 아직 개설되지 않은 내선 번호를 가리키고 있다."],"AO-201": ["석유는 줄지 않는다. 등의 무게는 매일 조금씩 늘어난다.","등불 아래에서 쓴 기록은 불빛이 닿지 않는 곳에서 읽을 수 없다.","돌아오지 못한 두 직원의 그림자가 가끔 등 안쪽 유리에 비친다."],"AO-264": ["덧붙은 문장의 필체는 섹터 직원 한 명의 것이다. 그 직원은 아직 입사하지 않았다.","종이를 다른 방으로 옮기면 그 방의 문서가 덧붙임을 대신 받는다.","어제 덧붙은 문장: \"관찰자는 이 문장을 두 번 읽었다.\""],"AO-307": ["수직굴로 내린 추는 줄을 다 풀어도 바닥에 닿지 않는다. 감아올린 줄은 젖어 있다.","메아리로 돌아온 말은 일주일 안에 말한 사람이 실제로 하게 된다.","굴 가장자리에 까마귀 깃털이 쌓여 있다. 섹터 안에서 까마귀가 목격된 적은 없다."],"AO-330": ["별의 밝기는 섹터에서 사망자가 나온 날 변한다.","관측 기록을 겹쳐 보면 별은 섹터 위에서만 움직인다.","오늘 밤 별까지의 추정 거리는 어제보다 1광년 짧다."]};
for (const e of ENTITIES) if (!e.observe && NOTES[e.id]) e.observe = NOTES[e.id];
ENTITIES.sort((a, b) => (b.op ? 1 : 0) - (a.op ? 1 : 0) || a.id.localeCompare(b.id));
const SECTIONS = { info: '기본 정보', procedure: '격리 절차', observe: '관찰 기록', incidents: '사고 기록', sealed: '봉인 항목' };
db.prepare('SELECT 1').get();
for (const e of ENTITIES) if (!db.prepare('SELECT 1 FROM sector_entities WHERE id = ?').get(e.id)) db.prepare('INSERT INTO sector_entities (id, stability, updated, captured) VALUES (?, ?, ?, 0)').run(e.id, 0, now());
// 확보 작전이 없는 개체는 이미 격리동에 수용된 상태로 본다 (기존 데이터도 한 번 맞춘다)
for (const e of ENTITIES) if (!e.op) db.prepare('UPDATE sector_entities SET captured = 1, stability = 80, updated = ? WHERE id = ? AND captured = 0').run(now(), e.id);
const QS = {
  ent: db.prepare('SELECT * FROM sector_entities WHERE id = ?'),
  setEnt: db.prepare('UPDATE sector_entities SET stability = ?, updated = ?, captured = ? WHERE id = ?'),
  unlocks: db.prepare('SELECT * FROM sector_unlocks'),
  unlock: db.prepare('INSERT OR IGNORE INTO sector_unlocks (entity, section, by_name, at) VALUES (?, ?, ?, ?)'),
  incidents: db.prepare('SELECT id, result, started, ended, members FROM op_runs WHERE op = ? ORDER BY id DESC LIMIT 20'),
  setRes: db.prepare('UPDATE agents SET res = ? WHERE user_id = ?'),
  setCont: db.prepare('UPDATE agents SET contamination = ? WHERE user_id = ?'),
  setRegen: db.prepare('UPDATE agents SET regen_until = ? WHERE user_id = ?'),
  setLock: db.prepare('UPDATE sector_entities SET stability = ?, updated = ?, lock_until = ? WHERE id = ?'),
  events: db.prepare('SELECT * FROM sector_events ORDER BY id DESC LIMIT 20'),
  addEvent: db.prepare('INSERT INTO sector_events (at, kind, entity, text, voice) VALUES (?, ?, ?, ?, ?)'),
  allRes: db.prepare('SELECT user_id, res FROM agents')
};
// 격리 실패: 안정도가 0%가 되면 RAVEN-3(팀장 벌처)이 투입된다.
// 개체는 재격리되지만(안정도 60%), 섹터 전 요원의 보급품이 줄고 그 개체의 작전이 몇 시간 봉쇄된다.
const RAVEN_LOCK_MS = Number(process.env.RAVEN_LOCK_HOURS || 3) * 3600000, RAVEN_COST = 20, RAVEN_RESTORE = 60;
// 벌처(RAVEN-3 팀장): 냉정함, 계산적, 결과주의. 필요하면 비인도적 수단도 감수한다.
const VULTURE = ['"RAVEN-3이 인수한다. 과정은 기록하지 않는다. 결과만 남긴다."', '"손실은 계산이 끝났다. 보급품은 비용이다. 다음엔 덜 나가게 해라."', '"격리실 근무자 하나를 회수 대상에 넣었다. 이유는 알 필요 없다."', '"재격리 완료. 다섯 번째는 뒤로 빠져 있어. 죽어도 돌아오는 놈들한테 맡길 일이 아니었다."']
const entLocked = e => { const row = e && QS.ent.get(e.id); return row && row.lock_until > now() ? row.lock_until : 0; };
function checkBreach() {
  let hit = false;
  for (const e of ENTITIES) {
    const row = QS.ent.get(e.id);
    if (!row || !row.captured || row.lock_until > now() || stabNow(row) > 0) continue;
    const r = e.op ? runs.get(e.op) : null; if (r && r.status === 'live') continue; // 작전 중이면 현장팀에 맡긴다
    const hours = Math.round(RAVEN_LOCK_MS / 360000) / 10;
    transaction(() => {
      QS.setLock.run(RAVEN_RESTORE, now(), now() + RAVEN_LOCK_MS, e.id);
      for (const a of QS.allRes.all()) { const res = JSON.parse(a.res); res.supply = Math.max(0, (res.supply || 0) - RAVEN_COST); QS.setRes.run(JSON.stringify(res), a.user_id); }
      QS.addEvent.run(now(), 'breach', e.id, `${e.id} 「${e.name}」 격리 실패. RAVEN-3 투입, 팀장 벌처 지휘. 재격리 완료, 안정도 ${RAVEN_RESTORE}%. 섹터 전 요원 보급품 −${RAVEN_COST}. ${e.op ? `작전 ${e.op}` : '격리실 관리 작업'} ${hours}시간 봉쇄.`, '벌처: ' + VULTURE[die(VULTURE.length) - 1]);
    });
    if (r) emitRun(r);
    hit = true;
  }
  if (hit) { io.emit('agents', Q.agents.all().map(publicAgent)); emitSector(); }
}
/* 격리 판정: 관리 작업과 긴급 점검은 주사위를 굴린다. 실패하면 개체가 탈출할 수 있다. */
const THREAT_DIFF = { 'Ⅰ': 1, 'Ⅱ': 1, 'Ⅲ': 2, 'Ⅳ': 2, 'Ⅴ': 3 };
const THREAT_ESC = { 'Ⅰ': 0.05, 'Ⅱ': 0.1, 'Ⅲ': 0.15, 'Ⅳ': 0.2, 'Ⅴ': 0.25 };
const CHECK = { maintain: { stat: 3, skill: '장비 운용' }, observe: { stat: 4, skill: '관찰' }, emergency: { stat: 3, skill: '장비 운용' } };
const ESC_LOW = 30, ESC_LOW_ADD = 0.25, ESC_ANOM = 0.1, ESC_CONT = 15;
const diffOf = e => THREAT_DIFF[e.threat] || 2;
// 격리 판정은 성공이 하나도 없으면 난이도와 상관없이 실패다
const containTier = (succ, diff) => succ >= diff + 2 ? 'crit' : succ >= diff ? 'ok' : succ >= Math.max(1, diff - 1) ? 'cost' : 'fail';
function containRoll(row, e, kind, support) {
  const c = CHECK[kind], res = rollPool(row, c.stat, c.skill, support || 0), diff = diffOf(e), tier = containTier(res.succ, diff);
  return { stat: res.stat, statVal: res.statVal, skill: c.skill, skillLv: res.skillLv, dice: res.dice, succ: res.succ, diff, tier, tierName: TIER_NAME[tier], anomalies: res.anomalies };
}
// 탈출 확률: 위협 등급 기본값 + 안정도가 낮으면 가산 + 검은 주사위의 1(이상 징후)마다 가산
const escChance = (e, st, anomalies) => Math.min(0.8, (THREAT_ESC[e.threat] || 0.15) + (st < ESC_LOW ? ESC_LOW_ADD : 0) + ESC_ANOM * (anomalies || 0));
function escapeIn(e, uid, what) {
  const row = Q.agent.get(uid), hours = Math.round(RAVEN_LOCK_MS / 360000) / 10;
  transaction(() => {
    QS.setLock.run(RAVEN_RESTORE, now(), now() + RAVEN_LOCK_MS, e.id);
    for (const a of QS.allRes.all()) { const res = JSON.parse(a.res); res.supply = Math.max(0, (res.supply || 0) - RAVEN_COST); QS.setRes.run(JSON.stringify(res), a.user_id); }
    if (row) QS.setCont.run((row.contamination || 0) + ESC_CONT, uid);
    QS.addEvent.run(now(), 'escape', e.id, `${e.id} 「${e.name}」 탈출. ${row ? `${row.name} 요원의 ${what} 중` : what} 격리가 무너졌다. RAVEN-3 투입, 재격리 완료, 안정도 ${RAVEN_RESTORE}%. 담당 요원 오염도 +${ESC_CONT}. 섹터 전 요원 보급품 −${RAVEN_COST}. 격리실 ${hours}시간 봉쇄.`, '벌처: ' + VULTURE[die(VULTURE.length) - 1]);
  });
  io.emit('agents', Q.agents.all().map(publicAgent));
}
// 위협 등급이 높을수록 빨리 떨어진다 (DECAY_PER_HOUR는 등급 Ⅲ 기준값)
const THREAT_DECAY = { 'Ⅰ': 0.5, 'Ⅱ': 0.75, 'Ⅲ': 1, 'Ⅳ': 1.25, 'Ⅴ': 1.5 };
const decayOf = id => { const e = ENTITIES.find(x => x.id === id); return DECAY_PER_H * (THREAT_DECAY[e && e.threat] || 1); };
const stabNow = row => row.captured ? Math.max(0, row.stability - decayOf(row.id) * (now() - row.updated) / 3600000) : 0;
function sectorOut() {
  const un = QS.unlocks.all();
  return {
    decay: DECAY_PER_H, cost: COST, raven: { cost: RAVEN_COST, restore: RAVEN_RESTORE },
    tasks: TASKS, check: { maintain: { stat: SN[CHECK.maintain.stat], skill: CHECK.maintain.skill }, observe: { stat: SN[CHECK.observe.stat], skill: CHECK.observe.skill }, emergency: { stat: SN[CHECK.emergency.stat], skill: CHECK.emergency.skill } }, esc: { low: ESC_LOW, lowAdd: ESC_LOW_ADD, anom: ESC_ANOM, cont: ESC_CONT },
    notice: (() => { const n = QS.events.all().find(x => x.kind === 'notice'); return n && now() - n.at < 864e5 ? { text: n.text, at: n.at } : null; })(),
    events: QS.events.all().filter(x => x.kind !== 'notice').slice(0, 8).map(x => ({ id: x.id, at: x.at, kind: x.kind, entity: x.entity, text: x.text, voice: x.voice })),
    entities: ENTITIES.map(e => {
      const row = QS.ent.get(e.id), st = Math.round(stabNow(row) * 10) / 10;
      const open = sec => sec === 'info' || sec === 'incidents' || un.some(u => u.entity === e.id && u.section === sec);
      const by = sec => { const u = un.find(x => x.entity === e.id && x.section === sec); return u ? { name: u.by_name, at: u.at } : null; };
      const doc = { info: { summary: e.summary, cell: e.cell, threat: e.threat } };
      if (e.op) {
        doc.procedure = open('procedure') ? { lines: e.procedure, by: by('procedure') } : null;
        doc.observe = open('observe') ? { lines: e.observe, by: by('observe') } : null;
        doc.sealed = open('sealed') ? { text: e.sealed, by: by('sealed') } : null;
        doc.incidents = QS.incidents.all(e.op).map(r => ({ id: r.id, result: r.result, ended: r.ended, members: JSON.parse(r.members).map(m => ({ name: m.name, dead: m.dead })) }));
      }
      // 관찰 기록: 열람(구매·RAVEN-1)하면 전부, 아니면 관리 작업의 관찰 진척만큼
      if (e.observe && !(doc.observe)) { const k = Math.min(e.observe.length, Math.floor((row.obs || 0) / OBS_PER_LINE)); doc.observe = k ? { lines: e.observe.slice(0, k), by: { name: '관찰 작업', at: row.updated } } : null; }
      doc.obs = { pts: row.obs || 0, per: OBS_PER_LINE, total: (e.observe || []).length };
      doc.breaches = QS.events.all().filter(x => x.entity === e.id && (x.kind === 'breach' || x.kind === 'escape')).map(x => ({ at: x.at, kind: x.kind, text: x.text }));
      return { id: e.id, name: e.name, icon: e.icon, threat: e.threat, cell: e.cell, op: e.op, captured: !!row.captured, stability: st, lockUntil: row.lock_until > now() ? row.lock_until : 0, decay: decayOf(e.id), diff: diffOf(e), escBase: THREAT_ESC[e.threat] || 0.15, doc, sections: e.op ? ['info', 'procedure', 'observe', 'incidents', 'sealed'] : ['info', 'observe'],
        workers: QT.activeOn.all(e.id, now()).map(t => ({ uid: String(t.user_id), name: t.name, kind: t.kind, ends: t.ends })) };
    })
  };
}
const emitSector = () => io.emit('sector', sectorOut());
function spend(uid, cost) {
  const row = Q.agent.get(uid); if (!row) return 'agent/none';
  const res = JSON.parse(row.res);
  for (const k in cost) if ((res[k] || 0) < cost[k]) return 'res/short';
  for (const k in cost) res[k] -= cost[k];
  QS.setRes.run(JSON.stringify(res), uid);
  return null;
}
// 작전 성공 → 해당 개체 격리 복구, 격리 절차 자동 열람
function sectorOnOp(opId, result, names) {
  const base = opDef(opId);
  if (base && base.patrol) {
    if (result !== 'success') return;
    for (const x of ENTITIES) { const row = QS.ent.get(x.id); if (row && row.captured && !(row.lock_until > now())) QS.setEnt.run(Math.min(100, stabNow(row) + PATROL_STAB), now(), 1, x.id); }
    return;
  }
  const e = ENTITIES.find(x => x.op === opId); if (!e) return;
  if (result === 'success') {
    QS.setEnt.run(100, now(), 1, e.id);
    QS.unlock.run(e.id, 'procedure', names || '작전 기록', now());
  }
}
let lastBase = null;
function baseAct(uid, kind, entity) {
  lastBase = null;
  const row = Q.agent.get(uid); if (!row) return 'agent/none';
  return transaction(() => {
    if (kind === 'maintain') {
      const e = ENTITIES.find(x => x.id === entity), er = e && QS.ent.get(e.id);
      if (!er || !er.captured) return 'base/not-captured';
      if (er.lock_until > now()) return 'base/raven';
      const st = stabNow(er); if (st >= 100) return 'base/full';
      const err = spend(uid, COST.maintain); if (err) return err;
      const roll = containRoll(row, e, 'emergency', 0), gain = { crit: 25, ok: 20, cost: 10, fail: -5 }[roll.tier], after = Math.max(0, Math.min(100, st + gain));
      QS.setEnt.run(after, now(), 1, e.id);
      if (roll.tier === 'cost') QS.setCont.run((row.contamination || 0) + 5, uid);
      const cm = skLv(row, '통신'), p = roll.tier === 'fail' ? escChance(e, after, roll.anomalies) * Math.max(0.25, 1 - COMMS_ESC * cm) : 0;
      lastBase = { kind: 'emergency', entity: e.id, entityName: e.name, roll, stab: gain, cont: roll.tier === 'cost' ? 5 : 0, escape: roll.tier === 'fail' ? { p, escaped: Math.random() < p, comms: cm } : null };
    } else if (kind === 'purge') {
      if (!row.contamination) return 'base/clean';
      const err = spend(uid, COST.purge); if (err) return err;
      QS.setCont.run(Math.max(0, row.contamination - 5), uid);
    } else if (kind === 'hasten') {
      if (!row.regen_until || row.regen_until <= now()) return 'base/not-regen';
      const err = spend(uid, COST.hasten); if (err) return err;
      QS.setRegen.run(Math.max(now(), row.regen_until - 3600000), uid);
    } else return 'base/bad';
    return null;
  });
}
function archiveUnlock(uid, entity, section) {
  const e = ENTITIES.find(x => x.id === entity); if (!e || !e.op) return 'archive/bad';
  if (!['observe', 'sealed'].includes(section)) return 'archive/bad';
  if (QS.unlocks.all().some(u => u.entity === e.id && u.section === section)) return 'archive/open';
  const row = Q.agent.get(uid); if (!row) return 'agent/none';
  return transaction(() => {
    const err = spend(uid, COST[section]); if (err) return err;
    QS.unlock.run(e.id, section, row.name, now());
    return null;
  });
}
/* ───────── 개체 관리 작업 (뽀모도로) ───────── */
// 작업을 걸어 두면 서버에서 시간이 흐른다. 끝나면 결과가 반영되고, 요원이 '확인'할 때까지 결과 카드가 남는다.
const TASK_MIN_MS = Number(process.env.TASK_MINUTE_MS || 60000); // 테스트할 때 줄여 쓴다
const OBS_PER_LINE = 3;
const TASKS = {
  maintain: { name: '유지 보수', len: { 30: { stab: 6, supply: 2 }, 60: { stab: 12, supply: 5 }, 120: { stab: 25, supply: 10 }, 180: { stab: 40, supply: 18 } } },
  observe: { name: '관찰·연구', len: { 30: { research: 1, obs: 1 }, 60: { research: 2, obs: 2 }, 120: { research: 4, obs: 3 }, 180: { research: 7, obs: 5 } } }
};
/* 이상 징후: 작업 중에 가끔 뜨는 작은 사건. 자리에 있던 요원이 대응하면 보너스, 놓쳐도 벌점은 없다. */
const SIGN_WINDOW = 10; // 대응할 수 있는 시간(작업 분 단위)
const SIGN_COUNT = { 30: [0, 1], 60: [1, 1], 120: [1, 2], 180: [2, 2] };
const SIGN_TEXT = ['격리실 온도가 갑자기 4도 떨어졌다.', '감시 카메라가 3초 동안 꺼졌다 켜졌다.', '{name}이(가) 관찰창 쪽으로 몸을 돌렸다.', '격리실 안에서 노크 소리가 세 번 들렸다.', '출입 기록 단말에 아무도 들어가지 않은 출입이 찍혔다.', '격리실 벽의 습도계 바늘이 끝까지 돌아갔다.', '환기구에서 누군가 숨 쉬는 소리가 들린다.', '관찰 일지의 마지막 줄이 저절로 지워졌다.', '격리실 조명이 다른 색으로 깜빡였다.', '{name}의 그림자가 한 박자 늦게 움직였다.'];
const SIGN_BONUS = { maintain: { stab: 5, supply: 2 }, observe: { research: 1, obs: 1 } };
function makeSigns(mins, started) {
  const [a, b] = SIGN_COUNT[mins] || [1, 1], n = a + (b > a && Math.random() < 0.5 ? 1 : 0), out = [];
  for (let i = 0; i < n; i++) {
    const lo = 0.15 + (0.7 / n) * i, at = started + Math.round((lo + Math.random() * (0.7 / n - 0.05)) * mins * TASK_MIN_MS);
    out.push({ id: i + 1, at, until: at + SIGN_WINDOW * TASK_MIN_MS, text: SIGN_TEXT[die(SIGN_TEXT.length) - 1], state: 'wait' });
  }
  return out;
}
const QT = {
  mine: db.prepare("SELECT * FROM tasks WHERE user_id = ? AND (status = 'run' OR (status = 'done' AND acked = 0)) ORDER BY id DESC LIMIT 1"),
  add: db.prepare("INSERT INTO tasks (user_id, entity, kind, mins, started, ends, status, signs) VALUES (?, ?, ?, ?, ?, ?, 'run', ?)"),
  setSigns: db.prepare('UPDATE tasks SET signs = ? WHERE id = ?'),
  running: db.prepare("SELECT * FROM tasks WHERE status = 'run'"),
  due: db.prepare("SELECT * FROM tasks WHERE status = 'run' AND ends <= ?"),
  set: db.prepare('UPDATE tasks SET status = ?, result = ? WHERE id = ?'),
  ack: db.prepare("UPDATE tasks SET acked = 1 WHERE user_id = ? AND status IN ('done', 'cancel')"),
  cancel: db.prepare("UPDATE tasks SET status = 'cancel', acked = 1 WHERE user_id = ? AND status = 'run'"),
  activeOn: db.prepare("SELECT t.user_id, t.kind, t.ends, a.name FROM tasks t JOIN agents a ON a.user_id = t.user_id WHERE t.entity = ? AND t.status = 'run' AND t.ends > ?"),
  overlap: db.prepare("SELECT COUNT(DISTINCT user_id) n FROM tasks WHERE entity = ? AND user_id != ? AND status IN ('run', 'done') AND started < ? AND ends > ?"),
  setObs: db.prepare('UPDATE sector_entities SET obs = obs + ? WHERE id = ?')
};
const signText = (sg, ent) => { const e = ENTITIES.find(x => x.id === ent), nm = e ? e.name : '개체'; return sg.text.replace('{name}이(가)', jo(nm, '이', '가')).replace('{name}', nm); };
function taskOut(t) {
  if (!t) return null;
  const sg = JSON.parse(t.signs || '[]'), live = sg.find(x => x.state === 'wait' && x.at <= now() && x.until > now());
  return { id: t.id, entity: t.entity, kind: t.kind, mins: t.mins, started: t.started, ends: t.ends, status: t.status, result: t.result ? JSON.parse(t.result) : null,
    sign: live && t.status === 'run' ? { id: live.id, text: signText(live, t.entity), until: live.until } : null,
    signs: { total: sg.length, ok: sg.filter(x => x.state === 'ok').length, seen: sg.filter(x => x.at <= now()).length } };
}
function taskStart(uid, entity, kind, mins) {
  const row = Q.agent.get(uid); if (!row) return 'agent/none';
  const T = TASKS[kind]; if (!T || !T.len[mins]) return 'task/bad';
  const e = ENTITIES.find(x => x.id === entity), er = e && QS.ent.get(e.id);
  if (!er || !er.captured) return 'base/not-captured';
  if (er.lock_until > now()) return 'base/raven';
  if (row.regen_until && row.regen_until > now()) return 'op/regen';
  if (row.quar_until && row.quar_until > now()) return 'op/quarantine';
  for (const r of runs.values()) if (r.status === 'live' && r.members.some(m => m.uid === String(uid))) return 'task/in-op';
  const cur = QT.mine.get(uid); if (cur && cur.status === 'run') return 'task/busy';
  if (cur) QT.ack.run(uid);
  const st0 = now(); QT.add.run(uid, e.id, kind, mins, st0, st0 + mins * TASK_MIN_MS, JSON.stringify(makeSigns(mins, st0)));
  return null;
}
const FAIL_DROP = { 30: 6, 60: 10, 120: 18, 180: 25 };
const TIER_MUL = { crit: 1.5, ok: 1, cost: 0.5, fail: 0 };
function taskFinish(t) {
  const T = TASKS[t.kind], mm = t.mins, base = T.len[mm], e = ENTITIES.find(x => x.id === t.entity);
  const n = Math.min(2, QT.overlap.get(t.entity, t.user_id, t.ends, t.started).n), mul = 1 + 0.25 * n;
  const out = { name: T.name, entity: t.entity, entityName: e ? e.name : '', together: n, mul };
  // 같은 격리실 동료 한 명당 지원 주사위 1개
  const g = v => { const x = v + Math.ceil(v * (mul - 1)); return Math.ceil(x * TIER_MUL[out.roll.tier]); };
  let esc = null;
  transaction(() => {
    const row = Q.agent.get(t.user_id); if (!row || !e) { QT.set.run('done', JSON.stringify(out), t.id); return; }
    const res = JSON.parse(row.res), er = QS.ent.get(t.entity), live = er && er.captured && !(er.lock_until > now());
    out.roll = containRoll(row, e, t.kind, n);
    const tier = out.roll.tier, sg = JSON.parse(t.signs || '[]'), sok = sg.filter(x => x.state === 'ok').length, SB = SIGN_BONUS[t.kind];
    out.signOk = sok; out.signTotal = sg.length;
    if (tier === 'fail') {
      if (live) {
        const st = stabNow(er), after = Math.max(0, st - FAIL_DROP[mm]); QS.setEnt.run(after, now(), 1, t.entity); out.stab = -(Math.round((st - after) * 10) / 10);
        const cm = skLv(row, '통신'), p = escChance(e, after, out.roll.anomalies) * Math.max(0.25, 1 - COMMS_ESC * cm) * (sok ? 0.5 : 1); out.escape = { p, escaped: Math.random() < p, comms: cm, signs: sok }; if (out.escape.escaped) esc = e;
      }
    } else {
      if (t.kind === 'maintain') {
        if (live) { const st = stabNow(er), gain = g(base.stab) + SB.stab * sok; QS.setEnt.run(Math.min(100, st + gain), now(), 1, t.entity); out.stab = gain; }
        out.supply = g(base.supply) + SB.supply * sok; res.supply += out.supply;
      } else {
        out.research = g(base.research) + skLv(row, '기록 해독') + SB.research * sok; res.research += out.research; if (skLv(row, '기록 해독')) out.decode = skLv(row, '기록 해독');
        out.obs = g(base.obs) + SB.obs * sok; QT.setObs.run(out.obs, t.entity);
        if (e.observe) { const before = Math.floor((er.obs || 0) / OBS_PER_LINE), after = Math.floor(((er.obs || 0) + out.obs) / OBS_PER_LINE); if (after > before && before < e.observe.length) out.newLine = true; }
      }
      if (tier === 'cost') { out.cont = 5; QS.setCont.run((row.contamination || 0) + 5, t.user_id); }
      QS.setRes.run(JSON.stringify(res), t.user_id);
    }
    QT.set.run('done', JSON.stringify(out), t.id);
  });
  if (out.roll) { out.xp = gainXp(t.user_id, out.roll.skill, XP.task[mm]); out.career = gainCareer(t.user_id, CAREER.task[mm]); QT.set.run('done', JSON.stringify(out), t.id); }
  if (out.roll) bumpAch(t.user_id, { tasks: 1 });
  if (esc) escapeIn(esc, t.user_id, T.name + ' 작업');
  return out;
}
function signTick() {
  for (const t of QT.running.all()) {
    const sg = JSON.parse(t.signs || '[]'); let ch = false;
    for (const x of sg) {
      if (x.state === 'wait' && x.at <= now() && !x.told) { x.told = 1; ch = true; io.to('u:' + t.user_id).emit('sign', { entity: t.entity, text: signText(x, t.entity), until: x.until }); }
      if (x.state === 'wait' && x.until <= now()) { x.state = 'miss'; ch = true; }
    }
    if (ch) { QT.setSigns.run(JSON.stringify(sg), t.id); const row = Q.agent.get(t.user_id); if (row) io.emit('agent', publicAgent(row)); }
  }
}
function taskTick() {
  signTick();
  const due = QT.due.all(now()); if (!due.length) return;
  for (const t of due) { taskFinish(t); const row = Q.agent.get(t.user_id); if (row) io.emit('agent', publicAgent(row)); }
  emitSector();
}
setInterval(() => { try { taskTick(); } catch (e) { console.error(e); } }, Math.min(15000, Math.max(1000, TASK_MIN_MS / 4))).unref();
app.post('/api/task', need, (req, res) => {
  const b = req.body || {}, err = taskStart(req.user.id, b.entity, b.kind, Number(b.mins));
  if (err) return fail(res, err);
  const agent = publicAgent(Q.agent.get(req.user.id)); io.emit('agent', agent); emitSector(); res.json({ agent });
});
app.post('/api/task/cancel', need, (req, res) => {
  QT.cancel.run(req.user.id);
  const agent = publicAgent(Q.agent.get(req.user.id)); io.emit('agent', agent); emitSector(); res.json({ agent });
});
app.post('/api/task/sign', need, (req, res) => {
  const row = Q.agent.get(req.user.id), t = QT.mine.get(req.user.id);
  if (!row || !t || t.status !== 'run') return fail(res, 'task/none');
  const sg = JSON.parse(t.signs || '[]'), x = sg.find(y => y.state === 'wait' && y.at <= now() && y.until > now());
  if (!x) return fail(res, 'task/no-sign');
  const e = ENTITIES.find(y => y.id === t.entity), roll = containRoll(row, e, t.kind, 0);
  roll.diff = 1; roll.tier = containTier(roll.succ, 1); roll.tierName = TIER_NAME[roll.tier];
  x.state = roll.tier === 'fail' ? 'fail' : 'ok';
  QT.setSigns.run(JSON.stringify(sg), t.id);
  const xp = gainXp(req.user.id, roll.skill, 1);
  bumpAch(req.user.id, { signs: 1 });
  const agent = publicAgent(Q.agent.get(req.user.id)); io.emit('agent', agent);
  res.json({ agent, sign: { text: signText(x, t.entity), ok: x.state === 'ok', roll, bonus: SIGN_BONUS[t.kind], xp } });
});
app.post('/api/task/ack', need, (req, res) => {
  QT.ack.run(req.user.id);
  const agent = publicAgent(Q.agent.get(req.user.id)); io.emit('agent', agent); res.json({ agent });
});
app.post('/api/stat', need, (req, res) => {
  const k = Number(req.body && req.body.k), row = Q.agent.get(req.user.id);
  if (!row) return fail(res, 'agent/none');
  if (!isInt(k, 0, 5)) return fail(res, 'agent/bad-field');
  if (!(row.stat_pts > 0)) return fail(res, 'stat/no-points');
  const st = JSON.parse(row.stats); if (st[k] >= STAT_CAP) return fail(res, 'stat/cap');
  st[k]++;
  db.prepare('UPDATE agents SET stats = ?, stat_pts = stat_pts - 1 WHERE user_id = ?').run(JSON.stringify(st), req.user.id);
  const agent = publicAgent(Q.agent.get(req.user.id)); io.emit('agent', agent); res.json({ agent });
});
/* ───────── 보급 상점 API ───────── */
const saveInv = (uid, v) => db.prepare('UPDATE agents SET inv = ? WHERE user_id = ?').run(JSON.stringify(v), uid);
const pushAgent = uid => { const agent = publicAgent(Q.agent.get(uid)); io.emit('agent', agent); return agent; };
app.get('/api/shop', need, (req, res) => res.json({ items: SHOP, useMax: USE_MAX, titles: TITLES.map(({ id, name, desc, key, need }) => ({ id, name, desc, key, need })) }));
app.post('/api/shop/buy', need, (req, res) => {
  const row = Q.agent.get(req.user.id); if (!row) return fail(res, 'agent/none');
  const it = SHOP_BY[req.body && req.body.id]; if (!it) return fail(res, 'shop/none');
  const v = invOf(row), r = JSON.parse(row.res);
  if (it.kind === 'use') { if ((v.items[it.id] || 0) >= USE_MAX) return fail(res, 'shop/full'); }
  else if (v.own.includes(it.id)) return fail(res, 'shop/owned');
  if ((r.supply || 0) < it.price) return fail(res, 'res/short');
  if (it.kind === 'use' && busyIn(req.user.id) && busyIn(req.user.id).status === 'live') return fail(res, 'shop/in-op');
  r.supply -= it.price;
  if (it.kind === 'use') v.items[it.id] = (v.items[it.id] || 0) + 1;
  else { v.own.push(it.id); if (it.kind === 'gear' && !v.eq[it.slot]) v.eq[it.slot] = it.id; if (it.kind === 'frame' && !v.frame) v.frame = it.id; if (it.kind === 'title' && !v.title) v.title = it.id; }
  db.prepare('UPDATE agents SET res = ?, inv = ? WHERE user_id = ?').run(JSON.stringify(r), JSON.stringify(v), req.user.id);
  res.json({ agent: pushAgent(req.user.id) });
});
// 튜토리얼(첫 출동): 진행은 브라우저에서 하고, 끝나면 보상만 받는다. 보급품 20과 칭호는 처음 한 번만.
app.post('/api/tutorial/done', need, (req, res) => {
  const row = Q.agent.get(req.user.id); if (!row) return fail(res, 'agent/none');
  const c = achOf(row), skip = !!(req.body && req.body.skip), first = !skip && !c.tutDone;
  if (first) { const r = JSON.parse(row.res); r.supply = (r.supply || 0) + 20; db.prepare('UPDATE agents SET res = ? WHERE user_id = ?').run(JSON.stringify(r), req.user.id); bumpAch(req.user.id, { tutDone: 1, tutSeen: c.tutSeen ? 0 : 1 }); }
  else if (!c.tutSeen) bumpAch(req.user.id, { tutSeen: 1 });
  res.json({ agent: pushAgent(req.user.id), first });
});
// 보급 요청서: 여러 물품을 한 번에 산다. 하나라도 안 되면 아무것도 사지 않는다.
app.post('/api/shop/order', need, (req, res) => {
  const row = Q.agent.get(req.user.id); if (!row) return fail(res, 'agent/none');
  const want = (req.body && req.body.items) || {}, v = invOf(row), r = JSON.parse(row.res);
  const L = Object.entries(want).map(([id, q]) => [SHOP_BY[id], Number(q)]).filter(([, q]) => q > 0);
  if (!L.length) return fail(res, 'shop/empty');
  let total = 0;
  for (const [it, q] of L) {
    if (!it || !Number.isInteger(q)) return fail(res, 'shop/none');
    if (it.kind === 'use') { if ((v.items[it.id] || 0) + q > USE_MAX) return fail(res, 'shop/full'); }
    else if (q !== 1 || v.own.includes(it.id)) return fail(res, 'shop/owned');
    total += it.price * q;
  }
  const live = busyIn(req.user.id); if (live && live.status === 'live' && L.some(([it]) => it.kind === 'use')) return fail(res, 'shop/in-op');
  if ((r.supply || 0) < total) return fail(res, 'res/short');
  r.supply -= total;
  for (const [it, q] of L) {
    if (it.kind === 'use') v.items[it.id] = (v.items[it.id] || 0) + q;
    else { v.own.push(it.id); if (it.kind === 'gear' && !v.eq[it.slot] && !live) v.eq[it.slot] = it.id; if (it.kind === 'frame' && !v.frame) v.frame = it.id; if (it.kind === 'title' && !v.title) v.title = it.id; }
  }
  db.prepare('UPDATE agents SET res = ?, inv = ? WHERE user_id = ?').run(JSON.stringify(r), JSON.stringify(v), req.user.id);
  res.json({ agent: pushAgent(req.user.id), total });
});
// 장착·해제: kind는 gear(slot과 함께)·frame·title, id가 없으면 해제
app.post('/api/shop/equip', need, (req, res) => {
  const row = Q.agent.get(req.user.id); if (!row) return fail(res, 'agent/none');
  const b = req.body || {}, v = invOf(row), id = b.id || null;
  if (busyIn(req.user.id) && b.kind === 'gear') return fail(res, 'shop/in-op');
  if (id && b.kind === 'title') { if (!TITLE_BY[id] || !v.titles.includes(id)) return fail(res, 'shop/none'); }
  else if (id) { const it = SHOP_BY[id]; if (!it || !v.own.includes(id) || it.kind !== b.kind) return fail(res, 'shop/none'); }
  if (b.kind === 'gear') { if (!SLOTS.includes(b.slot) || (id && SHOP_BY[id].slot !== b.slot)) return fail(res, 'shop/none'); v.eq[b.slot] = id; }
  else if (b.kind === 'frame') v.frame = id; else if (b.kind === 'title') v.title = id; else return fail(res, 'shop/none');
  saveInv(req.user.id, v); res.json({ agent: pushAgent(req.user.id) });
});
/* ───────── GM 도구 ───────── */
const gmAgentOut = row => ({ ...publicAgent(row) });
app.get('/api/gm/state', need, needGM, (req, res) => res.json({ agents: Q.agents.all().map(gmAgentOut), runs: [...runs.values()].map(r => runOut(r, req.user.id, true)) }));
app.post('/api/gm/agent', need, needGM, (req, res) => {
  const b = req.body || {}, uid = Number(b.uid), row = Q.agent.get(uid);
  if (!row) return fail(res, 'agent/none');
  const set = {};
  if (b.stats) { if (!Array.isArray(b.stats) || b.stats.length !== 6 || !b.stats.every(v => isInt(v, 1, 30))) return fail(res, 'gm/bad'); set.stats = JSON.stringify(b.stats); }
  if (b.skills) { const sk = {}; for (const k of SKILLS) { const v = Number(b.skills[k] || 0); if (!isInt(v, 0, SKILL_MAX)) return fail(res, 'gm/bad'); if (v) sk[k] = v; } set.skills = JSON.stringify(sk); }
  if (b.res) { const r = {}; for (const k of ['supply', 'research', 'pass']) { const v = Number(b.res[k]); if (!isInt(v, 0, 999999)) return fail(res, 'gm/bad'); r[k] = v; } set.res = JSON.stringify(r); }
  if (b.contamination != null) { if (!isInt(Number(b.contamination), 0, 300)) return fail(res, 'gm/bad'); set.contamination = Number(b.contamination); }
  if (b.career != null) { if (!isInt(Number(b.career), 0, 100000)) return fail(res, 'gm/bad'); set.career = Number(b.career); }
  if (b.statPts != null) { if (!isInt(Number(b.statPts), 0, 999)) return fail(res, 'gm/bad'); set.stat_pts = Number(b.statPts); }
  if (b.clearRegen) set.regen_until = null;
  if (b.clearQuar) set.quar_until = null;
  const keys = Object.keys(set); if (!keys.length) return fail(res, 'gm/bad');
  db.prepare(`UPDATE agents SET ${keys.map(k => k + ' = ?').join(', ')} WHERE user_id = ?`).run(...keys.map(k => set[k]), uid);
  const agent = publicAgent(Q.agent.get(uid)); io.emit('agent', agent); res.json({ agent });
});
// 작전 개입: 진행 중인 작전에 GM이 직접 손을 댄다. 기록에는 [GM]으로 남는다.
app.post('/api/gm/op', need, needGM, (req, res) => {
  const b = req.body || {}, r = runs.get(b.op); if (!r) return fail(res, 'op/none');
  const live = r.status === 'live', o = D(r), act = b.act;
  const needLive = () => live ? null : 'op/not-live';
  let err = null;
  if (act === 'narrate') { const t = str(b.text, 400); if (!t) err = 'chat/empty'; else if (!(err = needLive())) say(r, t, { kind: 'gm' }); }
  else if (act === 'dist') { if (!(err = needLive())) { r.dist = Math.max(0, Math.min(80, Math.round(Number(b.v) || 0))); say(r, `[GM] 개체 거리 조정: ${r.dist}m.`, { kind: 'gmsys' }); } }
  else if (act === 'count') { if (!(err = needLive())) { r.count = Math.max(0, Math.min(o.count, r.count + (Number(b.v) || 0))); say(r, `[GM] ${o.countLabel || '셈'} 조정: ${r.count}.`, { kind: 'gmsys' }); } }
  else if (act === 'noise') { if (!(err = needLive())) { if (r.noise) r.noise = null; else noiseStart(r); } }
  else if (act === 'shift') { if (!(err = needLive())) { if (!D(r).scenes[r.scene].shiftable) err = 'op/bad-choice'; else { r.shift = { kind: SHIFT_KINDS[b.kind] ? b.kind : pick(['extend', 'marks', 'flip']), at: r.turn + 1 }; say(r, '[GM] 구조 변형을 일으켰다.', { kind: 'gmsys' }); } } }
  else if (act === 'spawn') { if (!(err = needLive())) spawnMinion(r, b.side === 'rear' ? 'rear' : 'front', 1); }
  else if (act === 'clearMinions') { if (!(err = needLive())) { r.minions = []; say(r, '[GM] 하수인을 모두 치웠다.', { kind: 'gmsys' }); } }
  else if (act === 'restore') {
    if (!(err = needLive())) { const m = r.members.find(x => x.uid === String(b.uid)); if (!m) err = 'op/no-target'; else { m.hp = m.hpMax; m.san = m.sanMax; m.dead = false; m.broken = false; m.recovered = false; say(r, `[GM] ${m.name} 요원의 상태를 되돌렸다.`, { kind: 'gmsys' }); } }
  }
  else if (act === 'scene') { if (!(err = needLive())) { if (r.scene + 1 >= o.scenes.length) { say(r, '[GM] 마지막 장면을 넘겼다.', { kind: 'gmsys' }); opFinish(r, 'success'); } else { r.scene++; r.dist = o.scenes[r.scene].dist; r.pos = o.scenes[r.scene].map.start; r.minions = []; say(r, o.scenes[r.scene].text, { kind: 'scene', title: o.scenes[r.scene].title }); sceneEnter(r); startRound(r); } } }
  else if (act === 'end') { if (!(err = needLive())) { say(r, `[GM] 작전을 ${b.result === 'success' ? '성공' : b.result === 'fail' ? '실패' : '중단'}으로 종료한다.`, { kind: 'gmsys' }); opFinish(r, ['success', 'fail'].includes(b.result) ? b.result : 'abort'); } }
  else if (act === 'reset') { if (live) err = 'op/started'; else { runs.delete(r.op); lobbyOf(b.op); } }
  else err = 'gm/bad';
  if (err) return fail(res, err);
  const rr = runs.get(b.op); if (rr) emitRun(rr);
  res.json({ ok: true });
});
app.post('/api/gm/entity', need, needGM, (req, res) => {
  const b = req.body || {}, e = ENTITIES.find(x => x.id === b.id), row = e && QS.ent.get(e.id);
  if (!row) return fail(res, 'gm/bad');
  const cap = b.captured != null ? (b.captured ? 1 : 0) : row.captured;
  const st = b.stability != null ? Math.max(0, Math.min(100, Number(b.stability) || 0)) : stabNow(row);
  QS.setEnt.run(st, now(), cap, e.id);
  if (b.unlock) db.prepare('UPDATE sector_entities SET lock_until = 0 WHERE id = ?').run(e.id);
  if (b.obs != null && isInt(Number(b.obs), 0, 99)) db.prepare('UPDATE sector_entities SET obs = ? WHERE id = ?').run(Number(b.obs), e.id);
  emitSector(); res.json({ ok: true });
});
app.post('/api/gm/event', need, needGM, (req, res) => {
  const b = req.body || {}, text = str(b.text, 400), voice = str(b.voice, 200);
  if (!text) return fail(res, 'chat/empty');
  const ent = ENTITIES.find(x => x.id === b.entity);
  QS.addEvent.run(now(), b.kind === 'notice' ? 'notice' : 'gm', ent ? ent.id : null, text, voice || null);
  if (b.kind === 'notice') io.emit('notice', { text, level: b.level === 'alert' ? 'alert' : 'info', at: now() });
  emitSector(); res.json({ ok: true });
});
app.get('/api/sector', need, (req, res) => res.json(sectorOut()));
app.post('/api/base', need, (req, res) => {
  const err = baseAct(req.user.id, req.body && req.body.kind, req.body && req.body.entity);
  if (err) return fail(res, err);
  const roll = lastBase; lastBase = null;
  if (roll && roll.roll) { roll.xp = gainXp(req.user.id, roll.roll.skill, XP.emergency); roll.career = gainCareer(req.user.id, CAREER.emergency); }
  if (roll && roll.escape && roll.escape.escaped) escapeIn(ENTITIES.find(x => x.id === roll.entity), req.user.id, '긴급 점검');
  emitSector(); const agent = publicAgent(Q.agent.get(req.user.id)); io.emit('agent', agent); res.json({ agent, roll });
});
app.post('/api/archive/unlock', need, (req, res) => {
  const err = archiveUnlock(req.user.id, req.body && req.body.entity, req.body && req.body.section);
  if (err) return fail(res, err);
  emitSector(); const agent = publicAgent(Q.agent.get(req.user.id)); io.emit('agent', agent); res.json({ agent });
});

app.get('/api/records', need, (req, res) => res.json({ runs: Q.runs.all().map(r => ({ ...r, members: JSON.parse(r.members) })) }));
app.get('/api/records/:id', need, (req, res) => { const r = Q.runLog.get(Number(req.params.id)); if (!r) return fail(res, 'not-found', 404); res.json({ log: JSON.parse(r.log) }); });

// 알 수 없는 API 주소와 요청 오류도 JSON으로 답한다
app.use('/api', (req, res) => fail(res, 'not-found', 404));
app.use((err, req, res, next) => {
  if (err && err.type === 'entity.too.large') return fail(res, 'too-large', 413);
  if (err && err.type === 'entity.parse.failed') return fail(res, 'bad-json', 400);
  console.error(err); fail(res, 'server', 500);
});

/* ───────── 실시간 (Socket.io) ───────── */
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: false } });

io.use((socket, next) => {
  const u = authUser(socket.handshake.auth && socket.handshake.auth.token);
  if (!u) return next(new Error('auth/required'));
  socket.user = u; next();
});

/* ───────── 개인 메신저 ───────── */
const QD = {
  add: db.prepare('INSERT INTO dm (from_id, to_id, text, ts) VALUES (?, ?, ?, ?)'),
  history: db.prepare('SELECT * FROM dm WHERE (from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?) ORDER BY id DESC LIMIT 200'),
  mine: db.prepare('SELECT * FROM dm WHERE from_id = ? OR to_id = ? ORDER BY id DESC LIMIT 1000'),
  read: db.prepare('UPDATE dm SET read = 1 WHERE from_id = ? AND to_id = ? AND read = 0'),
  user: db.prepare('SELECT id, login FROM users WHERE id = ?')
};
const dmOut = d => ({ id: d.id, from: String(d.from_id), to: String(d.to_id), text: d.text, ts: d.ts, read: d.read });
// 요원이 있거나 GM인 계정만 주고받을 수 있다
const dmCan = id => !!Q.agent.get(id) || isGM(QD.user.get(id));
function dmList(uid) {
  const conv = new Map();
  for (const d of QD.mine.all(uid, uid)) {
    const other = d.from_id === uid ? d.to_id : d.from_id;
    if (!conv.has(other)) conv.set(other, { with: String(other), last: dmOut(d), unread: 0 });
    if (d.to_id === uid && !d.read) conv.get(other).unread++;
  }
  return { gms: Q.allUsers.all().filter(isGM).map(u => String(u.id)), conv: [...conv.values()] };
}
io.on('connection', socket => {
  const uid = socket.user.id;
  online.set(uid, (online.get(uid) || 0) + 1);
  Q.seen.run(now(), uid);
  socket.emit('agents', Q.agents.all().map(publicAgent));
  socket.emit('chat:history', Q.chatRecent.all().map(chatOut));
  const me = Q.agent.get(uid);
  if (me) io.emit('agent', publicAgent(me));

  socket.emit('ops', [...runs.values()].map(r => runOut(r, uid, isGM(socket.user))));
  socket.join('u:' + uid);
  socket.emit('dm:list', dmList(uid));
  socket.emit('sector', sectorOut());
  const opCall = fn => (m, ack) => {
    const reply = typeof ack === 'function' ? ack : () => {};
    const opId = m && m.op, err = fn(m || {}, opId);
    if (err) return reply({ error: err });
    const r = runs.get(opId); if (r) emitRun(r);
    reply({ ok: true });
  };
  socket.on('op:join', opCall((m, id) => opJoin(uid, id, 'field')));
  socket.on('op:leave', opCall((m, id) => opLeave(uid, id)));
  socket.on('op:ready', opCall((m, id) => opReady(uid, id, m.on)));
  socket.on('op:start', opCall((m, id) => opStart(uid, id)));
  socket.on('op:act', opCall((m, id) => opPick(uid, id, 'act', m.choice)));
  socket.on('op:move', opCall((m, id) => opPick(uid, id, 'move', m.kind)));
  socket.on('op:unpick', opCall((m, id) => opUnpick(uid, id)));
  socket.on('op:ctl', opCall(() => 'op/not-ctrl'));
  socket.on('op:cls', opCall((m, id) => opCls(uid, id, m.cls)));
  socket.on('op:item', opCall((m, id) => opItem(uid, id, m.item)));
  socket.on('op:abort', opCall((m, id) => opAbort(uid, id)));
  socket.on('op:close', opCall((m, id) => opClose(uid, id)));
  // 작전 채팅: 그 작전 참가자끼리만 (대기실·진행 중·종료 화면에서)
  let lastOpChat = 0;
  socket.on('op:chat', opCall((m, id) => {
    const r = runs.get(id); if (!r) return 'op/none';
    const mm = r.members.find(x => x.uid === String(uid)); if (!mm) return 'op/not-member';
    const text = str(m.text, 300); if (!text) return 'chat/empty';
    if (now() - lastOpChat < 400) return 'chat/too-fast'; lastOpChat = now();
    (r.chat || (r.chat = [])).push({ uid: mm.uid, name: mm.name, role: mm.role, text, ts: now() }); if (r.chat.length > 120) r.chat.shift();
    return null;
  }));

  let last = 0;
  socket.on('chat', (m, ack) => {
    const reply = typeof ack === 'function' ? ack : () => {};
    const a = Q.agent.get(uid);
    if (!a) return reply({ error: 'agent/none' });
    const text = str(m && m.text, 300), channel = CHANNELS.includes(m && m.channel) ? m.channel : 'all';
    if (!text) return reply({ error: 'chat/empty' });
    if (now() - last < 400) return reply({ error: 'chat/too-fast' });
    last = now();
    const ts = now(), info = Q.addChat.run(uid, a.num, a.name, a.role, channel, text, ts);
    io.emit('chat', { id: Number(info.lastInsertRowid), uid: String(uid), num: a.num, name: a.name, role: a.role, channel, text, ts });
    reply({ ok: true });
  });

  // 개인 메신저: 1:1. GM도 남의 대화는 볼 수 없다.
  let lastDm = 0;
  socket.on('dm:send', (m, ack) => {
    const reply = typeof ack === 'function' ? ack : () => {};
    const to = Number(m && m.to), text = str(m && m.text, 500);
    if (!dmCan(uid)) return reply({ error: 'agent/none' });
    if (!to || to === uid || !dmCan(to)) return reply({ error: 'dm/bad-target' });
    if (!text) return reply({ error: 'chat/empty' });
    if (now() - lastDm < 400) return reply({ error: 'chat/too-fast' });
    lastDm = now();
    const ts = now(), info = QD.add.run(uid, to, text, ts);
    const msg = { id: Number(info.lastInsertRowid), from: String(uid), to: String(to), text, ts, read: 0 };
    io.to('u:' + uid).to('u:' + to).emit('dm', msg);
    reply({ ok: true, msg });
  });
  socket.on('dm:history', (m, ack) => {
    const reply = typeof ack === 'function' ? ack : () => {};
    const w = Number(m && m.with); if (!w) return reply({ error: 'dm/bad-target' });
    reply({ ok: true, list: QD.history.all(uid, w, w, uid).reverse().map(dmOut) });
  });
  socket.on('dm:read', (m, ack) => {
    const w = Number(m && m.with); if (w) QD.read.run(w, uid);
    io.to('u:' + uid).emit('dm:read', { with: String(w) });
    if (typeof ack === 'function') ack({ ok: true });
  });

  socket.on('disconnect', () => {
    const n = (online.get(uid) || 1) - 1;
    if (n > 0) online.set(uid, n); else online.delete(uid);
    Q.seen.run(now(), uid);
    const row = Q.agent.get(uid);
    if (row) io.emit('agent', publicAgent(row));
  });
});

// 안정도가 시간에 따라 떨어지는 것을 5분마다 모두에게 알린다
setInterval(() => emitSector(), 5 * 60 * 1000).unref();
// 격리 실패 검사 (1분마다)
setInterval(() => { try { checkBreach(); } catch (e) { console.error(e); } }, 60 * 1000).unref();
setTimeout(() => { try { checkBreach(); } catch (e) { console.error(e); } }, 2000).unref();

server.listen(PORT, () => console.log(`SECTOR R-8 서버 가동: 포트 ${PORT}, 데이터 ${DATA_DIR}`));

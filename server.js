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
  email TEXT NOT NULL UNIQUE COLLATE NOCASE,
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
`);

const Q = {
  userByEmail: db.prepare('SELECT * FROM users WHERE email = ?'),
  addUser: db.prepare('INSERT INTO users (email, pw, created) VALUES (?, ?, ?)'),
  addSession: db.prepare('INSERT INTO sessions (token, user_id, created) VALUES (?, ?, ?)'),
  session: db.prepare('SELECT users.id, users.email FROM sessions JOIN users ON users.id = sessions.user_id WHERE token = ?'),
  dropSession: db.prepare('DELETE FROM sessions WHERE token = ?'),
  agent: db.prepare('SELECT * FROM agents WHERE user_id = ?'),
  agentByNum: db.prepare('SELECT user_id FROM agents WHERE num = ?'),
  agents: db.prepare('SELECT * FROM agents ORDER BY num'),
  addAgent: db.prepare(`INSERT INTO agents (user_id, num, name, role, head, age, stats, trait, skills, contamination, res, created, last_seen)
    VALUES (@user_id, @num, @name, @role, @head, @age, @stats, @trait, @skills, 0, @res, @created, @created)`),
  seen: db.prepare('UPDATE agents SET last_seen = ? WHERE user_id = ?'),
  sealed: db.prepare('SELECT data FROM sealed WHERE user_id = ?'),
  addSealed: db.prepare('INSERT INTO sealed (user_id, data) VALUES (?, ?)'),
  chatRecent: db.prepare('SELECT * FROM (SELECT * FROM chat ORDER BY id DESC LIMIT 300) ORDER BY id'),
  addChat: db.prepare('INSERT INTO chat (user_id, num, name, role, channel, text, ts) VALUES (?, ?, ?, ?, ?, ?, ?)')
};

/* ───────── 규칙 데이터 (설계서 기준, 검증용) ───────── */
const DEPT_SKILLS = [['장비 운용', '운반'], ['사격', '격투'], ['기록 해독', '관찰'], ['응급처치', '참기'], ['통신', '장비 운용'], ['기록 해독', '교섭'], ['은신', '관찰'], ['운반', '교섭'], ['교섭', '오컬트'], []];
const SKILLS = ['격투', '사격', '은신', '응급처치', '장비 운용', '기록 해독', '통신', '관찰', '오컬트', '교섭', '참기', '운반'];
const START_RES = { supply: 100, research: 0, pass: 1 };
const CHANNELS = ['all', 'field', 'ctrl'];

/* ───────── 도우미 ───────── */
const now = () => Date.now();
const isInt = (v, lo, hi) => Number.isInteger(v) && v >= lo && v <= hi;
const str = (v, max) => typeof v === 'string' ? v.trim().slice(0, max) : '';
const online = new Map(); // user_id → 연결 수

function publicAgent(row) {
  if (!row) return null;
  return {
    uid: String(row.user_id), num: row.num, name: row.name, role: row.role, head: row.head, age: row.age,
    stats: JSON.parse(row.stats), trait: row.trait, skills: JSON.parse(row.skills), contamination: row.contamination,
    res: JSON.parse(row.res), createdAt: row.created, lastSeen: row.last_seen, online: (online.get(row.user_id) || 0) > 0
  };
}
function chatOut(r) { return { id: r.id, uid: String(r.user_id), num: r.num, name: r.name, role: r.role, channel: r.channel, text: r.text, ts: r.ts }; }
function newToken(userId) { const t = crypto.randomBytes(32).toString('hex'); Q.addSession.run(t, userId, now()); return t; }
function authUser(token) { return token ? Q.session.get(String(token)) : null; }

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
app.use(express.json({ limit: '32kb' }));
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
  const email = str(req.body.email, 120).toLowerCase(), pw = typeof req.body.pw === 'string' ? req.body.pw : '';
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return fail(res, 'auth/invalid-email');
  if (pw.length < 6) return fail(res, 'auth/weak-password');
  if (Q.userByEmail.get(email)) return fail(res, 'auth/email-already-in-use');
  const info = Q.addUser.run(email, bcrypt.hashSync(pw, 10), now());
  res.json({ token: newToken(info.lastInsertRowid), user: { uid: String(info.lastInsertRowid), email } });
});

app.post('/api/login', (req, res) => {
  if (throttled(req.ip)) return fail(res, 'auth/too-many-requests', 429);
  const email = str(req.body.email, 120).toLowerCase(), pw = typeof req.body.pw === 'string' ? req.body.pw : '';
  if (!email) return fail(res, 'auth/invalid-email');
  if (!pw) return fail(res, 'auth/missing-password');
  const u = Q.userByEmail.get(email);
  if (!u || !bcrypt.compareSync(pw, u.pw)) return fail(res, 'auth/invalid-credential');
  res.json({ token: newToken(u.id), user: { uid: String(u.id), email: u.email } });
});

app.post('/api/logout', (req, res) => {
  Q.dropSession.run((req.get('authorization') || '').replace(/^Bearer /, ''));
  res.json({ ok: true });
});

app.get('/api/me', need, (req, res) => {
  const s = Q.sealed.get(req.user.id);
  res.json({ user: { uid: String(req.user.id), email: req.user.email }, agent: publicAgent(Q.agent.get(req.user.id)), sealed: s ? JSON.parse(s.data) : null });
});

app.get('/api/number/:num', need, (req, res) => res.json({ free: !Q.agentByNum.get(req.params.num) }));

// 출입증 발급: 재생 절차 결과를 검증하고 저장한다
app.post('/api/agent', need, (req, res) => {
  const uid = req.user.id, b = req.body || {};
  if (Q.agent.get(uid)) return fail(res, 'agent/exists');
  const num = str(b.num, 8), name = str(b.name, 12);
  if (!/^R8-\d{3}$/.test(num)) return fail(res, 'agent/bad-number');
  if (!name) return fail(res, 'agent/bad-name');
  if (!isInt(b.role, 0, 1) || !isInt(b.head, 0, 3) || !isInt(b.age, 18, 70) || !isInt(b.trait, 2, 12)) return fail(res, 'agent/bad-field');
  const stats = b.stats;
  if (!Array.isArray(stats) || stats.length !== 6 || !stats.every(v => isInt(v, 2, 7))) return fail(res, 'agent/bad-stats');
  const sum = stats.reduce((a, c) => a + c, 0);
  if (b.mode === 'pt' ? sum !== 24 : sum < 20) return fail(res, 'agent/bad-stats');
  const p = b.past || {};
  if (!isInt(p.dept, 0, 9)) return fail(res, 'agent/bad-past');
  const roll = p.mode !== 'write';
  if (roll && (!isInt(p.death, 0, 9) || !isInt(p.secret, 0, 9))) return fail(res, 'agent/bad-past');
  const story = str(p.story, 1500);
  if (!roll && !story) return fail(res, 'agent/bad-past');
  if (!isInt(b.free, 0, SKILLS.length - 1)) return fail(res, 'agent/bad-skill');
  const skills = {};
  DEPT_SKILLS[p.dept].forEach(k => { skills[k] = 1; });
  if (skills[SKILLS[b.free]]) return fail(res, 'agent/bad-skill');
  skills[SKILLS[b.free]] = 1;
  const realAge = b.age + 5 * (1 + ((p.dept + (roll ? p.death + p.secret : 0)) % 10));
  const sealed = roll
    ? { mode: 'roll', dept: p.dept, death: p.death, secret: p.secret, cross: str(p.cross, 8).toUpperCase(), realAge }
    : { mode: 'write', dept: p.dept, realName: str(p.realName, 20), story, cross: str(p.cross, 8).toUpperCase(), realAge };
  try {
    transaction(() => {
      if (Q.agentByNum.get(num)) throw Object.assign(new Error(), { code: 'number-taken' });
      Q.addAgent.run({ user_id: uid, num, name, role: b.role, head: b.head, age: b.age, stats: JSON.stringify(stats), trait: b.trait, skills: JSON.stringify(skills), res: JSON.stringify(START_RES), created: now() });
      Q.addSealed.run(uid, JSON.stringify(sealed));
    });
  } catch (e) {
    if (e.code === 'number-taken' || /UNIQUE/.test(e.message)) return fail(res, 'number-taken', 409);
    console.error(e); return fail(res, 'server', 500);
  }
  const agent = publicAgent(Q.agent.get(uid));
  io.emit('agent', agent);
  res.json({ agent, sealed });
});

/* ───────── 실시간 (Socket.io) ───────── */
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: false } });

io.use((socket, next) => {
  const u = authUser(socket.handshake.auth && socket.handshake.auth.token);
  if (!u) return next(new Error('auth/required'));
  socket.user = u; next();
});

io.on('connection', socket => {
  const uid = socket.user.id;
  online.set(uid, (online.get(uid) || 0) + 1);
  Q.seen.run(now(), uid);
  socket.emit('agents', Q.agents.all().map(publicAgent));
  socket.emit('chat:history', Q.chatRecent.all().map(chatOut));
  const me = Q.agent.get(uid);
  if (me) io.emit('agent', publicAgent(me));

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

  socket.on('disconnect', () => {
    const n = (online.get(uid) || 1) - 1;
    if (n > 0) online.set(uid, n); else online.delete(uid);
    Q.seen.run(now(), uid);
    const row = Q.agent.get(uid);
    if (row) io.emit('agent', publicAgent(row));
  });
});

server.listen(PORT, () => console.log(`SECTOR R-8 서버 가동: 포트 ${PORT}, 데이터 ${DATA_DIR}`));

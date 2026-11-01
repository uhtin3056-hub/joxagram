// JoxaGram server — tashqi paketlarsiz (Node 22+). Hisob, kirish, xabarlar, bloklash, shikoyat, SSE. Baza: SQLite yoki Turso.
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';

const __d = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
const DB_PATH = process.env.DB_PATH || path.join(__d, 'data', 'joxagram.db');
const ADMIN_USER = (process.env.ADMIN_USERNAME || 'joxagram').toLowerCase();
const ADMIN_KEY = process.env.ADMIN_KEY || '';
const TRUST_PROXY = process.env.TRUST_PROXY === '1';
const DAY = 86400000;
const scrypt = promisify(crypto.scrypt);

const TURSO_URL = process.env.TURSO_URL || '', TURSO_TOKEN = process.env.TURSO_TOKEN || '';

// Baza: TURSO_URL berilsa Turso (internetda doimiy, bepul), bo'lmasa shu kompyuterdagi SQLite fayl.
async function sqliteDb(file) {
  const { DatabaseSync } = await import('node:sqlite');
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const d = new DatabaseSync(file); d.exec('PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;');
  return {
    all: async (sql, ...a) => d.prepare(sql).all(...a),
    get: async (sql, ...a) => d.prepare(sql).get(...a),
    run: async (sql, ...a) => { const r = d.prepare(sql).run(...a); return { changes: Number(r.changes), lastInsertRowid: Number(r.lastInsertRowid) }; },
    exec: async (sql) => { d.exec(sql); },
  };
}
function tursoDb(url, token) {
  const endpoint = url.replace(/^libsql:/, 'https:').replace(/\/+$/, '') + '/v2/pipeline';
  const arg = (v) => v == null ? { type: 'null' } : typeof v === 'number' ? (Number.isInteger(v) ? { type: 'integer', value: String(v) } : { type: 'float', value: v }) : { type: 'text', value: String(v) };
  const val = (c) => c.type === 'null' ? null : c.type === 'integer' || c.type === 'float' ? Number(c.value) : c.type === 'blob' ? Buffer.from(c.base64, 'base64') : c.value;
  const exec = async (sql, args) => {
    const r = await fetch(endpoint, { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ requests: [{ type: 'execute', stmt: { sql, args: args.map(arg) } }, { type: 'close' }] }) });
    if (!r.ok) throw new Error('Turso HTTP ' + r.status);
    const res = (await r.json()).results[0];
    if (res.type !== 'ok') throw new Error('Turso: ' + (res.error && res.error.message));
    return res.response.result;
  };
  const rows = (x) => x.rows.map((row) => Object.fromEntries(x.cols.map((c, i) => [c.name, val(row[i])])));
  return {
    all: async (sql, ...a) => rows(await exec(sql, a)),
    get: async (sql, ...a) => rows(await exec(sql, a))[0],
    run: async (sql, ...a) => { const r = await exec(sql, a); return { changes: r.affected_row_count, lastInsertRowid: Number(r.last_insert_rowid) }; },
    exec: async (sql) => { await exec(sql, []); },
  };
}
const db = TURSO_URL ? tursoDb(TURSO_URL, TURSO_TOKEN) : await sqliteDb(DB_PATH);
const q = (sql) => ({ get: (...a) => db.get(sql, ...a), all: (...a) => db.all(sql, ...a), run: (...a) => db.run(sql, ...a) });
for (const st of [
  `CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username TEXT UNIQUE NOT NULL COLLATE NOCASE, name TEXT NOT NULL, bio TEXT DEFAULT '', birth TEXT DEFAULT '', salt TEXT NOT NULL, hash TEXT NOT NULL, stars REAL DEFAULT 0, premium_until INTEGER DEFAULT 0, is_admin INTEGER DEFAULT 0, created INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, created INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY, from_id INTEGER NOT NULL REFERENCES users(id), to_id INTEGER NOT NULL REFERENCES users(id), text TEXT NOT NULL, created INTEGER NOT NULL, edited INTEGER DEFAULT 0)`,
  `CREATE INDEX IF NOT EXISTS ix_m1 ON messages(from_id,to_id,id)`, `CREATE INDEX IF NOT EXISTS ix_m2 ON messages(to_id,from_id,id)`,
  `CREATE TABLE IF NOT EXISTS blocks(blocker_id INTEGER NOT NULL, blocked_id INTEGER NOT NULL, PRIMARY KEY(blocker_id,blocked_id))`,
  `CREATE TABLE IF NOT EXISTS reports(id INTEGER PRIMARY KEY, reporter_id INTEGER NOT NULL, reported_id INTEGER NOT NULL, snapshot TEXT NOT NULL, created INTEGER NOT NULL, status INTEGER DEFAULT 0)`,
]) await db.exec(st);

// ---------- yordamchilar ----------
const send = (res, code, obj) => { const b = JSON.stringify(obj); res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }); res.end(b); };
const fail = (res, code, msg) => send(res, code, { error: msg });
const readBody = (req) => new Promise((ok, no) => {
  let n = 0, over = false; const ch = [];
  req.on('data', (c) => { n += c.length; if (n > 65536) { if (!over) { over = true; no(new Error('big')); } } else ch.push(c); });
  req.on('end', () => { try { ok(ch.length ? JSON.parse(Buffer.concat(ch).toString()) : {}); } catch { no(new Error('json')); } });
  req.on('error', no);
});
const cookies = (req) => Object.fromEntries((req.headers.cookie || '').split(';').map((s) => s.trim().split('=')).filter((a) => a[0]).map(([k, ...v]) => [k, v.join('=')]));
const ip = (req) => (TRUST_PROXY && req.headers['x-forwarded-for'] ? String(req.headers['x-forwarded-for']).split(',')[0].trim() : req.socket.remoteAddress) || '?';
const hits = new Map();
const limited = (key, max, win) => { const now = Date.now(), a = (hits.get(key) || []).filter((t) => now - t < win); a.push(now); hits.set(key, a); return a.length > max; };
setInterval(() => { const now = Date.now(); for (const [k, a] of hits) if (!a.some((t) => now - t < 3600000)) hits.delete(k); }, 600000).unref();

const UNAME = /^[a-z_]{3,32}$/;
const nameErr = (u) => !UNAME.test(u) ? 'Username faqat lotin harflari (a–z) va _ dan iborat, 3–32 belgi bo’lsin.' : '';
const eq = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const hashPw = async (pw, salt) => (await scrypt(pw, salt, 64)).toString('hex');
const validBirth = (s) => { const m = /^(\d{2})\.(\d{2})\.(\d{4})$/.exec(s); if (!m) return false; const d = +m[1], mo = +m[2], y = +m[3], t = new Date(y, mo - 1, d); return t.getFullYear() === y && t.getMonth() === mo - 1 && t.getDate() === d && y >= 1900 && t <= new Date(); };
const pub = (u) => ({ id: u.id, username: u.username, name: u.name, premium: u.premium_until > Date.now() });
const me = (u) => ({ ...pub(u), bio: u.bio, birth: u.birth, stars: u.stars, premium_until: u.premium_until, is_admin: !!u.is_admin });
const blocked = async (a, b) => !!await q('SELECT 1 FROM blocks WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)').get(a, b, b, a);
const userBy = async (name) => await q('SELECT * FROM users WHERE username=?').get(String(name || '').toLowerCase());
const msgOut = (m) => ({ id: m.id, from: m.from_id, to: m.to_id, text: m.text, created: m.created, edited: !!m.edited });

async function auth(req) {
  const t = cookies(req).sid; if (!t) return null;
  const s = await q('SELECT * FROM sessions WHERE token=?').get(t); if (!s) return null;
  if (Date.now() - s.created > 30 * DAY) { await q('DELETE FROM sessions WHERE token=?').run(t); return null; }
  return await q('SELECT * FROM users WHERE id=?').get(s.user_id) || null;
}
async function login(req, res, uid) {
  const token = crypto.randomBytes(32).toString('hex');
  await q('INSERT INTO sessions(token,user_id,created) VALUES(?,?,?)').run(token, uid, Date.now());
  const secure = req.headers['x-forwarded-proto'] === 'https' || process.env.COOKIE_SECURE === '1' ? '; Secure' : '';
  return `sid=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${30 * 86400}${secure}`;
}

// ---------- real vaqt (SSE) ----------
const streams = new Map();
const push = (uid, event, data) => { for (const r of streams.get(uid) || []) r.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); };
setInterval(() => { for (const set of streams.values()) for (const r of set) r.write(': ping\n\n'); }, 25000).unref();

// ---------- API ----------
async function api(req, res, url) {
  const key = `${req.method} ${url.pathname}`, A = ip(req);
  if (req.method !== 'GET' && !String(req.headers['content-type'] || '').startsWith('application/json') && req.method !== 'DELETE') return fail(res, 415, 'JSON kerak.');

  if (key === 'POST /api/register') {
    if (limited('reg' + A, Number(process.env.REG_LIMIT) || 10, 3600000)) return fail(res, 429, 'Juda ko’p urinish. Keyinroq qayta urining.');
    const b = await readBody(req), un = String(b.username || '').trim().toLowerCase(), pw = String(b.password || ''), nm = String(b.name || '').trim().slice(0, 50) || un;
    const e = nameErr(un); if (e) return fail(res, 400, e);
    if (pw.length < 6 || pw.length > 200) return fail(res, 400, 'Parol kamida 6 belgi bo’lsin.');
    let admin = 0;
    if (un === ADMIN_USER) { if (!ADMIN_KEY || !eq(b.adminKey || '', ADMIN_KEY)) return fail(res, 409, 'Bu username band.'); admin = 1; }
    if (await userBy(un)) return fail(res, 409, 'Bu username band.');
    const salt = crypto.randomBytes(16).toString('hex'), hash = await hashPw(pw, salt);
    let r; try { r = await q('INSERT INTO users(username,name,salt,hash,is_admin,created) VALUES(?,?,?,?,?,?)').run(un, nm, salt, hash, admin, Date.now()); }
    catch (e) { if (/UNIQUE/i.test(e.message)) return fail(res, 409, 'Bu username band.'); throw e; }
    const cookie = await login(req, res, Number(r.lastInsertRowid));
    res.setHeader('set-cookie', cookie); return send(res, 201, { user: me(await q('SELECT * FROM users WHERE id=?').get(r.lastInsertRowid)) });
  }
  if (key === 'POST /api/login') {
    if (limited('log' + A, 20, 600000)) return fail(res, 429, 'Juda ko’p urinish. Keyinroq qayta urining.');
    const b = await readBody(req), u = await userBy(b.username);
    const h = await hashPw(String(b.password || ''), u ? u.salt : '0123456789abcdef');
    if (!u || !eq(h, u.hash)) return fail(res, 401, 'Username yoki parol noto’g’ri.');
    res.setHeader('set-cookie', await login(req, res, u.id)); return send(res, 200, { user: me(u) });
  }

  const u = await auth(req); if (!u) return fail(res, 401, 'Kirish kerak.');
  if (key === 'POST /api/logout') { await q('DELETE FROM sessions WHERE token=?').run(cookies(req).sid); res.setHeader('set-cookie', 'sid=; HttpOnly; Path=/; Max-Age=0'); return send(res, 200, { ok: true }); }
  if (key === 'GET /api/me') return send(res, 200, { user: me(u) });
  if (key === 'PATCH /api/me') {
    const b = await readBody(req), name = b.name !== undefined ? String(b.name).trim().slice(0, 50) : u.name, bio = b.bio !== undefined ? String(b.bio).slice(0, 300) : u.bio;
    const birth = b.birth !== undefined ? String(b.birth).trim() : u.birth; let un = u.username;
    if (!name) return fail(res, 400, 'Ism bo’sh bo’lmasin.');
    if (birth && !validBirth(birth)) return fail(res, 400, 'Tug’ilgan sana noto’g’ri. Masalan: 05.05.2002');
    if (b.username !== undefined && String(b.username).toLowerCase() !== u.username) {
      un = String(b.username).trim().toLowerCase(); const e = nameErr(un); if (e) return fail(res, 400, e);
      if (un === ADMIN_USER || await userBy(un)) return fail(res, 409, 'Bu username band.');
    }
    try { await q('UPDATE users SET name=?,bio=?,birth=?,username=? WHERE id=?').run(name, bio, birth, un, u.id); }
    catch (e) { if (/UNIQUE/i.test(e.message)) return fail(res, 409, 'Bu username band.'); throw e; }
    return send(res, 200, { user: me(await q('SELECT * FROM users WHERE id=?').get(u.id)) });
  }
  if (key === 'GET /api/users') {
    const s = String(url.searchParams.get('q') || '').toLowerCase().replace('@', '').trim(); if (s.length < 2) return send(res, 200, { users: [] });
    const like = '%' + s.replace(/[%_\\]/g, '\\$&') + '%';
    const rows = await q(`SELECT * FROM users WHERE id!=? AND (username LIKE ? ESCAPE '\\' OR LOWER(name) LIKE ? ESCAPE '\\')
      AND id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id=?) AND id NOT IN (SELECT blocker_id FROM blocks WHERE blocked_id=?) LIMIT 20`).all(u.id, like, like, u.id, u.id);
    return send(res, 200, { users: rows.map(pub) });
  }
  if (key === 'GET /api/chats') {
    const rows = await q(`SELECT x.id uid,x.username,x.name,x.premium_until,m.id mid,m.text,m.created,m.from_id FROM messages m
      JOIN users x ON x.id=CASE WHEN m.from_id=? THEN m.to_id ELSE m.from_id END
      WHERE m.id IN (SELECT MAX(id) FROM messages WHERE from_id=? OR to_id=? GROUP BY CASE WHEN from_id=? THEN to_id ELSE from_id END)
      AND x.id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id=?) ORDER BY m.id DESC`).all(u.id, u.id, u.id, u.id, u.id);
    return send(res, 200, { chats: rows.map((r) => ({ user: pub({ id: r.uid, username: r.username, name: r.name, premium_until: r.premium_until }), last: { id: r.mid, text: r.text, created: r.created, mine: r.from_id === u.id } })) });
  }
  if (key === 'GET /api/messages') {
    const p = await userBy(url.searchParams.get('with')); if (!p) return fail(res, 404, 'Foydalanuvchi topilmadi.');
    const before = Number(url.searchParams.get('before')) || 9e15;
    const rows = await q('SELECT * FROM messages WHERE ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?)) AND id<? ORDER BY id DESC LIMIT 50').all(u.id, p.id, p.id, u.id, before);
    return send(res, 200, { user: pub(p), messages: rows.reverse().map(msgOut) });
  }
  if (key === 'POST /api/messages') {
    if (limited('msg' + u.id, 60, 60000)) return fail(res, 429, 'Juda tez yozyapsiz.');
    const b = await readBody(req), p = await userBy(b.to), text = String(b.text || '').trim();
    if (!p || p.id === u.id) return fail(res, 404, 'Foydalanuvchi topilmadi.');
    if (!text || text.length > 4000) return fail(res, 400, 'Xabar 1–4000 belgi bo’lsin.');
    if (await blocked(u.id, p.id)) return fail(res, 403, 'Bu foydalanuvchiga yozib bo’lmaydi.');
    const r = await q('INSERT INTO messages(from_id,to_id,text,created) VALUES(?,?,?,?)').run(u.id, p.id, text, Date.now());
    const m = msgOut(await q('SELECT * FROM messages WHERE id=?').get(r.lastInsertRowid));
    push(p.id, 'message', { message: m, from: pub(u) }); push(u.id, 'message', { message: m, from: pub(u) });
    return send(res, 201, { message: m });
  }
  let m2 = /^(PATCH|DELETE) \/api\/messages\/(\d+)$/.exec(key);
  if (m2) {
    const m = await q('SELECT * FROM messages WHERE id=?').get(Number(m2[2]));
    if (!m || m.from_id !== u.id) return fail(res, 404, 'Xabar topilmadi.');
    if (m2[1] === 'DELETE') { await q('DELETE FROM messages WHERE id=?').run(m.id); const ev = { id: m.id }; push(m.to_id, 'delete', ev); push(u.id, 'delete', ev); return send(res, 200, { ok: true }); }
    const text = String((await readBody(req)).text || '').trim(); if (!text || text.length > 4000) return fail(res, 400, 'Xabar 1–4000 belgi bo’lsin.');
    await q('UPDATE messages SET text=?,edited=1 WHERE id=?').run(text, m.id);
    const out = msgOut(await q('SELECT * FROM messages WHERE id=?').get(m.id)); push(m.to_id, 'edit', { message: out }); push(u.id, 'edit', { message: out });
    return send(res, 200, { message: out });
  }
  if (key === 'GET /api/blocks') return send(res, 200, { blocks: (await q('SELECT x.* FROM blocks b JOIN users x ON x.id=b.blocked_id WHERE b.blocker_id=?').all(u.id)).map(pub) });
  if (key === 'POST /api/blocks') {
    const p = await userBy((await readBody(req)).username); if (!p || p.id === u.id) return fail(res, 404, 'Foydalanuvchi topilmadi.');
    await q('INSERT OR IGNORE INTO blocks(blocker_id,blocked_id) VALUES(?,?)').run(u.id, p.id); return send(res, 200, { ok: true });
  }
  m2 = /^DELETE \/api\/blocks\/([a-z_]+)$/.exec(key);
  if (m2) { const p = await userBy(m2[1]); if (p) await q('DELETE FROM blocks WHERE blocker_id=? AND blocked_id=?').run(u.id, p.id); return send(res, 200, { ok: true }); }
  if (key === 'POST /api/reports') {
    if (limited('rep' + u.id, 10, 3600000)) return fail(res, 429, 'Juda ko’p shikoyat.');
    const p = await userBy((await readBody(req)).username); if (!p || p.id === u.id) return fail(res, 404, 'Foydalanuvchi topilmadi.');
    const rows = (await q('SELECT * FROM messages WHERE (from_id=? AND to_id=?) OR (from_id=? AND to_id=?) ORDER BY id DESC LIMIT 30').all(u.id, p.id, p.id, u.id)).reverse()
      .map((m) => ({ from: m.from_id === u.id ? u.username : p.username, text: m.text, created: m.created }));
    await q('INSERT INTO reports(reporter_id,reported_id,snapshot,created) VALUES(?,?,?,?)').run(u.id, p.id, JSON.stringify(rows), Date.now());
    return send(res, 201, { ok: true });
  }
  if (key === 'GET /api/events') {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store', connection: 'keep-alive', 'x-accel-buffering': 'no' });
    res.write('retry: 3000\n\n'); (streams.get(u.id) || streams.set(u.id, new Set()).get(u.id)).add(res);
    req.on('close', () => { const s = streams.get(u.id); if (s) { s.delete(res); if (!s.size) streams.delete(u.id); } }); return;
  }

  // ---------- admin ----------
  if (url.pathname.startsWith('/api/admin/')) {
    if (!u.is_admin) return fail(res, 403, 'Ruxsat yo’q.');
    if (key === 'GET /api/admin/reports') {
      const rows = await q('SELECT r.*,a.username rn,b.username dn FROM reports r JOIN users a ON a.id=r.reporter_id JOIN users b ON b.id=r.reported_id ORDER BY r.id DESC LIMIT 100').all();
      return send(res, 200, { reports: rows.map((r) => ({ id: r.id, reporter: r.rn, reported: r.dn, created: r.created, status: r.status, messages: JSON.parse(r.snapshot) })) });
    }
    if (key === 'POST /api/admin/stars') {
      const b = await readBody(req), p = await userBy(b.username), d = Number(b.delta);
      if (!p || !Number.isFinite(d) || Math.abs(d) > 1e7) return fail(res, 400, 'Noto’g’ri so’rov.');
      await q('UPDATE users SET stars=MAX(0,ROUND(stars+?,2)) WHERE id=?').run(d, p.id); return send(res, 200, { stars: (await q('SELECT stars FROM users WHERE id=?').get(p.id)).stars });
    }
    if (key === 'POST /api/admin/premium') {
      const b = await readBody(req), p = await userBy(b.username), days = Number(b.days);
      if (!p || !Number.isFinite(days) || days < 0 || days > 3660) return fail(res, 400, 'Noto’g’ri so’rov.');
      const until = days === 0 ? 0 : Math.max(Date.now(), p.premium_until) + days * DAY;
      await q('UPDATE users SET premium_until=? WHERE id=?').run(until, p.id); return send(res, 200, { premium_until: until });
    }
  }
  return fail(res, 404, 'Topilmadi.');
}

// ---------- statik fayllar ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.webp': 'image/webp', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.json': 'application/json' };
function serveStatic(req, res, url) {
  const pub = path.join(__d, 'public'); const flat = !fs.existsSync(pub); const root = flat ? __d : pub; let dec; try { dec = decodeURIComponent(url.pathname); } catch { res.writeHead(400); return res.end(); }
  let p = path.normalize(dec).replace(/^(\.\.[/\\])+/, '');
  if (p === '/' || p === path.sep) p = '/index.html';
  if (flat && p !== '/index.html') { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end('Topilmadi'); }
  const f = path.join(root, p); if (!f.startsWith(root)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => { if (e) { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end('Topilmadi'); }
    res.writeHead(200, { 'content-type': MIME[path.extname(f)] || 'application/octet-stream', 'cache-control': 'no-cache' }); res.end(d); });
}

const server = http.createServer(async (req, res) => {
  res.setHeader('x-content-type-options', 'nosniff'); res.setHeader('referrer-policy', 'same-origin'); res.setHeader('x-frame-options', 'DENY');
  try {
    const url = new URL(req.url, 'http://x');
    if (url.pathname.startsWith('/api/')) return await api(req, res, url);
    if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
    serveStatic(req, res, url);
  } catch (e) {
    if (e.message === 'big') res.setHeader('connection', 'close');
    if (!res.headersSent) fail(res, e.message === 'big' ? 413 : e.message === 'json' ? 400 : 500, e.message === 'big' ? 'Juda katta so’rov.' : e.message === 'json' ? 'JSON noto’g’ri.' : 'Server xatosi.');
    if (!['big', 'json'].includes(e.message)) console.error(e);
  }
});
server.listen(PORT, () => console.log(`JoxaGram server: http://localhost:${PORT}`));

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
  `CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY, kind TEXT NOT NULL, username TEXT UNIQUE NOT NULL COLLATE NOCASE, name TEXT NOT NULL, descr TEXT DEFAULT '', private INTEGER DEFAULT 0, owner_id INTEGER NOT NULL, avatar TEXT DEFAULT '', bg TEXT DEFAULT '', upd INTEGER DEFAULT 0, created INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS members(channel_id INTEGER NOT NULL, user_id INTEGER NOT NULL, joined INTEGER NOT NULL, PRIMARY KEY(channel_id,user_id))`,
  `CREATE INDEX IF NOT EXISTS ix_mem_u ON members(user_id)`,
  `CREATE TABLE IF NOT EXISTS cmsgs(id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL, from_id INTEGER NOT NULL, text TEXT NOT NULL, created INTEGER NOT NULL, edited INTEGER DEFAULT 0)`,
  `CREATE INDEX IF NOT EXISTS ix_cm ON cmsgs(channel_id,id)`,
  `CREATE TABLE IF NOT EXISTS creacts(msg_id INTEGER NOT NULL, user_id INTEGER NOT NULL, emoji TEXT NOT NULL, PRIMARY KEY(msg_id,user_id))`,
  `CREATE TABLE IF NOT EXISTS votes(user_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, n INTEGER NOT NULL, PRIMARY KEY(user_id,channel_id))`,
  `CREATE TABLE IF NOT EXISTS media(id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL, kind TEXT NOT NULL, mime TEXT NOT NULL, data TEXT NOT NULL, size INTEGER NOT NULL, dur INTEGER DEFAULT 0, created INTEGER NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS ix_media_o ON media(owner_id)`,
  `CREATE TABLE IF NOT EXISTS avatars(user_id INTEGER PRIMARY KEY, data TEXT NOT NULL, upd INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS stars_ledger(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, delta REAL NOT NULL, balance REAL NOT NULL, reason TEXT NOT NULL, ref TEXT DEFAULT '', created INTEGER NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS ledger_u ON stars_ledger(user_id,id)`,
  `CREATE TABLE IF NOT EXISTS gifts_owned(id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL, gift INTEGER NOT NULL, price REAL NOT NULL, from_id INTEGER NOT NULL, hidden INTEGER DEFAULT 0, sold INTEGER DEFAULT 0, created INTEGER NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS gifts_o ON gifts_owned(owner_id,sold)`,
  `CREATE TABLE IF NOT EXISTS chat_clear(user_id INTEGER NOT NULL, peer_id INTEGER NOT NULL, upto INTEGER NOT NULL, PRIMARY KEY(user_id,peer_id))`,
  `CREATE TABLE IF NOT EXISTS saved(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, text TEXT NOT NULL DEFAULT '', kind TEXT DEFAULT '', media_id INTEGER DEFAULT 0, vtext TEXT DEFAULT '', created INTEGER NOT NULL)`,
  `CREATE INDEX IF NOT EXISTS saved_u ON saved(user_id,id)`,
  `CREATE TABLE IF NOT EXISTS chatbg(user_id INTEGER PRIMARY KEY, data TEXT NOT NULL, upd INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS badges(user_id INTEGER PRIMARY KEY, data TEXT NOT NULL, upd INTEGER NOT NULL)`,
  `CREATE TABLE IF NOT EXISTS gprice(gift INTEGER NOT NULL, slot INTEGER NOT NULL, price REAL NOT NULL, PRIMARY KEY(gift,slot))`,
]) await db.exec(st);
for (const st of [
  `ALTER TABLE messages ADD COLUMN kind TEXT DEFAULT ''`, `ALTER TABLE messages ADD COLUMN media_id INTEGER DEFAULT 0`, `ALTER TABLE messages ADD COLUMN vtext TEXT DEFAULT ''`,
  `ALTER TABLE cmsgs ADD COLUMN kind TEXT DEFAULT ''`, `ALTER TABLE cmsgs ADD COLUMN media_id INTEGER DEFAULT 0`, `ALTER TABLE cmsgs ADD COLUMN vtext TEXT DEFAULT ''`,
  `ALTER TABLE users ADD COLUMN aupd INTEGER DEFAULT 0`,
  `ALTER TABLE users ADD COLUMN gcol TEXT DEFAULT ''`, `ALTER TABLE users ADD COLUMN gl TEXT DEFAULT '[]'`, `ALTER TABLE users ADD COLUMN gsel INTEGER DEFAULT 0`,
  `ALTER TABLE messages ADD COLUMN gift INTEGER`, `ALTER TABLE messages ADD COLUMN rd INTEGER DEFAULT 0`,
  `ALTER TABLE users ADD COLUMN badge TEXT DEFAULT ''`, `ALTER TABLE users ADD COLUMN bgupd INTEGER DEFAULT 0`,
]) { try { await db.exec(st); } catch (e) { if (!/duplicate column/i.test(e.message)) throw e; } }

// ---------- yordamchilar ----------
const send = (res, code, obj) => { const b = JSON.stringify(obj); res.writeHead(code, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' }); res.end(b); };
const fail = (res, code, msg, extra) => send(res, code, { error: msg, ...extra });
const readBody = (req, max = 65536) => new Promise((ok, no) => {
  let n = 0, over = false; const ch = [];
  req.on('data', (c) => { n += c.length; if (n > max) { if (!over) { over = true; no(new Error('big')); } } else ch.push(c); });
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
const badgeOf = (u) => { if (!u.badge || !(u.premium_until > Date.now())) return ''; return u.badge.startsWith('i:') ? 'u:/api/users/' + u.id + '/badge?v=' + u.badge.slice(2) : u.badge; };
const pub = (u) => ({ id: u.id, bd: badgeOf(u), username: u.username, name: u.name, premium: u.premium_until > Date.now(), adm: !!u.is_admin, gc: u.gcol || '', gl: (() => { try { return JSON.parse(u.gl || '[]'); } catch { return []; } })(), ava: u.aupd ? `/api/users/${u.id}/avatar?v=${u.aupd}` : '' });
const me = (u) => ({ ...pub(u), bio: u.bio, birth: u.birth, stars: u.stars, premium_until: u.premium_until, is_admin: !!u.is_admin, badge: u.badge || '', bg: u.bgupd ? '/api/me/chatbg?v=' + u.bgupd : '' });
const blocked = async (a, b) => !!await q('SELECT 1 FROM blocks WHERE (blocker_id=? AND blocked_id=?) OR (blocker_id=? AND blocked_id=?)').get(a, b, b, a);
const userBy = async (name) => await q('SELECT * FROM users WHERE username=?').get(String(name || '').toLowerCase());
const taken = async (un) => !!(await userBy(un) || await q('SELECT 1 FROM channels WHERE username=?').get(un));

// ---------- sovg'alar katalogi va narxlar (serverda) ----------
const CAT = [
  { n: 'Yashil omad', p: 4300, c: '#5db06a' }, { n: 'Jigarrang omad', p: 350, c: '#865546' }, { n: 'Bunny Muffin', p: 12000, c: '#cc6b83' }, { n: 'Fen', p: 1000, c: '#181818' },
  { n: 'Gul dastasi', p: 50, c: '#101010' }, { n: 'Kepka', p: 120, c: '#908d87' }, { n: 'Qurbaqa', p: 1000, c: '#354056' }, { n: 'Olovrang uzuk', p: 700, c: '#ba8b31' },
  { n: 'Yashil uzuk', p: 700, c: '#519d55' }, { n: 'Qora tuynuk uzugi', p: 17000, c: '#161817' }, { n: 'Kristall', p: 28000, c: '#8e4890' }, { n: 'Axlat', p: 120, c: '#161616' },
  { n: 'Tort', p: 50, c: '#181818' }, { n: 'Yurak', p: 15, c: '#ffffff' }, { n: 'Brilliant uzuk', p: 100, c: '#181818' },
  { n: 'Qora ilon', p: 5000, c: '#8f854a', nft: 1, tag: '' },
  { n: 'Qizil 19', p: 10000, c: '#90864a', nft: 1, tag: '' },
  { n: 'Qora to‘p', p: 10000, c: '#90854a', nft: 1, tag: '' },
  { n: 'Katta ko‘z', p: 7777, c: '#90854a', nft: 1, tag: '' },
  { n: 'Qor paypoq', p: 7777, c: '#8f854a', nft: 1, tag: '' },
  { n: 'Qora tort', p: 6666, c: '#8f854a', nft: 1, tag: '' },
  { n: 'Tuxum agent 1', p: 4242, c: '#8f854a', nft: 1, tag: '' },
  { n: 'Tuxum agent 2', p: 4242, c: '#90854b', nft: 1, tag: '' },
  { n: 'Tuxum agent 3', p: 4242, c: '#8f8549', nft: 1, tag: '' },
  { n: 'Oynali yurak', p: 6666, c: '#8f854a', nft: 1, tag: '' },
  { n: 'Uzukli quti', p: 44444, c: '#90854a', nft: 1, tag: '' },
  { n: 'Kumush yurak', p: 6666, c: '#8f8449', nft: 1, tag: '' },
  { n: 'Qizil bantik', p: 7777, c: '#8d8349', nft: 1, tag: '' },
  { n: 'Kumush bant', p: 8888, c: '#8e844a', nft: 1, tag: '' },
  { n: 'Iblis lolipopi', p: 8888, c: '#8d8349', nft: 1, tag: '' },
  { n: 'Oltin shar', p: 12000, c: '#9d8c72', nft: 1, tag: '#4 346' },
  { n: 'Oltin nishon', p: 15000, c: '#9e8c73', nft: 1, tag: '#96' },
  { n: 'Oltin mushuk', p: 9000, c: '#9d8d73', nft: 1, tag: '#13 681' },
  { n: 'Oltin soat', p: 18000, c: '#9d8c72', nft: 1, tag: '#16 916' },
  { n: 'Oltin atirgul', p: 25000, c: '#9e8c73', nft: 1, tag: '#1 684' },
  { n: 'Sehrli chiroq', p: 11000, c: '#9d8c72', nft: 1, tag: '#5 115' },
  { n: 'Oltin taqvim', p: 3000, c: '#9d8d72', nft: 1, tag: '1/323.8K' },
  { n: 'Suyak guli', p: 6000, c: '#9e8c73', nft: 1, tag: '1/20.1K' },
  { n: 'Oltin qo‘lqop', p: 4000, c: '#9d8d73', nft: 1, tag: '1/28.2K' },
  { n: 'Yashil dubulg‘a', p: 7307, c: '#e4a4ff', nft: 1, tag: '#12 751' },
  { n: 'To‘q sariq dubulg‘a', p: 11000, c: '#b279fd', nft: 1, tag: '#4 357' },
  { n: 'Oltin mushuk dubulg‘a', p: 11481, c: '#b77dfe', nft: 1, tag: '#2 933' },
  { n: 'Qizil poyga dubulg‘asi', p: 12525, c: '#cd92ff', nft: 1, tag: '#11 252' },
  { n: 'Kumush mushuk dubulg‘a', p: 13333, c: '#9e68e7', nft: 1, tag: '#3 691' },
  { n: 'Ayiqcha dubulg‘a', p: 14613, c: '#9f69e9', nft: 1, tag: '#6 680' },
  { n: 'Oq mushuk dubulg‘a', p: 14980, c: '#e1a2ff', nft: 1, tag: '#12 354' },
  { n: 'Qulupnay dubulg‘a', p: 16701, c: '#ae77f9', nft: 1, tag: '#3 210' },
  { n: 'Pushti dubulg‘a', p: 22222, c: '#b27afd', nft: 1, tag: '#12 626' },
];
const PRICE_STEP = Number(process.env.PRICE_STEP_MS) || 3600000;
const r2 = (x) => Math.round(x * 100) / 100;
let lastSlot = 0, ticking = null;
async function tickPrices() {
  const slot = Math.floor(Date.now() / PRICE_STEP); if (slot === lastSlot) return;
  if (ticking) return ticking;
  ticking = (async () => {
    for (let i = 0; i < CAT.length; i++) {
      const b = CAT[i].p, last = await q('SELECT slot,price FROM gprice WHERE gift=? ORDER BY slot DESC LIMIT 1').get(i);
      let from, p;
      if (!last) { await q('INSERT OR IGNORE INTO gprice(gift,slot,price) VALUES(?,?,?)').run(i, slot, b); continue; }
      from = last.slot + 1; p = last.price;
      if (slot - from > 24 * 30) from = slot - 24 * 30;
      for (let k = from; k <= slot; k++) {
        const up = crypto.randomInt(0, 100) < 30, d = crypto.randomInt(0, 1501) / 10000; // 0..15%
        p = Math.min(b * 2.5, Math.max(b * 0.4, p * (up ? 1 + d : 1 - d))); p = r2(p);
        await q('INSERT OR IGNORE INTO gprice(gift,slot,price) VALUES(?,?,?)').run(i, k, p);
      }
    }
    lastSlot = slot;
  })().finally(() => { ticking = null; });
  return ticking;
}
async function curPrices() {
  const rows = await q('SELECT g.gift,g.price FROM gprice g JOIN (SELECT gift,MAX(slot) s FROM gprice GROUP BY gift) m ON m.gift=g.gift AND m.s=g.slot').all(), o = {};
  for (const r of rows) o[r.gift] = r.price; for (let i = 0; i < CAT.length; i++) if (o[i] == null) o[i] = CAT[i].p; return o;
}
async function giftList() {
  const out = [];
  for (let i = 0; i < CAT.length; i++) {
    const h = (await q('SELECT price FROM gprice WHERE gift=? ORDER BY slot DESC LIMIT 25').all(i)).map((x) => x.price).reverse(), n = h.length, cur = n ? h[n - 1] : CAT[i].p;
    out.push({ id: i, name: CAT[i].n, base: CAT[i].p, color: CAT[i].c, nft: !!CAT[i].nft, tag: CAT[i].tag || '', price: cur, change: n > 1 ? r2((h[n - 1] / h[n - 2] - 1) * 100) : 0, hist: h });
  }
  return out;
}
async function ledger(uid, delta, reason, ref) {
  const b = await q('SELECT stars FROM users WHERE id=?').get(uid);
  await q('INSERT INTO stars_ledger(user_id,delta,balance,reason,ref,created) VALUES(?,?,?,?,?,?)').run(uid, delta, b ? b.stars : 0, reason, ref || '', Date.now());
}
async function refreshGifts(uid) {
  const rows = await q('SELECT id,gift FROM gifts_owned WHERE owner_id=? AND sold=0 AND hidden=0 ORDER BY id').all(uid), us = await q('SELECT gsel FROM users WHERE id=?').get(uid);
  const sel = rows.find((r) => r.id === us.gsel) || rows[rows.length - 1];
  await q('UPDATE users SET gcol=?,gl=?,gsel=? WHERE id=?').run(sel ? CAT[sel.gift].c : '', JSON.stringify(rows.slice(-30).map((r) => r.gift)), sel ? sel.id : 0, uid);
  bcast(uid);
}
const msgOut = (m) => ({ id: m.id, from: m.from_id, to: m.to_id, text: m.text, created: m.created, edited: !!m.edited, kind: m.kind || '', media: m.media_id || 0, vtext: m.vtext || '', gift: m.gift == null ? null : m.gift, read: !!m.rd });
const kindLabel = (m) => m.text || (m.kind === 'img' ? '[rasm]' : m.kind === 'voice' ? '[ovozli xabar]' : '');

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
// profil o’zgarsa (rasm, ism, sovg’a rangi...) ulangan hamma odamga xabar beramiz
const adminMsg = async (u, p, text) => { if (!p || p.id === u.id) return; const r = await q('INSERT INTO messages(from_id,to_id,text,created) VALUES(?,?,?,?)').run(u.id, p.id, text, Date.now()); const m = msgOut(await q('SELECT * FROM messages WHERE id=?').get(r.lastInsertRowid)); push(p.id, 'message', { message: m, from: pub(u) }); push(u.id, 'message', { message: m, from: pub(p) }); };
const bcast = async (uid) => { try { const x = await q('SELECT * FROM users WHERE id=?').get(uid); if (x) for (const id of streams.keys()) push(id, 'profile', { user: pub(x) }); } catch {} };
setInterval(() => { for (const set of streams.values()) for (const r of set) r.write(': ping\n\n'); }, 25000).unref();

// ---------- kanal va guruhlar: yordamchilar ----------
const REACTS = ['👍', '❤️', '🔥'];
const VOTES_EACH = 5;
const lvOf = (v) => v < 1 ? 0 : v < 4 ? 1 : v < 10 ? 2 : 3 + Math.floor((v - 10) / 5);
const CHSEL = `SELECT c.id,c.kind,c.username,c.name,c.descr,c.private,c.owner_id,c.upd,c.created,(LENGTH(c.avatar)>0) hasava,(LENGTH(c.bg)>0) hasbg,(SELECT COUNT(*) FROM members WHERE channel_id=c.id) cnt,(SELECT COALESCE(SUM(n),0) FROM votes WHERE channel_id=c.id) v FROM channels c`;
const chOut = (r, uid) => ({ id: r.id, kind: r.kind, username: r.username, name: r.name, descr: r.descr, private: !!r.private, mine: r.owner_id === uid, count: Number(r.cnt), v: Number(r.v), created: r.created, level: r.kind === 'k' ? lvOf(Number(r.v)) : 0,
  ava: r.hasava ? `/api/channels/${r.id}/avatar?v=${r.upd}` : '', bg: r.hasbg && r.kind === 'k' ? `/api/channels/${r.id}/bg?v=${r.upd}` : '' });
const chById = async (id) => await q(CHSEL + ' WHERE c.id=?').get(id);
const isMember = async (cid, uid) => !!await q('SELECT 1 FROM members WHERE channel_id=? AND user_id=?').get(cid, uid);
const memberIds = async (cid) => (await q('SELECT user_id FROM members WHERE channel_id=?').all(cid)).map((r) => r.user_id);
async function pushCh(cid, event, data) { for (const id of await memberIds(cid)) push(id, event, data); }
async function pushChannel(cid, uids) { const r = await chById(cid); if (!r) return; for (const id of uids || await memberIds(cid)) push(id, 'channel', { channel: chOut(r, id) }); }
const votesLeft = async (uid) => VOTES_EACH - Number((await q('SELECT COALESCE(SUM(n),0) s FROM votes WHERE user_id=?').get(uid)).s);
const IMG = /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=]+)$/;
const imgOk = (s, maxBytes) => typeof s === 'string' && IMG.test(s) && s.length * 0.75 <= maxBytes;
const cmOut = (m) => ({ id: m.id, channel: m.channel_id, from: m.from_id, name: m.fname, text: m.text, created: m.created, edited: !!m.edited, kind: m.kind || '', media: m.media_id || 0, vtext: m.vtext || '' });
const cmById = async (id) => await q('SELECT x.*,u.name fname FROM cmsgs x JOIN users u ON u.id=x.from_id WHERE x.id=?').get(id);
async function reactCounts(ids) {
  const out = {}; if (!ids.length) return out;
  for (const r of await q(`SELECT msg_id,emoji,COUNT(*) n FROM creacts WHERE msg_id IN (${ids.map(Number).join(',')}) GROUP BY msg_id,emoji`).all()) (out[r.msg_id] ||= {})[r.emoji] = Number(r.n);
  return out;
}

// ---------- rasm, ovoz, profil rasmi ----------
const MEDIA_IMG = /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/=]+)$/;
const MEDIA_AUD = /^data:(audio\/(?:webm|ogg|mp4|mpeg|wav|x-m4a|aac))(?:;codecs=[\w.,-]+)?;base64,([A-Za-z0-9+/=]+)$/;
const USER_QUOTA = 40 * 1024 * 1024;
async function takeMedia(b, uid) {
  if (!b.media) return { id: 0, kind: '', vtext: '' };
  const r = await q('SELECT id,owner_id,kind FROM media WHERE id=?').get(Number(b.media));
  if (!r || r.owner_id !== uid) return { err: 'Fayl topilmadi.' };
  return { id: r.id, kind: r.kind, vtext: r.kind === 'voice' ? String(b.vtext || '').slice(0, 2000) : '' };
}
async function canSeeMedia(id, uid) {
  return !!await q(`SELECT 1 WHERE EXISTS(SELECT 1 FROM media WHERE id=? AND owner_id=?)
    OR EXISTS(SELECT 1 FROM messages WHERE media_id=? AND (from_id=? OR to_id=?))
    OR EXISTS(SELECT 1 FROM cmsgs x JOIN members m ON m.channel_id=x.channel_id AND m.user_id=? WHERE x.media_id=?)`).get(id, uid, id, uid, uid, uid, id);
}
async function sweepMedia() { try { await q('DELETE FROM media WHERE created<? AND id NOT IN (SELECT media_id FROM messages WHERE media_id>0) AND id NOT IN (SELECT media_id FROM cmsgs WHERE media_id>0) AND id NOT IN (SELECT media_id FROM saved WHERE media_id>0)').run(Date.now() - DAY); } catch (e) { console.error(e); } }
sweepMedia(); setInterval(sweepMedia, 3600000).unref();
tickPrices().catch((e) => console.error('tick', e.message)); setInterval(() => tickPrices().catch(() => {}), Math.min(PRICE_STEP, 60000)).unref();
function sendBinary(req, res, mime, buf) {
  const h = { 'content-type': mime, 'accept-ranges': 'bytes', 'cache-control': 'private, max-age=31536000, immutable' }, rg = /^bytes=(\d*)-(\d*)$/.exec(String(req.headers.range || ''));
  if (rg && (rg[1] || rg[2])) {
    let a = rg[1] ? Number(rg[1]) : buf.length - Number(rg[2]), z = rg[1] && rg[2] ? Number(rg[2]) : buf.length - 1; z = Math.min(z, buf.length - 1);
    if (a > z || a >= buf.length) { res.writeHead(416, { 'content-range': `bytes */${buf.length}` }); return res.end(); }
    res.writeHead(206, { ...h, 'content-range': `bytes ${a}-${z}/${buf.length}`, 'content-length': z - a + 1 }); return res.end(buf.subarray(a, z + 1));
  }
  res.writeHead(200, { ...h, 'content-length': buf.length }); res.end(buf);
}

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
    if (await taken(un)) return fail(res, 409, 'Bu username band.');
    const salt = crypto.randomBytes(16).toString('hex'), hash = await hashPw(pw, salt);
    let r; try { r = await q('INSERT INTO users(username,name,salt,hash,is_admin,created) VALUES(?,?,?,?,?,?)').run(un, nm, salt, hash, admin, Date.now()); }
    catch (e) { if (/UNIQUE/i.test(e.message)) return fail(res, 409, 'Bu username band.'); throw e; }
    const cookie = await login(req, res, Number(r.lastInsertRowid));
    res.setHeader('set-cookie', cookie); return send(res, 201, { user: me(await q('SELECT * FROM users WHERE id=?').get(r.lastInsertRowid)) });
  }
  if (key === 'POST /api/login') {
    const b = await readBody(req), lk = 'log' + A + ':' + String(b.username || '').toLowerCase().replace(/^@/, ''), now0 = Date.now();
    if ((hits.get(lk) || []).filter((t) => now0 - t < 600000).length >= 10) return fail(res, 429, 'Juda ko’p xato urinish. 10 daqiqadan keyin qayta urining.');
    const u = await userBy(b.username);
    const h = await hashPw(String(b.password || ''), u ? u.salt : '0123456789abcdef');
    if (!u || !eq(h, u.hash)) { limited(lk, 10, 600000); return fail(res, 401, 'Username yoki parol noto’g’ri.'); }
    res.setHeader('set-cookie', await login(req, res, u.id)); return send(res, 200, { user: me(u) });
  }

  if (key === 'POST /api/switch') {
    if (limited('sw' + A, 30, 600000)) return fail(res, 429, 'Juda ko’p urinish.');
    const b = await readBody(req), t = String(b.token || '');
    const ss = /^[0-9a-f]{64}$/.test(t) ? await q('SELECT * FROM sessions WHERE token=?').get(t) : null;
    if (!ss || Date.now() - ss.created > 30 * DAY) return fail(res, 401, 'Akkaunt muddati tugagan. Qayta kiring.');
    const uu = await q('SELECT * FROM users WHERE id=?').get(ss.user_id); if (!uu) return fail(res, 401, 'Akkaunt topilmadi.');
    const secure = req.headers['x-forwarded-proto'] === 'https' || process.env.COOKIE_SECURE === '1' ? '; Secure' : '';
    res.setHeader('set-cookie', `sid=${t}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${30 * 86400}${secure}`); return send(res, 200, { user: me(uu) });
  }

  const u = await auth(req); if (!u) return fail(res, 401, 'Kirish kerak.');
  if (key === 'POST /api/accounts/token') return send(res, 200, { token: cookies(req).sid });
  if (key === 'POST /api/logout') { await q('DELETE FROM sessions WHERE token=?').run(cookies(req).sid); res.setHeader('set-cookie', 'sid=; HttpOnly; Path=/; Max-Age=0'); return send(res, 200, { ok: true }); }
  if (key === 'GET /api/me') return send(res, 200, { user: me(u) });
  if (key === 'PATCH /api/me') {
    const b = await readBody(req), name = b.name !== undefined ? String(b.name).trim().slice(0, 50) : u.name, bio = b.bio !== undefined ? String(b.bio).slice(0, 300) : u.bio;
    const birth = b.birth !== undefined ? String(b.birth).trim() : u.birth; let un = u.username;
    if (!name) return fail(res, 400, 'Ism bo’sh bo’lmasin.');
    if (birth && !validBirth(birth)) return fail(res, 400, 'Tug’ilgan sana noto’g’ri. Masalan: 05.05.2002');
    if (b.username !== undefined && String(b.username).toLowerCase() !== u.username) {
      un = String(b.username).trim().toLowerCase(); const e = nameErr(un); if (e) return fail(res, 400, e);
      if (un === ADMIN_USER || await taken(un)) return fail(res, 409, 'Bu username band.');
    }
    try { await q('UPDATE users SET name=?,bio=?,birth=?,username=? WHERE id=?').run(name, bio, birth, un, u.id); }
    catch (e) { if (/UNIQUE/i.test(e.message)) return fail(res, 409, 'Bu username band.'); throw e; }
    bcast(u.id);
    return send(res, 200, { user: me(await q('SELECT * FROM users WHERE id=?').get(u.id)) });
  }
  if (key === 'GET /api/users') {
    const s = String(url.searchParams.get('q') || '').toLowerCase().replace('@', '').trim(); if (s.length < 2) return send(res, 200, { users: [] });
    const like = '%' + s.replace(/[%_\\]/g, '\\$&') + '%';
    const rows = await q(`SELECT * FROM users WHERE id!=? AND (username LIKE ? ESCAPE '\\' OR LOWER(name) LIKE ? ESCAPE '\\')
      AND id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id=?) AND id NOT IN (SELECT blocker_id FROM blocks WHERE blocked_id=?) LIMIT 20`).all(u.id, like, like, u.id, u.id);
    const chs = await q(`SELECT id,username,name,descr,upd,(LENGTH(avatar)>0) hasava,(SELECT COUNT(*) FROM members WHERE channel_id=c.id) cnt FROM channels c WHERE kind='k' AND private=0
      AND (username LIKE ? ESCAPE '\\' OR LOWER(name) LIKE ? ESCAPE '\\') AND id NOT IN (SELECT channel_id FROM members WHERE user_id=?) LIMIT 10`).all(like, like, u.id);
    return send(res, 200, { users: rows.map(pub), channels: chs.map((c) => ({ id: c.id, username: c.username, name: c.name, descr: c.descr, count: c.cnt, ava: c.hasava ? `/api/channels/${c.id}/avatar?v=${c.upd}` : '' })) });
  }
  if (key === 'GET /api/chats') {
    const rows = await q(`SELECT x.id uid,x.username,x.name,x.premium_until,x.aupd,x.is_admin,x.gcol,x.gl,x.badge,m.id mid,m.text,m.kind,m.created,m.from_id FROM messages m
      JOIN users x ON x.id=CASE WHEN m.from_id=? THEN m.to_id ELSE m.from_id END
      WHERE m.id IN (SELECT MAX(id) FROM messages WHERE from_id=? OR to_id=? GROUP BY CASE WHEN from_id=? THEN to_id ELSE from_id END)
      AND m.id>COALESCE((SELECT upto FROM chat_clear WHERE user_id=? AND peer_id=x.id),0)
      AND x.id NOT IN (SELECT blocked_id FROM blocks WHERE blocker_id=?) ORDER BY m.id DESC`).all(u.id, u.id, u.id, u.id, u.id, u.id);
    return send(res, 200, { chats: rows.map((r) => ({ user: pub({ id: r.uid, username: r.username, name: r.name, premium_until: r.premium_until, aupd: r.aupd, is_admin: r.is_admin, gcol: r.gcol, gl: r.gl, badge: r.badge }), last: { id: r.mid, text: r.text, kind: r.kind || '', created: r.created, mine: r.from_id === u.id } })) });
  }
  if (key === 'GET /api/messages') {
    const p = await userBy(url.searchParams.get('with')); if (!p) return fail(res, 404, 'Foydalanuvchi topilmadi.');
    const before = Number(url.searchParams.get('before')) || 9e15;
    const cl = await q('SELECT upto FROM chat_clear WHERE user_id=? AND peer_id=?').get(u.id, p.id);
    const rows = await q('SELECT * FROM messages WHERE ((from_id=? AND to_id=?) OR (from_id=? AND to_id=?)) AND id<? AND id>? ORDER BY id DESC LIMIT 50').all(u.id, p.id, p.id, u.id, before, cl ? cl.upto : 0);
    return send(res, 200, { user: pub(p), messages: rows.reverse().map(msgOut) });
  }
  if (key === 'POST /api/chats/clear') {
    const b = await readBody(req), names = [].concat(b.users || []).slice(0, 200);
    for (const nm of names) {
      const p = await userBy(nm); if (!p || p.id === u.id) continue;
      const mx = await q('SELECT MAX(id) m FROM messages WHERE (from_id=? AND to_id=?) OR (from_id=? AND to_id=?)').get(u.id, p.id, p.id, u.id);
      if (!mx || !mx.m) continue;
      await q('INSERT INTO chat_clear(user_id,peer_id,upto) VALUES(?,?,?) ON CONFLICT(user_id,peer_id) DO UPDATE SET upto=excluded.upto').run(u.id, p.id, mx.m);
      if (b.both) { await q('DELETE FROM messages WHERE (from_id=? AND to_id=?) OR (from_id=? AND to_id=?)').run(u.id, p.id, p.id, u.id); push(p.id, 'chatclear', { user: u.id }); }
    }
    return send(res, 200, { ok: true });
  }
  if (key === 'POST /api/messages') {
    if (limited('msg' + u.id, 60, 60000)) return fail(res, 429, 'Juda tez yozyapsiz.');
    const b = await readBody(req), p = await userBy(b.to), text = String(b.text || '').trim();
    if (!p || p.id === u.id) return fail(res, 404, 'Foydalanuvchi topilmadi.');
    const mx = await takeMedia(b, u.id); if (mx.err) return fail(res, 400, mx.err);
    if ((!text && !mx.id) || text.length > 4000) return fail(res, 400, 'Xabar 1–4000 belgi bo’lsin.');
    if (await blocked(u.id, p.id)) return fail(res, 403, 'Bu foydalanuvchiga yozib bo’lmaydi.');
    const r = await q('INSERT INTO messages(from_id,to_id,text,created,kind,media_id,vtext) VALUES(?,?,?,?,?,?,?)').run(u.id, p.id, text, Date.now(), mx.kind, mx.id, mx.vtext);
    const m = msgOut(await q('SELECT * FROM messages WHERE id=?').get(r.lastInsertRowid));
    push(p.id, 'message', { message: m, from: pub(u) }); push(u.id, 'message', { message: m, from: pub(u) });
    return send(res, 201, { message: m });
  }
  let m2 = /^(PATCH|DELETE) \/api\/messages\/(\d+)$/.exec(key);
  if (m2) {
    const m = await q('SELECT * FROM messages WHERE id=?').get(Number(m2[2]));
    if (!m || m.from_id !== u.id) return fail(res, 404, 'Xabar topilmadi.');
    if (m2[1] === 'DELETE') { await q('DELETE FROM messages WHERE id=?').run(m.id); if (m.media_id) await q('DELETE FROM media WHERE id=?').run(m.media_id); const ev = { id: m.id }; push(m.to_id, 'delete', ev); push(u.id, 'delete', ev); return send(res, 200, { ok: true }); }
    if (m.kind) return fail(res, 400, 'Rasm yoki ovozli xabarni tahrirlab bo’lmaydi.');
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
      .map((m) => ({ from: m.from_id === u.id ? u.username : p.username, text: kindLabel(m), created: m.created }));
    await q('INSERT INTO reports(reporter_id,reported_id,snapshot,created) VALUES(?,?,?,?)').run(u.id, p.id, JSON.stringify(rows), Date.now());
    return send(res, 201, { ok: true });
  }
  if (key === 'GET /api/events') {
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store', connection: 'keep-alive', 'x-accel-buffering': 'no' });
    res.write('retry: 3000\n\n'); (streams.get(u.id) || streams.set(u.id, new Set()).get(u.id)).add(res);
    req.on('close', () => { const s = streams.get(u.id); if (s) { s.delete(res); if (!s.size) streams.delete(u.id); } }); return;
  }

  if (key === 'POST /api/media') {
    if (limited('media' + u.id, 40, 3600000)) return fail(res, 429, 'Juda ko’p fayl yubordingiz. Keyinroq urinib ko’ring.');
    const b = await readBody(req, 1000000), kind = b.kind === 'voice' ? 'voice' : b.kind === 'img' ? 'img' : '', d = String(b.data || '');
    if (!kind) return fail(res, 400, 'Fayl turi noto’g’ri.');
    const mt = (kind === 'img' ? MEDIA_IMG : MEDIA_AUD).exec(d); if (!mt) return fail(res, 400, kind === 'img' ? 'Rasm noto’g’ri.' : 'Ovozli xabar formati noto’g’ri.');
    const size = Math.floor(mt[2].length * 0.75), dur = Math.max(0, Math.min(600, Math.round(Number(b.dur) || 0)));
    if (size > (kind === 'img' ? 450000 : 700000)) return fail(res, 413, kind === 'img' ? 'Rasm juda katta.' : 'Ovozli xabar juda uzun.');
    if (kind === 'voice' && dur > 180) return fail(res, 413, 'Ovozli xabar 3 daqiqadan oshmasin.');
    if (Number((await q('SELECT COALESCE(SUM(size),0) s FROM media WHERE owner_id=?').get(u.id)).s) + size > USER_QUOTA) return fail(res, 413, 'Fayllar uchun joy tugadi. Eski rasm va ovozlar o’chirilgach yana yuborasiz.');
    const r = await q('INSERT INTO media(owner_id,kind,mime,data,size,dur,created) VALUES(?,?,?,?,?,?,?)').run(u.id, kind, mt[1], mt[2], size, dur, Date.now());
    return send(res, 201, { media: { id: Number(r.lastInsertRowid), kind, size, dur } });
  }
  let mx2 = /^GET \/api\/media\/(\d+)$/.exec(key);
  if (mx2) {
    const id = Number(mx2[1]), row = await q('SELECT mime,data FROM media WHERE id=?').get(id);
    if (!row || !await canSeeMedia(id, u.id)) return fail(res, 404, 'Topilmadi.');
    return sendBinary(req, res, row.mime, Buffer.from(row.data, 'base64'));
  }
  if (key === 'PUT /api/me/avatar') {
    const a = String((await readBody(req, 400000)).avatar || '');
    if (a && !(MEDIA_IMG.test(a) && a.length * 0.75 <= 150000)) return fail(res, 400, 'Rasm noto’g’ri yoki juda katta.');
    if (a) { await q('INSERT OR REPLACE INTO avatars(user_id,data,upd) VALUES(?,?,?)').run(u.id, a, Date.now()); await q('UPDATE users SET aupd=? WHERE id=?').run(Date.now(), u.id); }
    else { await q('DELETE FROM avatars WHERE user_id=?').run(u.id); await q('UPDATE users SET aupd=0 WHERE id=?').run(u.id); }
    bcast(u.id);
    return send(res, 200, { user: me(await q('SELECT * FROM users WHERE id=?').get(u.id)) });
  }
  mx2 = /^GET \/api\/users\/(\d+)\/avatar$/.exec(key);
  if (mx2) {
    const d = await q('SELECT data FROM avatars WHERE user_id=?').get(Number(mx2[1])), mt = d && MEDIA_IMG.exec(d.data);
    if (!mt) return fail(res, 404, 'Topilmadi.');
    return sendBinary(req, res, mt[1], Buffer.from(mt[2], 'base64'));
  }

  // ---------- kanal va guruhlar ----------
  const mm = /^(PATCH|DELETE|POST) \/api\/cmsgs\/(\d+)(\/react)?$/.exec(key);
  if (mm) {
    const m = await cmById(Number(mm[2])); if (!m || !await isMember(m.channel_id, u.id)) return fail(res, 404, 'Xabar topilmadi.');
    const row = await chById(m.channel_id);
    if (mm[3]) {
      if (mm[1] !== 'POST') return fail(res, 405, 'Noto’g’ri so’rov.');
      if (limited('rx' + u.id, 120, 60000)) return fail(res, 429, 'Juda tez bosyapsiz.');
      if (row.kind !== 'k') return fail(res, 400, 'Reaksiya faqat kanal postlariga.');
      if (lvOf(Number(row.v)) <= 5) return fail(res, 403, 'Reaksiyalar kanal 6-darajadan oshganda ochiladi.');
      const em = String((await readBody(req)).emoji || ''); if (!REACTS.includes(em)) return fail(res, 400, 'Reaksiya noto’g’ri.');
      const cur = await q('SELECT emoji FROM creacts WHERE msg_id=? AND user_id=?').get(m.id, u.id);
      if (cur && cur.emoji === em) await q('DELETE FROM creacts WHERE msg_id=? AND user_id=?').run(m.id, u.id);
      else await q('INSERT OR REPLACE INTO creacts(msg_id,user_id,emoji) VALUES(?,?,?)').run(m.id, u.id, em);
      const counts = (await reactCounts([m.id]))[m.id] || {};
      await pushCh(m.channel_id, 'creact', { id: m.id, channel: m.channel_id, counts });
      return send(res, 200, { counts, my: cur && cur.emoji === em ? '' : em });
    }
    if (mm[1] === 'PATCH') {
      if (m.from_id !== u.id) return fail(res, 403, 'Faqat o’z xabaringizni tahrirlaysiz.');
      if (m.kind) return fail(res, 400, 'Rasm yoki ovozli xabarni tahrirlab bo’lmaydi.');
      const text = String((await readBody(req)).text || '').trim(); if (!text || text.length > 4000) return fail(res, 400, 'Xabar 1–4000 belgi bo’lsin.');
      await q('UPDATE cmsgs SET text=?,edited=1 WHERE id=?').run(text, m.id);
      const out = cmOut(await cmById(m.id)); await pushCh(m.channel_id, 'cedit', { message: out }); return send(res, 200, { message: out });
    }
    if (mm[1] === 'DELETE') {
      if (m.from_id !== u.id && row.owner_id !== u.id) return fail(res, 403, 'Ruxsat yo’q.');
      await q('DELETE FROM creacts WHERE msg_id=?').run(m.id); await q('DELETE FROM cmsgs WHERE id=?').run(m.id); if (m.media_id) await q('DELETE FROM media WHERE id=?').run(m.media_id);
      await pushCh(m.channel_id, 'cdelete', { id: m.id, channel: m.channel_id }); return send(res, 200, { ok: true });
    }
    return fail(res, 405, 'Noto’g’ri so’rov.');
  }
  const mc = /^(GET|POST|PATCH|DELETE) \/api\/channels(?:\/(\d+)(?:\/(avatar|bg|join|leave|members|messages|vote|votes)(?:\/([a-z_]+))?)?)?$/.exec(key);
  if (mc) {
    const meth = mc[1], cid = mc[2] ? Number(mc[2]) : 0, sub = mc[3] || '', arg = mc[4] || '';
    if (!cid) {
      if (meth === 'GET') {
        const rows = await q(CHSEL + ' JOIN members m ON m.channel_id=c.id WHERE m.user_id=? ORDER BY c.id DESC').all(u.id);
        const lm = new Map((await q('SELECT x.id,x.channel_id,x.text,x.kind,x.created,u.name fname FROM cmsgs x JOIN users u ON u.id=x.from_id WHERE x.id IN (SELECT MAX(id) FROM cmsgs WHERE channel_id IN (SELECT channel_id FROM members WHERE user_id=?) GROUP BY channel_id)').all(u.id)).map((l) => [l.channel_id, l]));
        return send(res, 200, { channels: rows.map((r) => { const l = lm.get(r.id); return { ...chOut(r, u.id), last: l ? { id: l.id, text: l.text, kind: l.kind || '', created: l.created, name: l.fname } : null }; }), left: await votesLeft(u.id) });
      }
      if (meth !== 'POST') return fail(res, 405, 'Noto’g’ri so’rov.');
      if (limited('chc' + u.id, 20, 3600000)) return fail(res, 429, 'Juda ko’p urinish. Keyinroq qayta urining.');
      const b = await readBody(req, 700000), kind = b.kind === 'g' ? 'g' : b.kind === 'k' ? 'k' : '';
      const un = String(b.username || '').trim().toLowerCase().replace(/^@/, ''), name = String(b.name || '').trim().slice(0, 50), descr = String(b.descr || '').slice(0, 300);
      if (!kind) return fail(res, 400, 'Tur noto’g’ri.');
      if (!name) return fail(res, 400, 'Nom bo’sh bo’lmasin.');
      const e = nameErr(un); if (e) return fail(res, 400, e);
      if (un === ADMIN_USER || await taken(un)) return fail(res, 409, 'Bu username band.');
      if (Number((await q('SELECT COUNT(*) n FROM channels WHERE owner_id=?').get(u.id)).n) >= 30) return fail(res, 400, 'Juda ko’p kanal yaratdingiz (30 tagacha).');
      const av = b.avatar ? String(b.avatar) : ''; if (av && !imgOk(av, 150000)) return fail(res, 400, 'Rasm noto’g’ri yoki juda katta.');
      const now = Date.now(); let r;
      try { r = await q('INSERT INTO channels(kind,username,name,descr,private,owner_id,avatar,upd,created) VALUES(?,?,?,?,?,?,?,?,?)').run(kind, un, name, descr, kind === 'g' ? 1 : b.private ? 1 : 0, u.id, av, now, now); }
      catch (er) { if (/UNIQUE/i.test(er.message)) return fail(res, 409, 'Bu username band.'); throw er; }
      const id = Number(r.lastInsertRowid), names = [...new Set((Array.isArray(b.members) ? b.members : []).map((x) => String(x).toLowerCase().replace(/^@/, '')))].slice(0, 50);
      let ids = [];
      if (names.length) {
        const found = (await q(`SELECT id FROM users WHERE username IN (${names.map(() => '?').join(',')}) AND id!=?`).all(...names, u.id)).map((x) => x.id);
        const bad = new Set((await q('SELECT blocked_id x FROM blocks WHERE blocker_id=? UNION SELECT blocker_id FROM blocks WHERE blocked_id=?').all(u.id, u.id)).map((x) => x.x));
        ids = found.filter((x) => !bad.has(x));
      }
      const all = [u.id, ...ids];
      await q(`INSERT OR IGNORE INTO members(channel_id,user_id,joined) VALUES ${all.map(() => '(?,?,?)').join(',')}`).run(...all.flatMap((x) => [id, x, now]));
      await pushChannel(id);
      return send(res, 201, { channel: chOut(await chById(id), u.id) });
    }
    const row = await chById(cid); if (!row) return fail(res, 404, 'Topilmadi.');
    const mem = await isMember(cid, u.id), owner = row.owner_id === u.id, open = row.kind === 'k' && !row.private;
    if (!sub) {
      if (meth === 'GET') { if (!mem && !open) return fail(res, 404, 'Topilmadi.'); return send(res, 200, { channel: chOut(row, u.id), member: mem }); }
      if (!owner) return fail(res, 403, 'Faqat egasi o’zgartira oladi.');
      if (meth === 'DELETE') {
        const ids = await memberIds(cid);
        await q('DELETE FROM media WHERE id IN (SELECT media_id FROM cmsgs WHERE channel_id=? AND media_id>0)').run(cid);
        await q('DELETE FROM creacts WHERE msg_id IN (SELECT id FROM cmsgs WHERE channel_id=?)').run(cid);
        for (const t of ['cmsgs', 'members', 'votes']) await q(`DELETE FROM ${t} WHERE channel_id=?`).run(cid);
        await q('DELETE FROM channels WHERE id=?').run(cid);
        for (const id of ids) push(id, 'cremove', { id: cid }); return send(res, 200, { ok: true });
      }
      if (meth !== 'PATCH') return fail(res, 405, 'Noto’g’ri so’rov.');
      const b = await readBody(req, 700000), sets = [], args = [];
      if (b.name !== undefined) { const n = String(b.name).trim().slice(0, 50); if (!n) return fail(res, 400, 'Nom bo’sh bo’lmasin.'); sets.push('name=?'); args.push(n); }
      if (b.descr !== undefined) { sets.push('descr=?'); args.push(String(b.descr).slice(0, 300)); }
      if (b.avatar !== undefined) { const a = String(b.avatar || ''); if (a && !imgOk(a, 150000)) return fail(res, 400, 'Rasm noto’g’ri yoki juda katta.'); sets.push('avatar=?'); args.push(a); }
      if (b.bg !== undefined) {
        const g = String(b.bg || ''); if (row.kind !== 'k') return fail(res, 400, 'Chat foni faqat kanalda.');
        if (g) { if (lvOf(Number(row.v)) <= 10) return fail(res, 403, 'Chat foni kanal 11-darajadan oshganda ochiladi.'); if (!imgOk(g, 400000)) return fail(res, 400, 'Rasm noto’g’ri yoki juda katta.'); }
        sets.push('bg=?'); args.push(g);
      }
      if (sets.length) { sets.push('upd=?'); args.push(Date.now()); await q(`UPDATE channels SET ${sets.join(',')} WHERE id=?`).run(...args, cid); await pushChannel(cid); }
      return send(res, 200, { channel: chOut(await chById(cid), u.id) });
    }
    if (sub === 'avatar' || sub === 'bg') {
      if (meth !== 'GET') return fail(res, 405, 'Noto’g’ri so’rov.');
      if (!mem && !open) return fail(res, 404, 'Topilmadi.');
      const d = await q(`SELECT ${sub} v FROM channels WHERE id=?`).get(cid), mt = d && IMG.exec(d.v || ''); if (!mt) return fail(res, 404, 'Topilmadi.');
      res.writeHead(200, { 'content-type': mt[1], 'cache-control': 'private, max-age=86400' }); return res.end(Buffer.from(mt[2], 'base64'));
    }
    if (sub === 'join') {
      if (meth !== 'POST') return fail(res, 405, 'Noto’g’ri so’rov.');
      if (!open) return fail(res, 403, 'Bu kanalga qo’shilib bo’lmaydi.');
      if (!mem) { await q('INSERT OR IGNORE INTO members(channel_id,user_id,joined) VALUES(?,?,?)').run(cid, u.id, Date.now()); await pushChannel(cid, [row.owner_id]); }
      return send(res, 200, { channel: chOut(await chById(cid), u.id) });
    }
    if (sub === 'leave') {
      if (meth !== 'POST') return fail(res, 405, 'Noto’g’ri so’rov.');
      if (owner) return fail(res, 400, 'Egasi chiqib keta olmaydi. Kerak bo’lsa o’chiring.');
      if (mem) { await q('DELETE FROM members WHERE channel_id=? AND user_id=?').run(cid, u.id); push(u.id, 'cremove', { id: cid }); await pushChannel(cid, [row.owner_id]); }
      return send(res, 200, { ok: true });
    }
    if (sub === 'members') {
      if (meth === 'GET') {
        if (!mem || (row.kind === 'k' && !owner)) return fail(res, 403, 'Ruxsat yo’q.');
        const us = await q('SELECT x.* FROM members m JOIN users x ON x.id=m.user_id WHERE m.channel_id=? ORDER BY m.joined LIMIT 300').all(cid);
        return send(res, 200, { members: us.map((x) => ({ ...pub(x), owner: x.id === row.owner_id })) });
      }
      if (!owner) return fail(res, 403, 'Faqat egasi a’zo qo’sha va chiqara oladi.');
      if (meth === 'POST') {
        const p = await userBy((await readBody(req)).username); if (!p) return fail(res, 404, 'Foydalanuvchi topilmadi.');
        if (await blocked(u.id, p.id)) return fail(res, 403, 'Bu foydalanuvchini qo’shib bo’lmaydi.');
        await q('INSERT OR IGNORE INTO members(channel_id,user_id,joined) VALUES(?,?,?)').run(cid, p.id, Date.now()); await pushChannel(cid, [p.id]);
        return send(res, 200, { ok: true });
      }
      if (meth === 'DELETE') {
        const p = await userBy(arg); if (!p || p.id === u.id) return fail(res, 404, 'Foydalanuvchi topilmadi.');
        await q('DELETE FROM members WHERE channel_id=? AND user_id=?').run(cid, p.id); push(p.id, 'cremove', { id: cid }); return send(res, 200, { ok: true });
      }
      return fail(res, 405, 'Noto’g’ri so’rov.');
    }
    if (sub === 'messages') {
      if (!mem) return fail(res, 403, 'Avval a’zo bo’ling.');
      if (meth === 'GET') {
        const before = Number(url.searchParams.get('before')) || 9e15;
        const rows = (await q('SELECT x.*,u.name fname FROM cmsgs x JOIN users u ON u.id=x.from_id WHERE x.channel_id=? AND x.id<? ORDER BY x.id DESC LIMIT 50').all(cid, before)).reverse();
        const ids = rows.map((x) => x.id), cnt = await reactCounts(ids);
        const my = ids.length ? Object.fromEntries((await q(`SELECT msg_id,emoji FROM creacts WHERE user_id=? AND msg_id IN (${ids.map(Number).join(',')})`).all(u.id)).map((x) => [x.msg_id, x.emoji])) : {};
        return send(res, 200, { messages: rows.map((x) => ({ ...cmOut(x), reacts: cnt[x.id] || {}, my: my[x.id] || '' })) });
      }
      if (meth !== 'POST') return fail(res, 405, 'Noto’g’ri so’rov.');
      if (limited('cmsg' + u.id, 60, 60000)) return fail(res, 429, 'Juda tez yozyapsiz.');
      if (row.kind === 'k' && !owner) return fail(res, 403, 'Kanalga faqat egasi post yozadi.');
      const b = await readBody(req), text = String(b.text || '').trim(), mx = await takeMedia(b, u.id); if (mx.err) return fail(res, 400, mx.err);
      if ((!text && !mx.id) || text.length > 4000) return fail(res, 400, 'Xabar 1–4000 belgi bo’lsin.');
      const r = await q('INSERT INTO cmsgs(channel_id,from_id,text,created,kind,media_id,vtext) VALUES(?,?,?,?,?,?,?)').run(cid, u.id, text, Date.now(), mx.kind, mx.id, mx.vtext);
      const out = cmOut(await cmById(r.lastInsertRowid)); await pushCh(cid, 'cmsg', { message: out }); return send(res, 201, { message: out });
    }
    if (sub === 'vote') {
      if (meth !== 'POST') return fail(res, 405, 'Noto’g’ri so’rov.');
      if (row.kind !== 'k') return fail(res, 400, 'Ovoz faqat kanalga beriladi.');
      if (u.premium_until <= Date.now()) return fail(res, 403, 'Ovoz berish faqat Premium foydalanuvchilarga.');
      if (!mem) return fail(res, 403, 'Avval kanalga a’zo bo’ling.');
      const r = await q(`INSERT INTO votes(user_id,channel_id,n) SELECT ?,?,1 WHERE (SELECT COALESCE(SUM(n),0) FROM votes WHERE user_id=?)<${VOTES_EACH} ON CONFLICT(user_id,channel_id) DO UPDATE SET n=n+1`).run(u.id, cid, u.id);
      if (!r.changes) return fail(res, 400, 'Ovozlaringiz tugadi (5/5). Har Premium foydalanuvchida 5 ta ovoz bor: bitta kanalga 5 tadan yoki 5 ta kanalga bittadan.');
      await pushChannel(cid); return send(res, 200, { channel: chOut(await chById(cid), u.id), left: await votesLeft(u.id) });
    }
    if (sub === 'votes') {
      if (!owner) return fail(res, 403, 'Faqat egasi ko’ra oladi.');
      const rows = await q('SELECT x.username,x.name,v.n FROM votes v JOIN users x ON x.id=v.user_id WHERE v.channel_id=? ORDER BY v.n DESC LIMIT 200').all(cid);
      return send(res, 200, { votes: rows.map((x) => ({ username: x.username, name: x.name, n: Number(x.n) })) });
    }
    return fail(res, 404, 'Topilmadi.');
  }



  // ---------- o'qilgan belgisi, saqlangan xabarlar, chat foni, nik belgisi, ovozni matnga aylantirish ----------
  if (key === 'POST /api/messages/read') {
    const b = await readBody(req), p = await userBy(b.with); if (!p) return fail(res, 404, 'Foydalanuvchi topilmadi.');
    const r = await q('UPDATE messages SET rd=1 WHERE from_id=? AND to_id=? AND rd=0').run(p.id, u.id);
    if (r.changes > 0) push(p.id, 'read', { by: u.id });
    return send(res, 200, { ok: true, n: r.changes });
  }
  const savedOut = (m) => ({ id: m.id, from: u.id, to: u.id, text: m.text, created: m.created, edited: false, kind: m.kind || '', media: m.media_id || 0, vtext: m.vtext || '', gift: null, read: true });
  if (key === 'GET /api/saved') {
    const rows = await q('SELECT * FROM saved WHERE user_id=? ORDER BY id DESC LIMIT 200').all(u.id);
    return send(res, 200, { messages: rows.reverse().map(savedOut) });
  }
  if (key === 'POST /api/saved') {
    if (limited('sv' + u.id, 60, 60000)) return fail(res, 429, 'Juda tez.');
    const b = await readBody(req), text = String(b.text || '').trim(), mx = await takeMedia(b, u.id); if (mx.err) return fail(res, 400, mx.err);
    if ((!text && !mx.id) || text.length > 4000) return fail(res, 400, 'Xabar 1–4000 belgi bo’lsin.');
    const r = await q('INSERT INTO saved(user_id,text,kind,media_id,vtext,created) VALUES(?,?,?,?,?,?)').run(u.id, text, mx.kind, mx.id, mx.vtext, Date.now());
    return send(res, 201, { message: savedOut(await q('SELECT * FROM saved WHERE id=?').get(r.lastInsertRowid)) });
  }
  const sd = /^DELETE \/api\/saved\/(\d+)$/.exec(key);
  if (sd) { await q('DELETE FROM saved WHERE id=? AND user_id=?').run(Number(sd[1]), u.id); return send(res, 200, { ok: true }); }
  if (key === 'PUT /api/me/chatbg') {
    if (!(u.premium_until > Date.now())) return fail(res, 403, 'Chat foni — Premium imkoniyati.');
    const a = String((await readBody(req, 500000)).image || '');
    if (a && !(MEDIA_IMG.test(a) && a.length * 0.75 <= 330000)) return fail(res, 400, 'Rasm noto’g’ri yoki juda katta.');
    if (a) { const n = Date.now(); await q('INSERT OR REPLACE INTO chatbg(user_id,data,upd) VALUES(?,?,?)').run(u.id, a, n); await q('UPDATE users SET bgupd=? WHERE id=?').run(n, u.id); }
    else { await q('DELETE FROM chatbg WHERE user_id=?').run(u.id); await q('UPDATE users SET bgupd=0 WHERE id=?').run(u.id); }
    return send(res, 200, { user: me(await q('SELECT * FROM users WHERE id=?').get(u.id)) });
  }
  if (key === 'GET /api/me/chatbg') {
    const d = await q('SELECT data FROM chatbg WHERE user_id=?').get(u.id), mt = d && MEDIA_IMG.exec(d.data); if (!mt) return fail(res, 404, 'Topilmadi.');
    const buf = Buffer.from(mt[2], 'base64'); res.writeHead(200, { 'content-type': mt[1], 'cache-control': 'private, max-age=31536000, immutable', 'content-length': buf.length }); return res.end(buf);
  }
  if (key === 'PUT /api/me/badge') {
    if (!(u.premium_until > Date.now())) return fail(res, 403, 'Nik belgisi — Premium imkoniyati.');
    const b = await readBody(req, 300000); let badge = '';
    if (b.logo) { if (!/^b[1-7]$/.test(String(b.logo))) return fail(res, 400, 'Belgi topilmadi.'); badge = 'l:' + b.logo; }
    else if (b.emoji) { const e = String(b.emoji).trim(); if (e.length > 16 || !/^(?:\p{Extended_Pictographic}|\p{Emoji_Modifier}|\uFE0F|\u200D|[\u{1F1E6}-\u{1F1FF}])+$/u.test(e)) return fail(res, 400, 'Faqat emoji kiriting.'); badge = 'e:' + e; }
    else if (b.image) { const a = String(b.image); if (!(MEDIA_IMG.test(a) && a.length * 0.75 <= 60000)) return fail(res, 400, 'Rasm noto’g’ri yoki juda katta.'); const n = Date.now(); await q('INSERT OR REPLACE INTO badges(user_id,data,upd) VALUES(?,?,?)').run(u.id, a, n); badge = 'i:' + n; }
    await q('UPDATE users SET badge=? WHERE id=?').run(badge, u.id); bcast(u.id);
    return send(res, 200, { user: me(await q('SELECT * FROM users WHERE id=?').get(u.id)) });
  }
  const bd = /^GET \/api\/users\/(\d+)\/badge$/.exec(key);
  if (bd) {
    const d = await q('SELECT data FROM badges WHERE user_id=?').get(Number(bd[1])), mt = d && MEDIA_IMG.exec(d.data); if (!mt) return fail(res, 404, 'Topilmadi.');
    const buf = Buffer.from(mt[2], 'base64'); res.writeHead(200, { 'content-type': mt[1], 'cache-control': 'private, max-age=31536000, immutable', 'content-length': buf.length }); return res.end(buf);
  }
  const tr = /^POST \/api\/media\/(\d+)\/transcribe$/.exec(key);
  if (tr) {
    if (!(u.premium_until > Date.now())) return fail(res, 403, 'Ovozni matnga aylantirish — Premium imkoniyati.');
    if (limited('tr' + u.id, 15, 60000)) return fail(res, 429, 'Juda tez. Biroz kuting.');
    const id = Number(tr[1]); if (!await canSeeMedia(id, u.id)) return fail(res, 404, 'Topilmadi.');
    const md = await q('SELECT mime,data,kind FROM media WHERE id=?').get(id); if (!md || md.kind !== 'voice') return fail(res, 404, 'Ovozli xabar topilmadi.');
    const old = await q(`SELECT vtext FROM (SELECT vtext FROM messages WHERE media_id=? UNION ALL SELECT vtext FROM cmsgs WHERE media_id=? UNION ALL SELECT vtext FROM saved WHERE media_id=?) WHERE vtext IS NOT NULL AND vtext!='' LIMIT 1`).get(id, id, id);
    if (old) return send(res, 200, { text: old.vtext });
    const HF = process.env.HF_TOKEN; if (!HF) return fail(res, 501, 'Matnga aylantirish hali sozlanmagan (HF_TOKEN kerak).');
    const mt = MEDIA_AUD.exec(md.data) || [null, md.mime, md.data.split(',')[1] || ''];
    const buf = Buffer.from(mt[2], 'base64');
    let text = '';
    try {
      const r = await fetch(process.env.HF_URL || `https://router.huggingface.co/hf-inference/models/${process.env.HF_MODEL || 'openai/whisper-large-v3'}`, { method: 'POST', headers: { authorization: 'Bearer ' + HF, 'content-type': mt[1] }, body: buf, signal: AbortSignal.timeout(90000) });
      const j = await r.json().catch(() => ({}));
      if (r.status === 503) return fail(res, 503, 'Model yuklanmoqda. Yarim daqiqadan keyin qayta urinib ko’ring.');
      if (!r.ok) { console.error('HF', r.status, JSON.stringify(j).slice(0, 200)); return fail(res, 502, 'Matnga aylantirib bo’lmadi. Keyinroq urinib ko’ring.'); }
      text = String(j.text || '').trim();
    } catch (e) { console.error('HF', e.message); return fail(res, 502, 'Matnga aylantirib bo’lmadi. Keyinroq urinib ko’ring.'); }
    if (!text) text = '(ovoz tushunarsiz yoki jim)';
    for (const t of ['messages', 'cmsgs', 'saved']) await q(`UPDATE ${t} SET vtext=? WHERE media_id=?`).run(text.slice(0, 2000), id);
    return send(res, 200, { text });
  }

  // ---------- sovg'alar, Stars daftari, savdo ----------
  if (url.pathname.startsWith('/api/gifts') || key === 'GET /api/stars/ledger') {
    await tickPrices();
    if (key === 'GET /api/stars/ledger') {
      const rows = await q('SELECT delta,balance,reason,ref,created FROM stars_ledger WHERE user_id=? ORDER BY id DESC LIMIT 50').all(u.id);
      return send(res, 200, { stars: (await q('SELECT stars FROM users WHERE id=?').get(u.id)).stars, ledger: rows });
    }
    if (key === 'GET /api/gifts') return send(res, 200, { gifts: await giftList(), step: PRICE_STEP });
    if (key === 'GET /api/gifts/mine') {
      const rows = await q('SELECT id,gift,hidden FROM gifts_owned WHERE owner_id=? AND sold=0 ORDER BY id').all(u.id), pr = await curPrices(), uu = await q('SELECT gsel FROM users WHERE id=?').get(u.id);
      return send(res, 200, { gifts: rows.map((r) => ({ id: r.id, gift: r.gift, hidden: !!r.hidden, price: pr[r.gift] })), selected: uu.gsel });
    }
    if (key === 'POST /api/gifts/buy') {
      if (limited('gb' + u.id, 30, 60000)) return fail(res, 429, 'Juda tez. Biroz kuting.');
      const b = await readBody(req), gi = Number(b.gift);
      if (!Number.isInteger(gi) || !CAT[gi]) return fail(res, 400, 'Sovg’a topilmadi.');
      let to = u; if (b.to) { to = await userBy(b.to); if (!to) return fail(res, 404, 'Foydalanuvchi topilmadi.'); }
      if (to.id !== u.id && await blocked(u.id, to.id)) return fail(res, 403, 'Bu foydalanuvchiga sovg’a yuborib bo’lmaydi.');
      const price = (await curPrices())[gi];
      if (b.expect != null && Math.abs(Number(b.expect) - price) > 0.005) return fail(res, 409, 'Narx o’zgardi: ★ ' + price + '. Qayta urinib ko’ring.', { price });
      // 1-qadam: Stars'ni bitta shartli buyruq bilan yechamiz (yetmasa 0 qator o’zgaradi)
      const dd = await q('UPDATE users SET stars=ROUND(stars-?,2) WHERE id=? AND stars>=?').run(price, u.id, price);
      if (dd.changes !== 1) return fail(res, 402, 'Stars yetarli emas.');
      let gid, msg = null;
      try {
        const now = Date.now();
        await ledger(u.id, -price, 'gift_buy', CAT[gi].n + (to.id !== u.id ? ' → @' + to.username : ''));
        gid = (await q('INSERT INTO gifts_owned(owner_id,gift,price,from_id,created) VALUES(?,?,?,?,?)').run(to.id, gi, price, u.id, now)).lastInsertRowid;
        await refreshGifts(to.id);
        if (to.id !== u.id) {
          const r = await q('INSERT INTO messages(from_id,to_id,text,created,kind,gift) VALUES(?,?,?,?,?,?)').run(u.id, to.id, '🎁 ' + CAT[gi].n + ' sovg’a qilindi (★ ' + price + ')', now, 'gift', gi);
          msg = msgOut(await q('SELECT * FROM messages WHERE id=?').get(r.lastInsertRowid));
        }
      } catch (e) {
        await q('UPDATE users SET stars=ROUND(stars+?,2) WHERE id=?').run(price, u.id); await ledger(u.id, price, 'refund', 'sovg’a xatosi').catch(() => {});
        if (gid) await q('UPDATE gifts_owned SET sold=1 WHERE id=?').run(gid).catch(() => {});
        console.error('gift buy', e.message); return fail(res, 500, 'Xatolik. Stars qaytarildi.');
      }
      const uu = await q('SELECT * FROM users WHERE id=?').get(u.id), tt = await q('SELECT * FROM users WHERE id=?').get(to.id);
      if (msg) { push(to.id, 'message', { message: msg, from: pub(uu) }); push(u.id, 'message', { message: msg, from: pub(uu) }); }
      push(to.id, 'gift', { id: gid }); 
      return send(res, 201, { id: gid, price, stars: uu.stars, to: pub(tt), message: msg });
    }
    const gm2 = /^(PATCH|POST) \/api\/gifts\/(\d+)(\/sell)?$/.exec(key);
    if (gm2) {
      const gid = Number(gm2[2]), row = await q('SELECT * FROM gifts_owned WHERE id=? AND owner_id=? AND sold=0').get(gid, u.id);
      if (!row) return fail(res, 404, 'Sovg’a topilmadi.');
      if (gm2[3]) { // sotish: avval "sotildi" deb belgilaymiz (faqat bir marta o’tadi), keyin Stars qo’shamiz
        const price = (await curPrices())[row.gift];
        const r = await q('UPDATE gifts_owned SET sold=1 WHERE id=? AND owner_id=? AND sold=0').run(gid, u.id);
        if (r.changes !== 1) return fail(res, 409, 'Sovg’a allaqachon sotilgan.');
        await q('UPDATE users SET stars=ROUND(stars+?,2) WHERE id=?').run(price, u.id);
        await ledger(u.id, price, 'gift_sell', CAT[row.gift].n);
        await refreshGifts(u.id);
        return send(res, 200, { price, stars: (await q('SELECT stars FROM users WHERE id=?').get(u.id)).stars });
      }
      const b = await readBody(req);
      if (b.hidden != null) await q('UPDATE gifts_owned SET hidden=? WHERE id=?').run(b.hidden ? 1 : 0, gid);
      if (b.select) { await q('UPDATE gifts_owned SET hidden=0 WHERE id=?').run(gid); await q('UPDATE users SET gsel=? WHERE id=?').run(gid, u.id); }
      await refreshGifts(u.id);
      const uu = await q('SELECT * FROM users WHERE id=?').get(u.id);
      return send(res, 200, { user: me(uu), selected: uu.gsel });
    }
    return fail(res, 404, 'Topilmadi.');
  }

  // ---------- admin ----------
  if (url.pathname.startsWith('/api/admin/')) {
    if (!u.is_admin) return fail(res, 403, 'Ruxsat yo’q.');
    if (key === 'GET /api/admin/reports') {
      const rows = await q('SELECT r.*,a.username rn,b.username dn FROM reports r JOIN users a ON a.id=r.reporter_id JOIN users b ON b.id=r.reported_id ORDER BY r.id DESC LIMIT 100').all();
      return send(res, 200, { reports: rows.map((r) => ({ id: r.id, reporter: r.rn, reported: r.dn, created: r.created, status: r.status, messages: JSON.parse(r.snapshot) })) });
    }
    if (key === 'GET /api/admin/user') {
      const p = await userBy(String(url.searchParams.get('username') || '').replace('@', '').trim()); if (!p) return fail(res, 404, 'Bunday foydalanuvchi topilmadi.');
      return send(res, 200, { user: { username: p.username, name: p.name, stars: p.stars, premium_until: p.premium_until, ava: pub(p).ava } });
    }
    if (key === 'POST /api/admin/stars') {
      const b = await readBody(req), p = await userBy(String(b.username || '').replace('@', '').trim()), d = Number(b.delta);
      if (!p || !Number.isFinite(d) || Math.abs(d) > 1e7) return fail(res, 400, 'Noto’g’ri so’rov.');
      const before = p.stars; await q('UPDATE users SET stars=MAX(0,ROUND(stars+?,2)) WHERE id=?').run(d, p.id); const after = (await q('SELECT stars FROM users WHERE id=?').get(p.id)).stars; await ledger(p.id, Math.round((after - before) * 100) / 100, 'admin', 'by @' + u.username); if (after > before) await adminMsg(u, p, '✅ Buyurtmangiz tushdi: ★ ' + (Math.round((after - before) * 100) / 100) + ' Stars hisobingizga qo’shildi.'); return send(res, 200, { stars: (await q('SELECT stars FROM users WHERE id=?').get(p.id)).stars });
    }
    if (key === 'POST /api/admin/premium') {
      const b = await readBody(req), p = await userBy(String(b.username || '').replace('@', '').trim()), PLANS = { '1m': [30, '1 oylik'], '3m': [90, '3 oylik'], '1y': [365, '1 yillik'] }, pl = PLANS[b.plan], days = pl ? pl[0] : Number(b.days);
      if (!p || !Number.isFinite(days) || days < 0 || days > 3660) return fail(res, 400, 'Noto’g’ri so’rov.');
      const until = days === 0 ? 0 : Math.max(Date.now(), p.premium_until) + days * DAY;
      await q('UPDATE users SET premium_until=? WHERE id=?').run(until, p.id); bcast(p.id); if (days > 0) await adminMsg(u, p, '✅ Buyurtmangiz tushdi: Premium ' + (pl ? pl[1] : days + ' kunlik') + ' faollashtirildi. Rahmat!'); return send(res, 200, { premium_until: until });
    }
  }
  return fail(res, 404, 'Topilmadi.');
}

// ---------- ilova (PWA): manifest, ikonkalar, service worker ----------
const B64 = (s) => Buffer.from(s, 'base64');
const SW = `const C='jg-v1';
self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!==C).map(x=>caches.delete(x)))).then(()=>self.clients.claim())));
self.addEventListener('fetch',e=>{const r=e.request,u=new URL(r.url);if(r.method!=='GET'||u.origin!==location.origin||u.pathname.startsWith('/api/'))return;
e.respondWith(fetch(r).then(res=>{if(res.ok){const cp=res.clone();caches.open(C).then(c=>c.put(r,cp))}return res}).catch(()=>caches.match(r).then(m=>m||caches.match('/'))))});`;
const MANIFEST = JSON.stringify({ id: '/', name: 'JoxaGram', short_name: 'JoxaGram', description: 'JoxaGram messenjer', lang: 'uz', start_url: '/', scope: '/', display: 'standalone', orientation: 'portrait', background_color: '#050b1f', theme_color: '#050b1f',
  icons: [{ src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' }, { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' }, { src: '/icon-maskable-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' }] });
const PWA = {
  '/manifest.webmanifest': ['application/manifest+json; charset=utf-8', MANIFEST],
  '/sw.js': ['text/javascript; charset=utf-8', SW],
  '/admin-badge.png': ['image/png', B64('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')],
  '/icon-192.png': ['image/png', B64('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')],
  '/icon-512.png': ['image/png', B64('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')],
  '/icon-maskable-512.png': ['image/png', B64('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')],
  '/apple-touch-icon.png': ['image/png', B64('iVBORw0KGgoAAAANSUhEUgAAALQAAAC0CAMAAAAKE/YAAAAAkFBMVEX4+f/v8//u8//t8//r8f+t0vdGrfsfbf8gbecZYOoTLnUSK28RKWoQKGYQJ2QPJWAPJF0OI1sOIlgOIVYNIFQNH1ENHk8MHk4MHUwLHUsMHEoLG0gLGkULGUMKGUIKGEAJGD8KFz0JFjsJFTkJFTgJEzUIFDYIEzMIEjEHES4HECwGDykGDicGDSUFDCIFCx+uOo+kAAAON0lEQVR42u3aiXLayBYGYNnGDgjEIiTAICRWA97y/m93W1uv53Sflplkaup2zVRNrFj58s9/WgsO1mytyrVcLhZptRK25uWK2ZqVa1quSbXG7YrYGklr6Fzy7y6/m5+qPnP1h1R/XPnnxiWglNSmxWK5XJbM0ht0RUcaejjsrkbRQg2jlxytmK1oT7OiRqLG0YsavWrQKxfaHfRw+CM1FDUNvYDQXYMOq2VTk/qhlhpEp13RoBh026K+BxqqNCXoUFsdonZMYkCdQ3LQYWhV+6PTTmivdoRhFzVlEpe8Hw40Umk/s6qmoB173p3RYehUd5xELWmPHW98B/TQUWrC9hF02qYVNMGMR91pzwvQC6Jj8xj9HB0RSv1X0SElaX9052sLyYxF/efQ2Bx2QEdel8S/hx79H/3fr8cfHMTEtuWlPlte9K/Y8v78xWX084tL53uPv3kZvwd62HUOu94wUR4Cxv+aW9N/w0PA2OsdgolOvR+3hk71XR63yE8u/s+Iw+5B054R7/UKYfiXXiEQ0RPSCyaSmTyHVvTyjq/FkPdL/8Rrsc6l/udfQBrov/aq16vSdjSp1B0+CLjPS/XVTz4JuMPHF9F9Pr7w7YeXemQPml5pBe1Xav9We39OhFS6RmOl1vphj3oysbrL47ZyeFRaQmOltkfdqsdhv8/SRtzswKjfD8eQmRK0Wul1sPEoNa4ehy9shRGsHg2j+vgYNRPQKxjtG7VQT/ol6mUYDc12sy9Ew+pwf2IPmljpTY1eE9GYelIFydUVvP63+u/GzKKedA8aRqP9MKI2C/LyoqulbYWbX14MMylordIM7dUPMOpR3H/B1bL5pR+PIDMaNNQOjrb3wxI1U7fleG7UY0XNfvkiHWYFwczkdihorR+2qBX1rAr6+RlSC3N9uD8bEc3o3lGhCf2wqUdxFfRzD1RHwtyrDofxSDdL5UCCRtDQ9UWPWikIf4YZj1oVpJ729aOjcQSZG/QMCVpuh4GmR92oo3lfqIS6vbbL5qYg8wgrxzReJDN30BUa7QcctaKOpqIcpnqqmpuCTCMo5+l0vpqEw6U7aIHWRxGM2lRHsRK0qtbMbdRxZJin0zhNh+Hg169wGQNBA2hK1HJBhDqaa0EL9QQwN1Gzgkxk9GSaLCaluFy1Ggu6RmfuqM2C8C1kEukuoY772LFoIgU9mcTpPBw05F+/BpvYEXRmou1Rq+px2jddjawPmZuCpO3Vm/2TJKOQi8uk17F0MYSCZuhM6YcetVU9js1ySFlD5nazbtRxMlPEbA0XjqAVtBG1XhBTHYNBS2rkUD+uck6SUCf/GqRzV9AlmhI1qB6nIQJr1eihMBnPk7EhrtuROIJW0FLU1oK0G9+snsLec09fTQuewUP1LCbS7FXrhbdDmOGgK7QR9dIRdXM9T/p4nFWgz/j/hL4WMvurt+1IoIuhHLSKtkVtqmfi1t93aZVg/0uCoP5iuMGnsA26RjtmUVFLvZ6F/XuQS/HDU69pxxIshxI0iJZnEc2asceb/h1CfggCVvWgaceCEHSDphVEzZqpY/ayw2tBtXjs9R4fe0o7DLMaNIzGCqKpZ+yWoVmptBJtlV9JN5uJsV08s1qU4sfHpwe5HXA5RNAtuqt6ItYYW1E0jSf6llyF/NR7eqyW0o4UL0eL3go0UhCbur3QyLd+2prO58Z1r5q9OuQGLdqRwuWQguZoMGqLGmQD8DiJzJCr2XviZKUdBPM22MJRe6hVdnV73Ihn5e3QAJk9afF2cHNdaLgcW4EGCiJqragJ7DLy6Vy751RnT0U37cjmlKC3Qb7FC0JXz3RynE7ts/cItWOVWMw86LxEUwrSbGSaWmYLOKtF4pw9Wzts5hqdYwWxqRF2BZ+nY/fswe1IYLNajrxEAwXxUaslUZ/38NmD2jGq2yGbgUJztFEQglpitzdRs2QREWcPasdKytlSjhqNFwRUI+zpfBmDIT9htQDbQTDnQeGj1sMWF8j5YjH0mD24HQRziS6CopALQlSr7FmcriZ+s4e1wzTLha6DLhq0iBpT6xURAzlLVqnv7IHt2CaYWS1HhcYKgqhlNnvDuVxP/GcPbMeaaq7QBPUSVMfpOuk0e3A7zL0OKnSLxmqNqesfKEvX646zB7cjlWJGzSqaohbFnierzazz7MHtsJtFOVo0OoygOkkWm9XwB7MHtWOdks1FsKepBZs972XTH80e2I4iJZv3wd6lVsJO0/V2+dPZA9uxWbRk2CzQ+xJNVi/S1TaLfj57UDs2ixXRvKvRO2AYTfVimRXze8we0g6FjJtZ0BVaiVpVt8VeLtdFpj89dZw9uB1LurlGu9Srdb6P7zZ7YDvMyyBubtAOdbG54+zB7VjoMVvM++DgVmd3nT2oHVFGM+9atE1dsV8PoT57D70fh2y0Y002Hzjaot7GajFYzuUL5d792rFfwjGr5qYchxLdqHeoOkuNGXzuPQRVRX4Cl9vhY67QDvW6GJofQv0qP1EpIw+6R87bkSm7htNco021zF6rnb5f5KIdKyhm1Nyg7epsOQv1PQ+I/NF3O3l4bN6G5Ss8ZsB8CI4UdX44FuVPNgzskfc63pUKMsnM0BQ126w3WXE4bKe2yJ/91Lwdh1eDbDUfOVrd+cyws/LSuNnuD8VihMH91LwdhdFmzbxTzcfgRFZn1TX9db3J9/tsDkX+IqHLj5SJVxZXzLqZoT3U7XX99XWdFftiwd6OKnIJXe+GpHYcX60xN9UQ5tOJoTE1EnYLX61YWfbbWIqco3tPAfsI3N4W3o7dWidbzQ3aqQbZm/rpoIx8HQ2UTrON5Fmvi60dGR4zZK7RhlqpiN4R1V3BUwXdC3ovzskU7VhbYt5B5lPwZlWDYcvsEr4qmktm7+GpNvOW91A1b8d+Y5K1ash9ZusteGvVtorY2e2960s5efyz+fpLD49PjvsOnIyZSzSidrEzCT3gc9jWGdoFwXacNhhZq7MwV2iaGmC38IxXWtTZccHR2rElxMzNNRpT72js9iGB+aQ6D2y1VtuxxWOGzA1aVxthW9n7YTuHUjXCfGCptdwOAlk1vwWXs1VNYG+O7RwK82B4KGYDvNa8HQeozA7zJbhcpKytYcts5XJzaubwgdd5kOzX4nnYrDVvhxADZMxcopusCWErbOHeNWhhznN2xXg9hViteTvOGZ3MzRUaU1vYsjtfard74TGr9zG01m07RsdMI+8cMZfmGu2sCMRu3NtsH6k/LDpst97NDqk1b0dOIcvm80VCY2rB3hWIOzuH6g+4Hjai7XCteTsumSQGyHo1avMluCpqXhGALaUtuXMVPSgK6aZtcwZrzdtx2kJkM2ZR53JdgyukhsNW2a07O0iVDk/bbNteLsrOF0CteTsKK1mvBjeX6FZ91tRuduUuXsVPxo/OLZcXPh4YBeHtuG71HQMiG+YKjYYNsneqe3vkczhYNJtBztF59mbWWrQjR8l6m9s6l+YG7VbLbDXuQ9qa2fNubqzswmv90APascfIWMwCrVXEyZa3wOOguVAU2xxae77vPWntyPdEsmrm6Fathq2xQXd+KB+2BqNLbk5p1Z9DrBakbUd0LhxkLebWfA1ut6stbCu7hufHfBgdT3mBrO1FqTVvBygGyLr5dmPoGxI2wtbdTL47vxWomf2trgOp1rwdtwIma+azFvOtRhtqLWwnu9xQdjiaHRO1fhLt2GliiGxU49aiuRoL22Cbcbd12UHmt6koiNyOgy+5MjdoM2yQrboxOLDyq6h12473nZN8hs234F1TGx2xscnwQtQ6aNpx3cNiTkZivt3eg3dUzcNW2YabBD/s2lo37dDEONk0l2gfNuZ20neXmfqg8L4HxAQyM9doQw2wKW5YX391967cdlftODrJQMwc3aqBsCF247bAob/LTbqFHbRgQAyRFXOLtoUtsSW3P/y0F+rw4yCDieTGLNBm2FQ2Vb6/ilpHtwMoppAZ+uODxD7DbjqdHd9/tLUewGKQbJg/PhjappbYctyaW5bb1uF9wNthis8kMjNXaKF2sOW4TfjpaLGfygIf35rnyflVA8tiiKyYazQQNsJW8wbg9nW4Vi92wvejepqLD5mjgbBRtub2kh+v1zAcvWO1cJAb80fw+Uliq+7zm7nc4up3vX18vp/AiBGyHvPH5ydDW9Q4G3RDfuMg6zcUsSKGyMJcoVu1k625L+fLW9d1xjO2kktzjfZg62575CSvJgbJUswCDahRtummhn6GvvNKJzfmz+Drk8LW3SC81p+17aX+ErJQsYVcornak427ics4oZ3MzV9fDP1lhC2zre6ucuBENyKZmWu0GTbGhtx+8qsDLIthcov2YcNuNx79Lg9ybeZooYbZqtsm91o3VCzIWswl+vubytbdP4TfNDGZ/P3N0FwNdERj382tn0cVQ2TJXKGtYTvdfvSb6dXATnKLBsK2xA25K7uFD2EdYoBcm1u0i224MbjXMk4KhqyTJbQ/+2fyd5vYSmbo399kNuj2l8Nn+XCQJTNDy2qQrbgROMmOfismhsm/fzP0bzBshU10G38Hym9UTk0hN2iEjcdNhPt5VTFMrswtGu6ILe4S/vEHxCZZoFG26jbgHUL/eDfP8elBltEqG40bdNNSh7QGWBbDZBWNs3U3DBfJI79wglHxt8JU0Ra24bbLCevTJraQTbTGdrm7yaHzfOFk3QygrWzQ7UFHvt1HjKB1tubG4Kjf/vu/bOJvkAejDbbudsCpSz8riYyjTbbh/pn8ywVGyTY0zd2FDp2ELnahATYmp9i/sPXtRXai/dz+69sbTEMj7jvAwdNSPCQ05u5q//5CTkbEUNE2t4/cchK6xAPtgNv4zm/79lL8D4wlMQWi11BfAAAAAElFTkSuQmCC')],
};

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
    if (PWA[url.pathname]) { const [t, b] = PWA[url.pathname]; res.writeHead(200, { 'content-type': t, 'cache-control': 'no-cache' }); return res.end(req.method === 'HEAD' ? undefined : b); }
    serveStatic(req, res, url);
  } catch (e) {
    if (e.message === 'big') res.setHeader('connection', 'close');
    if (!res.headersSent) fail(res, e.message === 'big' ? 413 : e.message === 'json' ? 400 : 500, e.message === 'big' ? 'Juda katta so’rov.' : e.message === 'json' ? 'JSON noto’g’ri.' : 'Server xatosi.');
    if (!['big', 'json'].includes(e.message)) console.error(e);
  }
});
server.listen(PORT, () => console.log(`JoxaGram server: http://localhost:${PORT}`));

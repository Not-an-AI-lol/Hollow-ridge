const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const LB_FILE = process.env.LB_FILE || path.join(__dirname, 'leaderboard.json');
const MAX_PEERS = 200;
const MAX_PRES_BYTES = 4096;
const MAX_MSGS_PER_SEC = 60;
const TICK_MS = 50;

let lb = {};
try { lb = JSON.parse(fs.readFileSync(LB_FILE, 'utf8')); } catch (e) { lb = {}; }
let lbDirty = false;
setInterval(() => {
  if (!lbDirty) return;
  lbDirty = false;
  try { fs.writeFileSync(LB_FILE, JSON.stringify(lb)); } catch (e) {}
}, 30000).unref();

function topBoard(n) {
  return Object.values(lb)
    .sort((a, b) => b.w - a.w || b.k - a.k)
    .slice(0, n || 20)
    .map(p => ({ n: p.n, w: p.w, k: p.k, d: p.d, g: p.g }));
}

const clean = (s, n) => String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n);

const server = http.createServer((req, res) => {
  const cors = { 'Access-Control-Allow-Origin': '*' };
  if (req.url === '/health') { res.writeHead(200, cors); res.end('ok'); return; }
  if (req.url === '/leaderboard') {
    res.writeHead(200, Object.assign({ 'Content-Type': 'application/json' }, cors));
    res.end(JSON.stringify(topBoard(50)));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Hollow Ridge server is running. Players: ' + wss.clients.size);
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });
let nextId = 1;
const peers = new Map();
let dirty = {};
let gone = [];

function send(ws, o) { if (ws.readyState === 1) { try { ws.send(JSON.stringify(o)); } catch (e) {} } }
function broadcast(o, exceptId) {
  const s = JSON.stringify(o);
  peers.forEach((p, id) => { if (id !== exceptId && p.ws.readyState === 1) { try { p.ws.send(s); } catch (e) {} } });
}
function merge(dst, patch) {
  for (const k in patch) { if (patch[k] === null) delete dst[k]; else dst[k] = patch[k]; }
}

wss.on('connection', (ws) => {
  if (peers.size >= MAX_PEERS) { send(ws, { t: 'full' }); ws.close(); return; }
  const id = 'p' + (nextId++).toString(36) + Math.random().toString(36).slice(2, 5);
  const me = { ws, pres: {}, name: 'Player', pid: '', alive: true, rate: 0, rateT: Date.now() };
  peers.set(id, me);
  const snapshot = {};
  peers.forEach((p, pid) => { if (pid !== id) snapshot[pid] = p.pres; });
  send(ws, { t: 'hi', id, peers: snapshot, n: peers.size });

  ws.on('pong', () => { me.alive = true; });
  ws.on('message', (raw) => {
    me.alive = true;
    const now = Date.now();
    if (now - me.rateT > 1000) { me.rate = 0; me.rateT = now; }
    if (++me.rate > MAX_MSGS_PER_SEC) return;
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    switch (m.t) {
      case 'ping': send(ws, { t: 'pong' }); break;
      case 'id': me.pid = clean(m.pid, 40); break;
      case 'p': {
        if (!m.p || typeof m.p !== 'object' || JSON.stringify(m.p).length > MAX_PRES_BYTES) return;
        merge(me.pres, m.p);
        if (typeof me.pres.n === 'string') me.name = clean(me.pres.n, 20) || 'Player';
        dirty[id] = Object.assign(dirty[id] || {}, m.p);
        break;
      }
      case 'chat': {
        const text = clean(m.m, 200);
        if (!text) return;
        broadcast({ t: 'chat', from: id, n: me.name, m: text, ts: Date.now() });
        break;
      }
      case 'ev': {
        const topic = clean(m.k, 24);
        if (!topic || JSON.stringify(m.d || null).length > 2048) return;
        broadcast({ t: 'ev', from: id, k: topic, d: m.d }, id);
        break;
      }
      case 'res': {
        const key = me.pid || id;
        const e = lb[key] || (lb[key] = { n: me.name, w: 0, k: 0, d: 0, g: 0 });
        e.n = me.name;
        e.g += 1;
        if (m.w) e.w += 1;
        e.k += Math.max(0, Math.min(200, m.k | 0));
        e.d += Math.max(0, Math.min(200, m.d | 0));
        lbDirty = true;
        send(ws, { t: 'lb', top: topBoard(20) });
        break;
      }
      case 'lbq': send(ws, { t: 'lb', top: topBoard(20) }); break;
    }
  });
  ws.on('close', () => {
    peers.delete(id);
    delete dirty[id];
    gone.push(id);
  });
  ws.on('error', () => {});
});

setInterval(() => {
  const keys = Object.keys(dirty);
  if (!keys.length && !gone.length) return;
  const d = dirty, g = gone;
  dirty = {}; gone = [];
  peers.forEach((p, id) => {
    if (p.ws.readyState !== 1) return;
    const u = {};
    let n = 0;
    for (const k of Object.keys(d)) { if (k !== id) { u[k] = d[k]; n++; } }
    if (n || g.length) { try { p.ws.send(JSON.stringify({ t: 'd', u, g })); } catch (e) {} }
  });
}, TICK_MS);

setInterval(() => {
  peers.forEach((p) => {
    if (!p.alive) { try { p.ws.terminate(); } catch (e) {} return; }
    p.alive = false;
    try { p.ws.ping(); } catch (e) {}
  });
}, 15000).unref();

server.listen(PORT, () => console.log('Hollow Ridge server listening on ' + PORT));

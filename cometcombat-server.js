#!/usr/bin/env node
/*
 * CometCombat matchmaking + relay server  (v0.9.2)
 *
 *   node cometcombat-server.js            # listens on :8080 (or $PORT)
 *
 * - Serves cometbound-games.html at "/" so the game and the server share an origin.
 * - Speaks WebSocket (RFC 6455) using only Node built-ins - no npm install needed.
 * - Pairs waiting players 1v1 and relays their messages. The first player of a pair
 *   is the "host" (runs the authoritative simulation in their browser).
 * - Tells the survivor when a peer disconnects, and cleans up queues on disconnect.
 *
 * For internet play put it behind HTTPS (e.g. a reverse proxy) so the page uses wss://.
 */
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = +process.env.PORT || 8080;
const HTML = path.join(__dirname, 'cometbound-games.html');
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_MSG = 64 * 1024, MAX_CLIENTS = 2000, MAX_RATE = 240;   // messages per second

const clients = new Set();
let waiting = [];

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'content-type': 'application/json' }); res.end(JSON.stringify({ ok: true, online: clients.size, waiting: waiting.length })); return; }
  if (url === '/' || url === '/index.html') {
    fs.readFile(HTML, (err, data) => {
      if (err) { res.writeHead(404); res.end('cometbound-games.html not found next to the server'); return; }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
      res.end(data);
    });
    return;
  }
  res.writeHead(404); res.end('Not found');
});

// ---------------------------------------------------------------- minimal WebSocket
function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}

class Client {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.frag = []; this.fragOp = 0;
    this.state = 'new'; this.name = 'Player'; this.peer = null; this.alive = true;
    this.rateT = Date.now(); this.rateN = 0; this.lastPong = Date.now();
    socket.on('data', d => this.onData(d));
    socket.on('close', () => this.close());
    socket.on('error', () => this.close());
    socket.setNoDelay(true);
  }
  send(obj) { if (this.alive) { try { this.socket.write(frame(0x1, Buffer.from(JSON.stringify(obj)))); } catch (e) { /* ignore */ } } }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > MAX_MSG * 2) return this.kill();
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX_MSG)) return this.kill(); len = Number(big); off = 10; }
      if (!masked) return this.kill();                       // clients must mask
      if (len > MAX_MSG) return this.kill();
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), data = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
      this.buf = b.subarray(off + 4 + len);
      if (op === 0x8) { this.close(); return; }
      if (op === 0x9) { this.socket.write(frame(0xA, data)); continue; }
      if (op === 0xA) { this.lastPong = Date.now(); continue; }
      if (op === 0x1 || op === 0x2) { this.fragOp = op; this.frag = [data]; } else if (op === 0x0) this.frag.push(data); else continue;
      if (fin) { const msg = Buffer.concat(this.frag); this.frag = []; if (this.fragOp === 0x1) this.onText(msg.toString('utf8')); }
    }
  }
  onText(text) {
    const now = Date.now();
    if (now - this.rateT > 1000) { this.rateT = now; this.rateN = 0; }
    if (++this.rateN > MAX_RATE) return this.kill();
    let m; try { m = JSON.parse(text); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'join' && this.state === 'new') {
      this.name = String(m.name || 'Player').replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, 16) || 'Player';
      this.state = 'waiting'; waiting.push(this); pair();
    } else if (m.t === 'msg' && this.state === 'matched' && this.peer && m.d && typeof m.d === 'object') {
      this.peer.send({ t: 'msg', d: m.d });
    }
  }
  kill() { try { this.socket.destroy(); } catch (e) { /* ignore */ } this.close(); }
  close() {
    if (!this.alive) return;
    this.alive = false; clients.delete(this);
    waiting = waiting.filter(c => c !== this);
    if (this.peer && this.peer.alive) { this.peer.send({ t: 'peer-left' }); this.peer.peer = null; }
    this.peer = null;
    try { this.socket.end(); } catch (e) { /* ignore */ }
  }
}

function pair() {
  waiting = waiting.filter(c => c.alive);
  while (waiting.length >= 2) {
    const a = waiting.shift(), b = waiting.shift();
    a.peer = b; b.peer = a; a.state = b.state = 'matched';
    a.send({ t: 'matched', role: 'host', opp: b.name });
    b.send({ t: 'matched', role: 'guest', opp: a.name });
    console.log(`[match] ${a.name} vs ${b.name}`);
  }
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket' || clients.size >= MAX_CLIENTS) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  clients.add(new Client(socket));
});

setInterval(() => {                                           // lobby info + liveness
  const now = Date.now();
  for (const c of clients) {
    if (now - c.lastPong > 45000) { c.kill(); continue; }
    try { c.socket.write(frame(0x9, Buffer.alloc(0))); } catch (e) { /* ignore */ }
    if (c.state === 'waiting') c.send({ t: 'lobby', online: clients.size, waiting: waiting.length });
  }
}, 2000);

server.listen(PORT, () => console.log(`CometCombat server on http://localhost:${PORT}  (WebSocket on the same port)`));

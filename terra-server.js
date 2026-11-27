// Terra multiplayer server. Needs Node.js only (no extra packages).
//   1. Keep this file in the same folder as terra-game.html
//   2. Run:  node terra-server.js
//   3. Open: http://localhost:8080
// Settings (optional environment variables): PORT, REGION, MAX_PLAYERS
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 8080;
const REGION = String(process.env.REGION || 'My server').slice(0, 30);
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 12;
const SAVE = path.join(__dirname, 'terra-world.json');
const WORLD = 12742000, HEIGHT = 48, MAX_BLOCK = 9, MAX_EDITS = 300000;

let seed = crypto.randomInt(1e9), age = 0;
const edits = new Map();           // "x,y,z" -> block type
try {
  const saved = JSON.parse(fs.readFileSync(SAVE, 'utf8'));
  seed = saved.seed; age = saved.age || 0;
  for (const [x, y, z, t] of saved.edits) edits.set(x + ',' + y + ',' + z, t);
  console.log('Loaded the saved world (' + edits.size + ' changed blocks).');
} catch (error) {
  if (error.code !== 'ENOENT') { console.error('Could not read ' + SAVE + ': ' + error.message); process.exit(1); }
}
const started = Date.now() - age * 1000;
const worldAge = () => (Date.now() - started) / 1000;
const editList = () => [...edits].map(([key, t]) => [...key.split(',').map(Number), t]);
let changed = !fs.existsSync(SAVE);
function save() {
  if (!changed) return;
  fs.writeFileSync(SAVE + '.tmp', JSON.stringify({ seed, age: worldAge(), edits: editList() }));
  fs.renameSync(SAVE + '.tmp', SAVE);
  changed = false;
}
setInterval(save, 10000);
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { changed = true; save(); process.exit(0); });

const server = http.createServer((req, res) => {
  if (req.url.split('?')[0] !== '/') { res.writeHead(404); return res.end('Not found'); }
  fs.readFile(path.join(__dirname, 'terra-game.html'), (error, page) => {
    if (error) { res.writeHead(500); return res.end('Put terra-game.html in the same folder as terra-server.js'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(page);
  });
});

// ----- a small WebSocket implementation -----
const clients = new Set();
let nextId = 1;
function frame(opcode, data) {
  const n = data.length;
  let head;
  if (n < 126) head = Buffer.from([0x80 | opcode, n]);
  else if (n < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(n, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([head, data]);
}
function send(client, message) {
  if (client.socket.destroyed) return;
  // drop anyone too slow to keep up
  if (client.socket.writableLength > 20e6) return client.socket.destroy();
  client.socket.write(frame(1, Buffer.from(JSON.stringify(message))));
}
const players = () => [...clients].filter(c => c.name);
const broadcast = (message, except) => players().forEach(c => { if (c !== except) send(c, message); });

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || clients.size >= MAX_PLAYERS * 3) return socket.destroy();
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const client = { socket, id: nextId++, name: null, seen: Date.now() };
  clients.add(client);
  let buffer = Buffer.alloc(0), parts = [], partSize = 0;
  const leave = () => {
    if (!clients.delete(client)) return;
    if (client.name) { broadcast({ type: 'leave', id: client.id }); console.log(client.name + ' left.'); }
  };
  socket.on('close', leave);
  socket.on('error', () => socket.destroy());
  socket.on('data', chunk => {
    client.seen = Date.now();
    buffer = Buffer.concat([buffer, chunk]);
    while (buffer.length >= 2) {
      const fin = buffer[0] & 0x80, opcode = buffer[0] & 15, masked = buffer[1] & 0x80;
      let length = buffer[1] & 127, offset = 2;
      if (length === 126) { if (buffer.length < 4) return; length = buffer.readUInt16BE(2); offset = 4; }
      else if (length === 127) { if (buffer.length < 10) return; length = Number(buffer.readBigUInt64BE(2)); offset = 10; }
      if (!masked || length > 300000) return socket.destroy();
      if (buffer.length < offset + 4 + length) return;
      const mask = buffer.subarray(offset, offset + 4);
      const data = Buffer.from(buffer.subarray(offset + 4, offset + 4 + length));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i % 4];
      buffer = buffer.subarray(offset + 4 + length);
      if (opcode === 8) { socket.end(frame(8, Buffer.alloc(0))); return leave(); }
      if (opcode === 9) { socket.write(frame(10, data)); continue; }
      if (opcode > 2) continue;
      parts.push(data); partSize += data.length;
      if (partSize > 300000) return socket.destroy();
      if (!fin) continue;
      const text = Buffer.concat(parts).toString();
      parts = []; partSize = 0;
      try { receive(client, JSON.parse(text)); } catch (error) { return socket.destroy(); }
    }
  });
});

const number = (v, low, high) => typeof v === 'number' && v >= low && v <= high;
const validEdit = e => Array.isArray(e) && e.length === 4 && e.every(Number.isInteger) &&
  number(e[0], 0, WORLD - 1) && number(e[1], 0, HEIGHT - 1) && number(e[2], 0, WORLD - 1) && number(e[3], 0, MAX_BLOCK);

function receive(client, m) {
  if (m.type === 'ping') return send(client, { type: 'pong', region: REGION, players: players().length, max: MAX_PLAYERS });
  if (m.type === 'join' && !client.name) {
    if (players().length >= MAX_PLAYERS) return send(client, { type: 'full' });
    client.name = String(m.name || '').replace(/[^\p{L}\p{N} _-]/gu, '').trim().slice(0, 16) || 'Player ' + client.id;
    send(client, { type: 'welcome', id: client.id, name: client.name, region: REGION, seed, age: worldAge(), edits: editList(),
      players: players().filter(c => c !== client).map(c => ({ id: c.id, name: c.name, state: c.state })) });
    broadcast({ type: 'join', id: client.id, name: client.name }, client);
    return console.log(client.name + ' joined.');
  }
  if (!client.name) return;
  if (m.type === 'move') {
    const s = m.state;
    if (!Array.isArray(s) || s.length !== 4 || !number(s[0], 0, WORLD) || !number(s[1], -10, 200) || !number(s[2], 0, WORLD) || !number(s[3], -1e7, 1e7)) return;
    client.state = s;
    return broadcast({ type: 'move', id: client.id, state: s }, client);
  }
  if (m.type === 'edits' && Array.isArray(m.list) && m.list.length <= 8000) {
    const accepted = m.list.filter(e => validEdit(e) && (edits.size < MAX_EDITS || edits.has(e[0] + ',' + e[1] + ',' + e[2])));
    for (const [x, y, z, t] of accepted) edits.set(x + ',' + y + ',' + z, t);
    if (accepted.length) { changed = true; broadcast({ type: 'edits', list: accepted }, client); }
  }
}
// remove connections that have gone quiet (players send updates all the time while playing)
setInterval(() => clients.forEach(c => { if (Date.now() - c.seen > 90000) c.socket.destroy(); else if (!c.socket.destroyed) c.socket.write(frame(9, Buffer.alloc(0))); }), 20000);

server.listen(PORT, () => {
  console.log('Terra server "' + REGION + '" is running.');
  console.log('On this computer open:  http://localhost:' + PORT);
  for (const list of Object.values(require('os').networkInterfaces()))
    for (const a of list) if (a.family === 'IPv4' && !a.internal) console.log('Same Wi-Fi can try:     http://' + a.address + ':' + PORT);
});

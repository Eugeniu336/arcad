// Tablă Îndoită — server de joc.
// Servește jocul și leagă cei doi jucători prin WebSocket.
// Nu are dependențe: trebuie doar Node.js 18+.  Pornire:  node server.js
// Merge pe rețeaua locală (telefoanele intră pe IP-ul calculatorului) și pe un hosting (ex. Render).

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 8080;
const CODE_RE = /^[A-Z2-9]{6}$/;
const MAX_ROOMS = 500;
const MAX_MSG = 64 * 1024;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const page = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8')
  .replace('/*RELAY*/false', 'true');

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    return res.end(page);
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Nu există. Deschide pagina principală.');
});

/* ---------- WebSocket minimal (RFC 6455, doar text) ---------- */
function upgradeToWs(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return null; }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);

  const c = { open: true, alive: true, onmessage: null, onclose: null };
  let buf = Buffer.alloc(0), frags = [];

  function frame(op, data) {
    if (!c.open) return;
    const len = data.length;
    let head;
    if (len < 126) head = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
    try { socket.write(Buffer.concat([head, data])); } catch (e) {}
  }
  function kill() {
    if (!c.open) return;
    c.open = false;
    try { socket.end(); } catch (e) {}
    setTimeout(() => socket.destroy(), 500);
    if (c.onclose) c.onclose();
  }

  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 2) return;
      const fin = buf[0] & 0x80, op = buf[0] & 0x0f, masked = buf[1] & 0x80;
      let len = buf[1] & 0x7f, off = 2;
      if (len === 126) { if (buf.length < 4) return; len = buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (buf.length < 10) return; if (buf.readUInt32BE(2) !== 0) return kill(); len = buf.readUInt32BE(6); off = 10; }
      if (len > MAX_MSG || !masked) return kill();
      if (buf.length < off + 4 + len) return;
      const mask = buf.subarray(off, off + 4), payload = Buffer.alloc(len);
      for (let i = 0; i < len; i++) payload[i] = buf[off + 4 + i] ^ mask[i & 3];
      buf = buf.subarray(off + 4 + len);
      if (op === 0x8) { frame(0x8, Buffer.alloc(0)); return kill(); }
      if (op === 0x9) { frame(0xA, payload); continue; }
      if (op === 0xA) { c.alive = true; continue; }
      if (op === 0x1 || op === 0x0) {
        frags.push(payload);
        if (fin) { const text = Buffer.concat(frags).toString('utf8'); frags = []; if (c.onmessage) c.onmessage(text); }
        continue;
      }
      frags = []; // binar: ignorat
    }
  });
  socket.on('close', kill);
  socket.on('end', kill);
  socket.on('error', kill);

  c.send = text => frame(0x1, Buffer.from(text, 'utf8'));
  c.ping = () => frame(0x9, Buffer.alloc(0));
  c.close = kill;
  return c;
}

/* ---------- camere de joc: o gazdă + un invitat ---------- */
const rooms = new Map();   // cod -> { host, guest }
const clients = new Set();
const ctl = (c, obj) => { if (c && c.open) c.send(JSON.stringify(obj)); };

server.on('upgrade', (req, socket) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname !== '/ws') { socket.destroy(); return; }
  const code = String(u.searchParams.get('room') || '').toUpperCase();
  const role = u.searchParams.get('role');
  const c = upgradeToWs(req, socket);
  if (!c) return;
  clients.add(c);

  if (!CODE_RE.test(code) || (role !== 'host' && role !== 'guest')) { ctl(c, { k: '_err', e: 'bad' }); return c.close(); }

  let room = rooms.get(code);
  if (role === 'host') {
    if (room) { ctl(c, { k: '_err', e: 'taken' }); return c.close(); }
    if (rooms.size >= MAX_ROOMS) { ctl(c, { k: '_err', e: 'busy' }); return c.close(); }
    room = { host: c, guest: null };
    rooms.set(code, room);
    ctl(c, { k: '_ok' });
  } else {
    if (!room || !room.host) { ctl(c, { k: '_err', e: 'noroom' }); return c.close(); }
    if (room.guest) { ctl(c, { k: '_err', e: 'full' }); return c.close(); }
    room.guest = c;
    ctl(c, { k: '_ok' });
    ctl(room.host, { k: '_peer' });
    ctl(room.guest, { k: '_peer' });
  }

  c.onmessage = text => {
    const r = rooms.get(code); if (!r) return;
    const other = c === r.host ? r.guest : r.host;
    if (other && other.open) other.send(text);
  };
  c.onclose = () => {
    clients.delete(c);
    const r = rooms.get(code); if (!r) return;
    if (c === r.host) {
      ctl(r.guest, { k: '_left' });
      if (r.guest) r.guest.close();
      rooms.delete(code);
      console.log('Joc închis: ' + code);
    } else if (c === r.guest) {
      r.guest = null;
      ctl(r.host, { k: '_left' });
    }
  };
  console.log((role === 'host' ? 'Joc nou: ' : 'Invitat intrat în: ') + code);
});

// Închide conexiunile moarte (telefon blocat, Wi-Fi pierdut).
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { c.close(); continue; }
    c.alive = false;
    c.ping();
  }
}, 15000);

server.listen(PORT, '0.0.0.0', () => {
  const ips = [];
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) if ((a.family === 'IPv4' || a.family === 4) && !a.internal) ips.push(a.address);
  }
  console.log('\nTablă Îndoită rulează.\n');
  console.log('Pe acest calculator:   http://localhost:' + PORT);
  if (ips.length) {
    console.log('De pe telefoane, în aceeași rețea Wi-Fi, deschide:');
    for (const ip of ips) console.log('   http://' + ip + ':' + PORT);
  }
  console.log('\nLasă fereastra deschisă cât jucați. Oprire: Ctrl+C\n');
});

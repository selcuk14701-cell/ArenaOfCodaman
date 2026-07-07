'use strict';
/* =========================================================================
   KODAMAN ARENA - MULTIPLAYER SUNUCUSU
   Express (statik dosya + HTTP) + ws (WebSocket, gerçek zamanlı oyun verisi)
   Tek bir Render "Web Service" içinde çalışır.
   ========================================================================= */
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'data.json');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (req, res) => res.send('ok'));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

/* =========================================================================
   KALICI VERİ (basit dosya tabanlı - sunucu her yeniden başladığında okunur)
   NOT: Render ücretsiz planında instance uykuya dalıp yeniden başladığında
   veya yeniden deploy edildiğinde disk sıfırlanabilir. Gerçek kalıcılık için
   bir veritabanı (örn. Render Postgres) önerilir. Bu, prototip için yeterlidir.
   ========================================================================= */
let DB = { prestige: {}, friends: {}, usedNicknames: [] };
function loadDB() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      DB = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      DB.prestige = DB.prestige || {};
      DB.friends = DB.friends || {};
      DB.usedNicknames = DB.usedNicknames || [];
    }
  } catch (e) { console.error('DB yüklenemedi:', e.message); }
}
let saveTimeout = null;
function saveDB() {
  clearTimeout(saveTimeout);
  saveTimeout = setTimeout(() => {
    try { fs.writeFileSync(DATA_FILE, JSON.stringify(DB)); } catch (e) { console.error('DB kaydedilemedi:', e.message); }
  }, 500);
}
loadDB();

function nickKey(n) { return n.toLowerCase(); }
function isNickTaken(n) { return DB.usedNicknames.some(x => nickKey(x) === nickKey(n)); }
function registerNick(n) { DB.usedNicknames.push(n); if (!(nickKey(n) in DB.prestige)) DB.prestige[nickKey(n)] = 0; saveDB(); }
function getPrestige(n) { return DB.prestige[nickKey(n)] || 0; }
function addPrestige(n, delta) { DB.prestige[nickKey(n)] = getPrestige(n) + delta; saveDB(); }
function getFriends(n) { return DB.friends[nickKey(n)] || []; }
function addFriendTo(n, friendName) {
  const key = nickKey(n);
  const list = DB.friends[key] || [];
  if (!list.some(f => nickKey(f) === nickKey(friendName))) list.push(friendName);
  DB.friends[key] = list;
  saveDB();
}
function leaderboardTop(n) {
  return Object.entries(DB.prestige)
    .map(([key, prestige]) => {
      const original = DB.usedNicknames.find(x => nickKey(x) === key) || key;
      return { name: original, prestige };
    })
    .sort((a, b) => b.prestige - a.prestige)
    .slice(0, n);
}

/* =========================================================================
   OYUN SABİTLERİ (İSTEMCİ İLE AYNI OLMALI!)
   ========================================================================= */
const WORLD_W = 3200, WORLD_H = 2200;
const MATCH_SECONDS = 300;
const RESPAWN_SECONDS = 2.2;

const WEAPONS = {
  0: { key: 'sniper', name: 'Keskin Nişancı', damage: 100, fireRate: 1150, range: 2200, melee: false, angleTol: 0.06 },
  1: { key: 'ak47', name: 'AK47', damage: 13, fireRate: 110, range: 1100, melee: false, angleTol: 0.16 },
  2: { key: 'knife', name: 'Bıçak', damage: 100, fireRate: 480, range: 78, melee: true, angleTol: 1.05 }
};

const SPAWN_POINTS = [
  { x: 120, y: 120 }, { x: WORLD_W - 120, y: 120 }, { x: 120, y: WORLD_H - 120 }, { x: WORLD_W - 120, y: WORLD_H - 120 },
  { x: WORLD_W / 2, y: 120 }, { x: WORLD_W / 2, y: WORLD_H - 120 }, { x: 120, y: WORLD_H / 2 }, { x: WORLD_W - 120, y: WORLD_H / 2 }
];

function randSpawn() { return SPAWN_POINTS[Math.floor(Math.random() * SPAWN_POINTS.length)]; }
function normAngle(a) { while (a > Math.PI) a -= Math.PI * 2; while (a < -Math.PI) a += Math.PI * 2; return a; }
function dist(ax, ay, bx, by) { return Math.hypot(ax - bx, ay - by); }
function genCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let s = ''; for (let i = 0; i < 6; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

/* =========================================================================
   ODA (ROOM) YÖNETİMİ
   ========================================================================= */
const rooms = new Map();      // roomId -> room
let currentPublicRoomId = null;
const onlineNicknames = new Map(); // nickKey -> ws  (arkadaş çevrimiçi durumu için)

function createRoom(type, code) {
  const id = type === 'public' ? ('public_' + crypto.randomUUID()) : code;
  const room = {
    id, type, code: code || null,
    players: new Map(), // playerId -> playerState
    timeLeft: MATCH_SECONDS, ended: false, tickInterval: null
  };
  rooms.set(id, room);
  room.tickInterval = setInterval(() => roomTick(room), 1000);
  return room;
}

function getOrCreatePublicRoom() {
  let room = currentPublicRoomId && rooms.get(currentPublicRoomId);
  if (!room || room.ended) {
    room = createRoom('public', null);
    currentPublicRoomId = room.id;
  }
  return room;
}

function broadcast(room, msg, exceptPlayerId) {
  const data = JSON.stringify(msg);
  for (const [pid, p] of room.players) {
    if (pid === exceptPlayerId) continue;
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(data);
  }
}
function sendTo(player, msg) {
  if (player.ws.readyState === WebSocket.OPEN) player.ws.send(JSON.stringify(msg));
}

function roomTick(room) {
  if (room.ended) return;
  room.timeLeft -= 1;
  broadcast(room, { type: 'timer', timeLeft: room.timeLeft });

  // respawn kontrolü
  for (const [pid, p] of room.players) {
    if (!p.alive) {
      p.respawnLeft -= 1;
      if (p.respawnLeft <= 0) {
        const sp = randSpawn();
        p.x = sp.x + (Math.random() * 60 - 30);
        p.y = sp.y + (Math.random() * 60 - 30);
        p.hp = 100; p.alive = true;
        broadcast(room, { type: 'respawn', id: pid, x: p.x, y: p.y });
      }
    }
  }

  if (room.timeLeft <= 0) endRoom(room);
}

function endRoom(room) {
  if (room.ended) return;
  room.ended = true;
  clearInterval(room.tickInterval);

  const ranking = Array.from(room.players.values())
    .sort((a, b) => b.kills - a.kills || a.deaths - b.deaths)
    .map(p => ({ id: p.id, name: p.name, kills: p.kills, deaths: p.deaths }));

  const prestigeGains = {};
  if (room.type === 'public') {
    const gainsByRank = [3, 2, 1];
    ranking.forEach((r, i) => {
      if (i < 3) {
        addPrestige(r.name, gainsByRank[i]);
        prestigeGains[r.id] = gainsByRank[i];
      }
    });
  }

  broadcast(room, {
    type: 'match_end',
    roomType: room.type,
    ranking,
    prestigeGains,
    newPrestige: Object.fromEntries(ranking.map(r => [r.id, getPrestige(r.name)]))
  });

  setTimeout(() => rooms.delete(room.id), 15000);
  if (room.id === currentPublicRoomId) currentPublicRoomId = null;
}

function findHitTarget(room, shooterId, weapon, ox, oy, angle) {
  let best = null, bestDist = Infinity;
  for (const [pid, p] of room.players) {
    if (pid === shooterId || !p.alive) continue;
    const d = dist(ox, oy, p.x, p.y);
    if (d > weapon.range + 20) continue;
    const angTo = Math.atan2(p.y - oy, p.x - ox);
    const diff = Math.abs(normAngle(angTo - angle));
    const radiusTol = Math.atan2(20, Math.max(d, 1));
    if (diff <= weapon.angleTol + radiusTol) {
      if (d < bestDist) { bestDist = d; best = p; }
    }
  }
  return best;
}

function playerSummary(p) {
  return { id: p.id, name: p.name, x: p.x, y: p.y, angle: p.angle, hp: p.hp, weaponIndex: p.weaponIndex, kills: p.kills, deaths: p.deaths, alive: p.alive };
}

/* =========================================================================
   WEBSOCKET BAĞLANTI YÖNETİMİ
   ========================================================================= */
wss.on('connection', (ws) => {
  ws.playerId = crypto.randomUUID();
  ws.nickname = null;
  ws.roomId = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    handleMessage(ws, msg);
  });

  ws.on('close', () => {
    if (ws.nickname) onlineNicknames.delete(nickKey(ws.nickname));
    const room = ws.roomId && rooms.get(ws.roomId);
    if (room && room.players.has(ws.playerId)) {
      room.players.delete(ws.playerId);
      broadcast(room, { type: 'player_left', id: ws.playerId });
      if (room.players.size === 0 && !room.ended) {
        clearInterval(room.tickInterval);
        rooms.delete(room.id);
        if (room.id === currentPublicRoomId) currentPublicRoomId = null;
      }
    }
  });
});

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'register': return onRegister(ws, msg);
    case 'friend_add': return onFriendAdd(ws, msg);
    case 'friend_list_request': return onFriendList(ws);
    case 'leaderboard_request': return onLeaderboard(ws);
    case 'join_public': return onJoinPublic(ws);
    case 'create_private': return onCreatePrivate(ws);
    case 'join_private': return onJoinPrivate(ws, msg);
    case 'state': return onState(ws, msg);
    case 'shoot': return onShoot(ws, msg);
    case 'melee': return onMelee(ws, msg);
    case 'leave_room': return onLeaveRoom(ws);
  }
}

function onRegister(ws, msg) {
  const name = String(msg.nickname || '').trim();
  if (name.length < 3 || name.length > 16 || !/^[A-Za-z0-9_ığüşöçİĞÜŞÖÇ]+$/.test(name)) {
    return sendTo({ ws }, { type: 'register_fail', reason: 'İsim 3-16 karakter, harf/rakam/alt çizgi olmalı.' });
  }
  if (isNickTaken(name)) {
    return sendTo({ ws }, { type: 'register_fail', reason: 'Bu isim zaten alınmış! Başka bir isim dene.' });
  }
  registerNick(name);
  ws.nickname = name;
  onlineNicknames.set(nickKey(name), ws);
  sendTo({ ws }, { type: 'register_ok', nickname: name, prestige: getPrestige(name) });
}

function onFriendAdd(ws, msg) {
  if (!ws.nickname) return;
  const target = String(msg.target || '').trim();
  if (!target || nickKey(target) === nickKey(ws.nickname)) {
    return sendTo({ ws }, { type: 'friend_add_fail', reason: 'Geçersiz isim.' });
  }
  if (!isNickTaken(target)) {
    return sendTo({ ws }, { type: 'friend_add_fail', reason: 'Bu isimde bir oyuncu bulunamadı.' });
  }
  addFriendTo(ws.nickname, target);
  onFriendList(ws);
}

function onFriendList(ws) {
  if (!ws.nickname) return;
  const friends = getFriends(ws.nickname).map(f => ({
    name: f, online: onlineNicknames.has(nickKey(f)), prestige: getPrestige(f)
  }));
  sendTo({ ws }, { type: 'friend_list', friends });
}

function onLeaderboard(ws) {
  sendTo({ ws }, { type: 'leaderboard', list: leaderboardTop(50) });
}

function joinRoomCommon(ws, room) {
  ws.roomId = room.id;
  const sp = randSpawn();
  const player = {
    id: ws.playerId, ws, name: ws.nickname,
    x: sp.x + (Math.random() * 60 - 30), y: sp.y + (Math.random() * 60 - 30),
    angle: 0, hp: 100, weaponIndex: 1, kills: 0, deaths: 0, alive: true,
    respawnLeft: 0, lastShot: {}
  };
  room.players.set(ws.playerId, player);

  sendTo(player, {
    type: 'room_joined',
    roomType: room.type, code: room.code, selfId: player.id,
    timeLeft: room.timeLeft, worldW: WORLD_W, worldH: WORLD_H,
    players: Array.from(room.players.values()).map(playerSummary)
  });
  broadcast(room, { type: 'player_joined', player: playerSummary(player) }, player.id);
}

function onJoinPublic(ws) {
  if (!ws.nickname) return;
  const room = getOrCreatePublicRoom();
  joinRoomCommon(ws, room);
}
function onCreatePrivate(ws) {
  if (!ws.nickname) return;
  let code;
  do { code = genCode(); } while (rooms.has(code));
  const room = createRoom('private', code);
  joinRoomCommon(ws, room);
}
function onJoinPrivate(ws, msg) {
  if (!ws.nickname) return;
  const code = String(msg.code || '').trim().toUpperCase();
  const room = rooms.get(code);
  if (!room || room.ended) {
    return sendTo({ ws }, { type: 'error', message: 'Bu kodla bir oda bulunamadı.' });
  }
  joinRoomCommon(ws, room);
}
function onLeaveRoom(ws) {
  const room = ws.roomId && rooms.get(ws.roomId);
  if (!room) return;
  room.players.delete(ws.playerId);
  broadcast(room, { type: 'player_left', id: ws.playerId });
  ws.roomId = null;
}

function onState(ws, msg) {
  const room = ws.roomId && rooms.get(ws.roomId);
  if (!room || room.ended) return;
  const p = room.players.get(ws.playerId);
  if (!p || !p.alive) return;
  p.x = clampNum(msg.x, 0, WORLD_W);
  p.y = clampNum(msg.y, 0, WORLD_H);
  p.angle = Number(msg.angle) || 0;
  if (typeof msg.weaponIndex === 'number') p.weaponIndex = msg.weaponIndex;
  broadcast(room, { type: 'state_update', id: p.id, x: p.x, y: p.y, angle: p.angle, weaponIndex: p.weaponIndex }, p.id);
}
function clampNum(v, a, b) { v = Number(v) || 0; return Math.max(a, Math.min(b, v)); }

function onShoot(ws, msg) {
  const room = ws.roomId && rooms.get(ws.roomId);
  if (!room || room.ended) return;
  const shooter = room.players.get(ws.playerId);
  if (!shooter || !shooter.alive) return;
  const weapon = WEAPONS[msg.weaponIndex];
  if (!weapon || weapon.melee) return;

  const now = Date.now();
  const last = shooter.lastShot[msg.weaponIndex] || 0;
  if (now - last < weapon.fireRate * 0.85) return; // basit sunucu taraflı hız sınırlaması
  shooter.lastShot[msg.weaponIndex] = now;

  const ox = Number(msg.x), oy = Number(msg.y), angle = Number(msg.angle);
  broadcast(room, { type: 'bullet', ownerId: shooter.id, x: ox, y: oy, angle, weaponIndex: msg.weaponIndex });

  const target = findHitTarget(room, shooter.id, weapon, ox, oy, angle);
  if (target) applyDamage(room, shooter, target, weapon);
}

function onMelee(ws, msg) {
  const room = ws.roomId && rooms.get(ws.roomId);
  if (!room || room.ended) return;
  const shooter = room.players.get(ws.playerId);
  if (!shooter || !shooter.alive) return;
  const weapon = WEAPONS[2];
  const now = Date.now();
  const last = shooter.lastShot[2] || 0;
  if (now - last < weapon.fireRate * 0.85) return;
  shooter.lastShot[2] = now;

  const target = findHitTarget(room, shooter.id, weapon, shooter.x, shooter.y, shooter.angle);
  if (target) applyDamage(room, shooter, target, weapon);
}

function applyDamage(room, shooter, target, weapon) {
  target.hp -= weapon.damage;
  broadcast(room, { type: 'hit', targetId: target.id, hp: Math.max(target.hp, 0), attackerId: shooter.id, weaponName: weapon.name });
  if (target.hp <= 0) {
    target.alive = false; target.deaths += 1; target.respawnLeft = RESPAWN_SECONDS;
    shooter.kills += 1;
    broadcast(room, {
      type: 'kill', killerId: shooter.id, killerName: shooter.name,
      targetId: target.id, targetName: target.name, weaponName: weapon.name
    });
  }
}

/* =========================================================================
   BAŞLAT
   ========================================================================= */
server.listen(PORT, () => {
  console.log(`Kodaman Arena sunucusu ${PORT} portunda çalışıyor.`);
});

const WebSocket = require('ws');
const http = require('http');
const url = require('url');
const { MongoClient } = require('mongodb');
const crypto = require('crypto');

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const MONGO_URI = process.env.MONGO_URI;
const PORT = process.env.PORT || 8080;
const SESSION_SECRET = process.env.SESSION_SECRET || 'gladiator_secret_change_me';

// ─── DB ───────────────────────────────────────────────────────────────────────
let db = null;

async function connectDB() {
  try {
    const client = await MongoClient.connect(MONGO_URI);
    db = client.db('gladiator_db');
    // Ensure unique index on username
    await db.collection('users').createIndex({ username: 1 }, { unique: true });
    console.log('MongoDB connected → gladiator_db');
  } catch (e) {
    console.error('MongoDB connection failed:', e);
  }
}
connectDB();

// ─── HELPERS ─────────────────────────────────────────────────────────────────
function generateId() {
  return crypto.randomBytes(8).toString('hex');
}

function hashPassword(password) {
  return crypto.createHmac('sha256', SESSION_SECRET).update(password).digest('hex');
}

function generateSessionToken() {
  return crypto.randomBytes(32).toString('hex');
}

function log(type, data) {
  const skip = ['HEARTBEAT', 'POSITION'];
  if (skip.includes(type)) return;
  console.log(`[${new Date().toISOString()}] ${type}:`, JSON.stringify(data));
}

// ─── SERVER SETUP ─────────────────────────────────────────────────────────────
const server = http.createServer();
const wss = new WebSocket.Server({ server });

// clientId → { ws, userId, username, elo, room, lastHeartbeat, inQueue, inMatch }
const clients = new Map();
// roomName → Set of clientIds
const rooms = new Map();
// sessionToken → userId  (in-memory session store)
const sessions = new Map();

// ─── BROADCAST ───────────────────────────────────────────────────────────────
function broadcast(room, message, excludeId = null) {
  if (!rooms.has(room)) return;
  rooms.get(room).forEach(clientId => {
    if (clientId === excludeId) return;
    const c = clients.get(clientId);
    if (c && c.ws.readyState === WebSocket.OPEN) {
      c.ws.send(JSON.stringify(message));
    }
  });
}

function sendTo(clientId, message) {
  const c = clients.get(clientId);
  if (c && c.ws.readyState === WebSocket.OPEN) {
    c.ws.send(JSON.stringify(message));
  }
}

// ─── WEBSOCKET ────────────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const clientId = generateId();
  clients.set(clientId, {
    ws,
    id: clientId,
    userId: null,
    username: null,
    elo: null,
    room: null,
    lastHeartbeat: Date.now(),
    inQueue: false,
    inMatch: false
  });

  ws.send(JSON.stringify({ type: 'connected', clientId }));
  log('CONNECT', { clientId });

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw);
      handleMessage(clientId, data);
    } catch (e) {
      sendTo(clientId, { type: 'error', message: 'Invalid JSON' });
    }
  });

  ws.on('close', () => handleDisconnect(clientId));
  ws.on('error', (e) => log('WS_ERROR', { clientId, error: e.message }));
});

// ─── MESSAGE ROUTER ───────────────────────────────────────────────────────────
function handleMessage(clientId, data) {
  switch (data.type) {
    case 'register':       return handleRegister(clientId, data);
    case 'login':          return handleLogin(clientId, data);
    case 'resume_session': return handleResumeSession(clientId, data);
    case 'logout':         return handleLogout(clientId, data);
    case 'join_room':      return handleJoinRoom(clientId, data);
    case 'leave_room':     return handleLeaveRoom(clientId);
    case 'heartbeat':      return handleHeartbeat(clientId, data);
    case 'get_players':    return handleGetPlayers(clientId);
    case 'get_queue_count':return handleGetQueueCount(clientId);
    case 'get_leaderboard':return handleGetLeaderboard(clientId);
    case 'player_action':  return handlePlayerAction(clientId, data);
    case 'update_elo':     return handleUpdateElo(clientId, data);
    default:
      sendTo(clientId, { type: 'error', message: `Unknown type: ${data.type}` });
  }
}

// ─── AUTH ─────────────────────────────────────────────────────────────────────
async function handleRegister(clientId, data) {
  const { username, password } = data;

  if (!username || !password) {
    return sendTo(clientId, { type: 'register_result', success: false, message: 'Username and password required' });
  }
  if (username.length < 3 || username.length > 20) {
    return sendTo(clientId, { type: 'register_result', success: false, message: 'Username must be 3–20 characters' });
  }
  if (!/^[a-zA-Z0-9_]+$/.test(username)) {
    return sendTo(clientId, { type: 'register_result', success: false, message: 'Username: letters, numbers, underscores only' });
  }
  if (password.length < 4) {
    return sendTo(clientId, { type: 'register_result', success: false, message: 'Password must be at least 4 characters' });
  }
  if (!db) return sendTo(clientId, { type: 'register_result', success: false, message: 'Database unavailable' });

  try {
    const userId = generateId();
    const token = generateSessionToken();
    await db.collection('users').insertOne({
      userId,
      username,
      password: hashPassword(password),
      elo: 1000,
      wins: 0,
      losses: 0,
      createdAt: Date.now()
    });

    sessions.set(token, userId);

    const client = clients.get(clientId);
    client.userId = userId;
    client.username = username;
    client.elo = 1000;

    log('REGISTER', { username, userId });
    sendTo(clientId, {
      type: 'register_result',
      success: true,
      token,
      username,
      elo: 1000
    });
  } catch (e) {
    if (e.code === 11000) {
      sendTo(clientId, { type: 'register_result', success: false, message: 'Username already taken' });
    } else {
      log('REGISTER_ERROR', { error: e.message });
      sendTo(clientId, { type: 'register_result', success: false, message: 'Server error' });
    }
  }
}

async function handleLogin(clientId, data) {
  const { username, password } = data;
  if (!username || !password) {
    return sendTo(clientId, { type: 'login_result', success: false, message: 'Username and password required' });
  }
  if (!db) return sendTo(clientId, { type: 'login_result', success: false, message: 'Database unavailable' });

  try {
    const user = await db.collection('users').findOne({ username });
    if (!user || user.password !== hashPassword(password)) {
      return sendTo(clientId, { type: 'login_result', success: false, message: 'Invalid username or password' });
    }

    const token = generateSessionToken();
    sessions.set(token, user.userId);

    const client = clients.get(clientId);
    client.userId = user.userId;
    client.username = user.username;
    client.elo = user.elo;

    log('LOGIN', { username, userId: user.userId });
    sendTo(clientId, {
      type: 'login_result',
      success: true,
      token,
      username: user.username,
      elo: user.elo,
      wins: user.wins || 0,
      losses: user.losses || 0
    });
  } catch (e) {
    log('LOGIN_ERROR', { error: e.message });
    sendTo(clientId, { type: 'login_result', success: false, message: 'Server error' });
  }
}

async function handleResumeSession(clientId, data) {
  const { token } = data;
  if (!token) return sendTo(clientId, { type: 'session_result', success: false });
  if (!db) return sendTo(clientId, { type: 'session_result', success: false });

  const userId = sessions.get(token);
  if (!userId) return sendTo(clientId, { type: 'session_result', success: false, message: 'Session expired' });

  try {
    const user = await db.collection('users').findOne({ userId });
    if (!user) {
      sessions.delete(token);
      return sendTo(clientId, { type: 'session_result', success: false, message: 'User not found' });
    }

    const client = clients.get(clientId);
    client.userId = user.userId;
    client.username = user.username;
    client.elo = user.elo;

    sendTo(clientId, {
      type: 'session_result',
      success: true,
      username: user.username,
      elo: user.elo,
      wins: user.wins || 0,
      losses: user.losses || 0
    });
  } catch (e) {
    sendTo(clientId, { type: 'session_result', success: false });
  }
}

function handleLogout(clientId, data) {
  const { token } = data;
  if (token) sessions.delete(token);
  const client = clients.get(clientId);
  if (client) {
    handleLeaveRoom(clientId);
    client.userId = null;
    client.username = null;
    client.elo = null;
  }
  sendTo(clientId, { type: 'logout_result', success: true });
}

// ─── ROOM ─────────────────────────────────────────────────────────────────────
function handleJoinRoom(clientId, data) {
  const client = clients.get(clientId);
  if (!client || !client.username) {
    return sendTo(clientId, { type: 'error', message: 'Must be logged in to join a room' });
  }

  const room = data.room || 'gladiator_arena';
  if (client.room) handleLeaveRoom(clientId);

  client.room = room;
  if (!rooms.has(room)) rooms.set(room, new Set());
  rooms.get(room).add(clientId);

  const players = getRoomPlayers(room);
  sendTo(clientId, { type: 'joined', room, players });
  broadcast(room, { type: 'player_joined', player: { id: clientId, username: client.username, elo: client.elo } }, clientId);
  broadcast(room, { type: 'players_update', players });

  log('JOIN_ROOM', { clientId, username: client.username, room });
}

function handleLeaveRoom(clientId) {
  const client = clients.get(clientId);
  if (!client || !client.room) return;
  const room = client.room;

  if (rooms.has(room)) {
    rooms.get(room).delete(clientId);
    if (rooms.get(room).size === 0) {
      rooms.delete(room);
    } else {
      broadcast(room, { type: 'player_left', player: { id: clientId, username: client.username } });
      broadcast(room, { type: 'players_update', players: getRoomPlayers(room) });
    }
  }
  client.room = null;
  client.inQueue = false;
}

function getRoomPlayers(room) {
  if (!rooms.has(room)) return [];
  return Array.from(rooms.get(room)).map(id => {
    const c = clients.get(id);
    return { id: c.id, username: c.username, elo: c.elo };
  });
}

// ─── HEARTBEAT ────────────────────────────────────────────────────────────────
function handleHeartbeat(clientId, data) {
  const client = clients.get(clientId);
  if (!client) return;
  client.lastHeartbeat = Date.now();
  if (typeof data.inQueue !== 'undefined') client.inQueue = data.inQueue;
  if (typeof data.inMatch !== 'undefined') client.inMatch = data.inMatch;
}

// ─── LOBBY INFO ───────────────────────────────────────────────────────────────
function handleGetPlayers(clientId) {
  const client = clients.get(clientId);
  if (!client || !client.room) return;
  sendTo(clientId, { type: 'players_update', players: getRoomPlayers(client.room) });
}

function handleGetQueueCount(clientId) {
  const client = clients.get(clientId);
  if (!client || !client.room) return;
  let count = 0;
  if (rooms.has(client.room)) {
    rooms.get(client.room).forEach(id => {
      const c = clients.get(id);
      if (c && c.inQueue) count++;
    });
  }
  sendTo(clientId, { type: 'queue_count', count });
}

async function handleGetLeaderboard(clientId) {
  if (!db) return;
  try {
    const top = await db.collection('users')
      .find({}, { projection: { username: 1, elo: 1, wins: 1, losses: 1 } })
      .sort({ elo: -1 })
      .limit(50)
      .toArray();
    sendTo(clientId, { type: 'leaderboard_data', leaderboard: top });
  } catch (e) {
    log('LEADERBOARD_ERROR', { error: e.message });
  }
}

// ─── GAME ACTIONS ─────────────────────────────────────────────────────────────
function handlePlayerAction(clientId, data) {
  const client = clients.get(clientId);
  if (!client || !client.room) return;

  // Relay to everyone else in the room
  broadcast(client.room, {
    type: 'player_action',
    playerId: clientId,
    action: data.action,
    actionData: data.data
  }, clientId);
}

// ─── ELO UPDATE ───────────────────────────────────────────────────────────────
async function handleUpdateElo(clientId, data) {
  const client = clients.get(clientId);
  if (!client || !client.userId || !db) return;

  const { elo, won } = data;
  if (typeof elo !== 'number' || elo < 0) return;

  try {
    const update = { elo, lastMatch: Date.now() };
    if (won === true)  update.$inc = { wins: 1 };
    if (won === false) update.$inc = { losses: 1 };

    // Use separate inc to avoid overwrite conflict
    const setData = { elo, lastMatch: Date.now() };
    const incData = {};
    if (won === true)  incData.wins = 1;
    if (won === false) incData.losses = 1;

    const mongoUpdate = { $set: setData };
    if (Object.keys(incData).length) mongoUpdate.$inc = incData;

    await db.collection('users').updateOne({ userId: client.userId }, mongoUpdate);
    client.elo = elo;

    log('ELO_UPDATE', { username: client.username, elo, won });
    sendTo(clientId, { type: 'elo_updated', elo });
  } catch (e) {
    log('ELO_UPDATE_ERROR', { error: e.message });
  }
}

// ─── DISCONNECT ───────────────────────────────────────────────────────────────
function handleDisconnect(clientId) {
  const client = clients.get(clientId);
  if (!client) return;
  log('DISCONNECT', { clientId, username: client.username });
  handleLeaveRoom(clientId);
  clients.delete(clientId);
}

// ─── CLEANUP INACTIVE ─────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [clientId, client] of clients.entries()) {
    if (client.room && now - client.lastHeartbeat > 30000) {
      log('TIMEOUT', { clientId, username: client.username });
      handleDisconnect(clientId);
    }
  }
}, 10000);

// ─── HTTP STATUS ──────────────────────────────────────────────────────────────
server.on('request', (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.statusCode = 200;
  res.end(JSON.stringify({
    status: 'online',
    connections: clients.size,
    rooms: rooms.size,
    uptime: Math.floor(process.uptime())
  }));
});

server.listen(PORT, () => {
  console.log(`Gladiator Arena server on port ${PORT}`);
});

process.on('SIGTERM', () => {
  wss.close(() => server.close(() => process.exit(0)));
});

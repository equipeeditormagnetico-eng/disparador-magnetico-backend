import express from 'express';
import cors from 'cors';
import cron from 'node-cron';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'db.json');

app.use(cors());
app.use(express.json());

// ── DB helpers ──────────────────────────────────────────────
function loadDB() {
  if (!fs.existsSync(DB_FILE)) return defaultDB();
  try { return JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); }
  catch { return defaultDB(); }
}

function saveDB(data) {
  fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
}

function defaultDB() {
  return {
    config: {
      token: '', instanceId: '', clientToken: '',
      intervalMin: 30, intervalMax: 60,
      blockSize: 20, blockPause: 15,
      maxPerDay: 150
    },
    contacts: [],
    messages: ['', '', '', '', ''],
    schedule: {},
    dispatch: {
      running: false, paused: false,
      currentIndex: 0, sentToday: 0,
      lastDate: '', log: [], history: []
    }
  };
}

// ── Dispatch state ──────────────────────────────────────────
let dispatchTimer = null;
let countdownValue = 0;

function resetDailyIfNeeded(db) {
  const today = new Date().toISOString().split('T')[0];
  if (db.dispatch.lastDate !== today) {
    db.dispatch.sentToday = 0;
    db.dispatch.lastDate = today;
  }
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function sendMessage(config, phone, message) {
  const url = `https://api.z-api.io/instances/${config.instanceId}/token/${config.token}/send-text`;
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Client-Token': config.clientToken
    },
    body: JSON.stringify({ phone, message })
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`HTTP ${res.status}: ${err}`);
  }
  return await res.json();
}

async function runDispatch() {
  const db = loadDB();
  resetDailyIfNeeded(db);

  if (!db.dispatch.running || db.dispatch.paused) return;

  const { config, contacts, messages, dispatch } = db;
  const validMessages = messages.filter(m => m.trim());

  if (dispatch.currentIndex >= contacts.length) {
    // Finished
    dispatch.running = false;
    dispatch.paused = false;
    dispatch.history.unshift({
      date: new Date().toISOString(),
      total: contacts.length,
      sent: dispatch.log.filter(l => l.status === 'success').length,
      errors: dispatch.log.filter(l => l.status === 'error').length
    });
    if (dispatch.history.length > 50) dispatch.history = dispatch.history.slice(0, 50);
    saveDB(db);
    return;
  }

  if (dispatch.sentToday >= config.maxPerDay) {
    dispatch.running = false;
    dispatch.paused = false;
    const entry = { time: new Date().toISOString(), info: `Limite diario de ${config.maxPerDay} mensagens atingido.` };
    dispatch.log.unshift(entry);
    saveDB(db);
    return;
  }

  const contact = contacts[dispatch.currentIndex];
  const msgTemplate = validMessages[Math.floor(Math.random() * validMessages.length)];
  const message = msgTemplate.replace(/{nome}/gi, contact.nome || '');
  const phone = contact.numero;

  let status = 'success';
  let error = null;

  try {
    await sendMessage(config, phone, message);
    dispatch.sentToday++;
  } catch (e) {
    status = 'error';
    error = e.message;
  }

  dispatch.log.unshift({
    time: new Date().toISOString(),
    nome: contact.nome,
    numero: phone,
    message: msgTemplate,
    status,
    error
  });
  if (dispatch.log.length > 500) dispatch.log = dispatch.log.slice(0, 500);

  dispatch.currentIndex++;

  // Block pause check
  const blockSize = config.blockSize || 20;
  const blockPause = config.blockPause || 15;
  if (dispatch.currentIndex % blockSize === 0 && dispatch.currentIndex < contacts.length) {
    dispatch.paused = true;
    dispatch.pauseReason = 'block';
    saveDB(db);
    const pauseMs = blockPause * 60 * 1000;
    setTimeout(() => {
      const db2 = loadDB();
      if (db2.dispatch.pauseReason === 'block') {
        db2.dispatch.paused = false;
        db2.dispatch.pauseReason = null;
        saveDB(db2);
        scheduleNext();
      }
    }, pauseMs);
    return;
  }

  saveDB(db);
  scheduleNext();
}

function scheduleNext() {
  const db = loadDB();
  if (!db.dispatch.running || db.dispatch.paused) return;
  const delay = randomInt(db.config.intervalMin, db.config.intervalMax) * 1000;
  countdownValue = Math.ceil(delay / 1000);
  const tick = setInterval(() => { if (countdownValue > 0) countdownValue--; }, 1000);
  dispatchTimer = setTimeout(() => {
    clearInterval(tick);
    runDispatch();
  }, delay);
}

// ── Routes ──────────────────────────────────────────────────
app.get('/', (_, res) => res.json({ status: 'O Disparador Magnetico backend online!' }));

app.post('/api/config', (req, res) => {
  const db = loadDB();
  db.config = { ...db.config, ...req.body };
  saveDB(db);
  res.json({ ok: true });
});

app.get('/api/config', (req, res) => {
  const db = loadDB();
  res.json(db.config);
});

app.post('/api/contacts', (req, res) => {
  const db = loadDB();
  db.contacts = req.body.contacts || [];
  saveDB(db);
  res.json({ ok: true, total: db.contacts.length });
});

app.post('/api/messages', (req, res) => {
  const db = loadDB();
  db.messages = req.body.messages || [];
  saveDB(db);
  res.json({ ok: true });
});

app.post('/api/schedule', (req, res) => {
  const db = loadDB();
  db.schedule = req.body.schedule || {};
  saveDB(db);
  res.json({ ok: true });
});

app.post('/api/dispatch/start', (req, res) => {
  const db = loadDB();
  resetDailyIfNeeded(db);
  db.dispatch.running = true;
  db.dispatch.paused = false;
  db.dispatch.currentIndex = 0;
  db.dispatch.log = [];
  saveDB(db);
  scheduleNext();
  res.json({ ok: true });
});

app.post('/api/dispatch/pause', (req, res) => {
  const db = loadDB();
  db.dispatch.paused = true;
  db.dispatch.pauseReason = 'manual';
  if (dispatchTimer) { clearTimeout(dispatchTimer); dispatchTimer = null; }
  saveDB(db);
  res.json({ ok: true, pausedAt: db.dispatch.currentIndex });
});

app.post('/api/dispatch/resume', (req, res) => {
  const db = loadDB();
  db.dispatch.paused = false;
  db.dispatch.pauseReason = null;
  db.dispatch.running = true;
  saveDB(db);
  scheduleNext();
  res.json({ ok: true, resumingFrom: db.dispatch.currentIndex });
});

app.get('/api/status', (req, res) => {
  const db = loadDB();
  resetDailyIfNeeded(db);
  const { dispatch, config, contacts } = db;
  res.json({
    running: dispatch.running,
    paused: dispatch.paused,
    pauseReason: dispatch.pauseReason || null,
    currentIndex: dispatch.currentIndex,
    total: contacts.length,
    sentToday: dispatch.sentToday,
    maxPerDay: config.maxPerDay,
    countdown: countdownValue,
    log: dispatch.log.slice(0, 50),
    percent: contacts.length ? Math.round((dispatch.currentIndex / contacts.length) * 100) : 0
  });
});

app.get('/api/history', (req, res) => {
  const db = loadDB();
  res.json(db.dispatch.history || []);
});

app.delete('/api/history', (req, res) => {
  const db = loadDB();
  db.dispatch.history = [];
  saveDB(db);
  res.json({ ok: true });
});

// ── Cron: agendamento semanal ────────────────────────────────
const DAYS = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'];

cron.schedule('* * * * *', () => {
  const db = loadDB();
  if (!db.schedule || db.dispatch.running) return;
  const now = new Date();
  const dayName = DAYS[now.getDay()];
  const hhmm = `${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}`;
  const slots = db.schedule[dayName] || [];
  if (slots.includes(hhmm)) {
    const db2 = loadDB();
    resetDailyIfNeeded(db2);
    db2.dispatch.running = true;
    db2.dispatch.paused = false;
    db2.dispatch.currentIndex = 0;
    db2.dispatch.log = [];
    saveDB(db2);
    scheduleNext();
    console.log(`[CRON] Disparo agendado iniciado: ${dayName} ${hhmm}`);
  }
});

// ── Start ────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Disparador Magnetico backend rodando na porta ${PORT}`);
});

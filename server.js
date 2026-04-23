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

app.use(cors({ origin: '*' }));
app.use(express.json());

function loadDB() {
  if (!fs.existsSync(DB_FILE)) return defaultDB();
  try { return JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); }
  catch { return defaultDB(); }
}
function saveDB(data) { fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2)); }
function defaultDB() {
  return {
    config: { token:'', instanceId:'', clientToken:'', intervalMin:45, intervalMax:90, blockSize:10, blockPause:15, maxPerDay:55 },
    contacts: [], messages: ['','','','',''], schedule: {},
    followup: {
      enabled: false,
      messages: ['', '', ''],
      waitDaysAfterSend: 2,
      waitDaysAfterView: 1,
      maxFollowups: 3
    },
    leads: {},
    dispatch: { running:false, paused:false, pauseReason:null, blockPauseUntil:null, currentIndex:0, sentToday:0, lastDate:'', log:[], history:[] }
  };
}

let dispatchTimer = null;
let countdownValue = 0;

function resetDailyIfNeeded(db) {
  const today = new Date().toISOString().split('T')[0];
  if (db.dispatch.lastDate !== today) {
    db.dispatch.sentToday = 0;
    db.dispatch.lastDate = today;
    if (db.dispatch.paused && db.dispatch.pauseReason === 'daily_limit') {
      db.dispatch.paused = false;
      db.dispatch.pauseReason = null;
    }
  }
}

function randomInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

async function sendMessage(config, phone, message) {
  const url = `https://api.z-api.io/instances/${config.instanceId}/token/${config.token}/send-text`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Client-Token': config.clientToken },
    body: JSON.stringify({ phone, message })
  });
  if (!res.ok) { const err = await res.text(); throw new Error(`HTTP ${res.status}: ${err}`); }
  return await res.json();
}

async function runDispatch() {
  const db = loadDB();
  resetDailyIfNeeded(db);
  if (!db.dispatch.running || db.dispatch.paused) return;

  const { config, contacts, messages, dispatch } = db;
  const validMessages = messages.filter(m => m && m.trim());
  if (!validMessages.length) { dispatch.running = false; saveDB(db); return; }

  if (dispatch.currentIndex >= contacts.length) {
    dispatch.running = false; dispatch.paused = false; dispatch.pauseReason = null;
    dispatch.history.unshift({ date: new Date().toISOString(), total: contacts.length,
      sent: dispatch.log.filter(l => l.status === 'success').length,
      errors: dispatch.log.filter(l => l.status === 'error').length });
    if (dispatch.history.length > 50) dispatch.history = dispatch.history.slice(0, 50);
    saveDB(db); console.log('[DISPATCH] Finalizado!'); return;
  }

  if (dispatch.sentToday >= config.maxPerDay) {
    dispatch.running = true; dispatch.paused = true; dispatch.pauseReason = 'daily_limit';
    saveDB(db); return;
  }

  const contact = contacts[dispatch.currentIndex];
  const msgTemplate = validMessages[Math.floor(Math.random() * validMessages.length)];
  let message = msgTemplate;
  if (contact.nome && String(contact.nome).trim() && String(contact.nome).trim() !== '0') {
    message = message.replace(/{nome}/gi, String(contact.nome).trim());
  } else {
    message = message.replace(/,?\s*{nome}\s*/gi, '').replace(/\s+/g, ' ').trim();
  }

  const phone = String(contact.numero).trim();
  let status = 'success', error = null, messageId = null;

  try {
    const result = await sendMessage(config, phone, message);
    messageId = result.messageId || result.id || null;
    dispatch.sentToday++;

    // Registrar lead para follow-up
    if (!db.leads) db.leads = {};
    db.leads[phone] = {
      numero: phone,
      nome: contact.nome || '',
      sentAt: new Date().toISOString(),
      messageId,
      status: 'sent',
      viewed: false,
      replied: false,
      followupCount: 0,
      lastFollowupAt: null,
      blacklisted: false
    };
    console.log(`[DISPATCH] ✅ ${phone} (${dispatch.currentIndex + 1}/${contacts.length})`);
  } catch (e) { status = 'error'; error = e.message; }

  dispatch.log.unshift({ time: new Date().toISOString(), nome: contact.nome || '', numero: phone, message: msgTemplate, status, error });
  if (dispatch.log.length > 500) dispatch.log = dispatch.log.slice(0, 500);
  dispatch.currentIndex++;

  const blockSize = config.blockSize || 10;
  const blockPause = config.blockPause || 15;
  if (dispatch.currentIndex % blockSize === 0 && dispatch.currentIndex < contacts.length) {
    const resumeAt = new Date(Date.now() + blockPause * 60 * 1000).toISOString();
    dispatch.paused = true; dispatch.pauseReason = 'block'; dispatch.blockPauseUntil = resumeAt;
    saveDB(db); return;
  }

  saveDB(db);
  scheduleNext();
}

function scheduleNext() {
  const db = loadDB();
  if (!db.dispatch.running || db.dispatch.paused) return;
  if (dispatchTimer) clearTimeout(dispatchTimer);
  const delay = randomInt(db.config.intervalMin, db.config.intervalMax) * 1000;
  countdownValue = Math.ceil(delay / 1000);
  const tick = setInterval(() => { if (countdownValue > 0) countdownValue--; }, 1000);
  dispatchTimer = setTimeout(() => { clearInterval(tick); runDispatch(); }, delay);
}

// ── WEBHOOK Z-API ─────────────────────────────────────────────
app.post('/api/webhook', (req, res) => {
  const db = loadDB();
  if (!db.leads) db.leads = {};
  const body = req.body;
  console.log('[WEBHOOK]', JSON.stringify(body).substring(0, 200));

  // Mensagem recebida (resposta do lead)
  if (body.type === 'ReceivedCallback' || body.event === 'message-received') {
    const phone = body.phone || body.from || (body.data && body.data.phone);
    if (phone && db.leads[phone]) {
      db.leads[phone].replied = true;
      db.leads[phone].repliedAt = new Date().toISOString();
      db.leads[phone].status = 'replied';
      console.log(`[WEBHOOK] 💬 ${phone} respondeu!`);
      saveDB(db);
    }
    // Verificar blacklist
    const text = (body.text || body.message || (body.data && body.data.text) || '').toLowerCase();
    if (phone && ['pare', 'sair', 'remover', 'stop', 'cancelar', 'nao quero'].some(w => text.includes(w))) {
      if (db.leads[phone]) {
        db.leads[phone].blacklisted = true;
        db.leads[phone].status = 'blacklisted';
        console.log(`[WEBHOOK] 🚫 ${phone} adicionado à blacklist`);
        saveDB(db);
      }
    }
  }

  // Status da mensagem (visto, entregue)
  if (body.type === 'MessageStatusCallback' || body.event === 'message-status') {
    const phone = body.phone || body.to || (body.data && body.data.phone);
    const status = body.status || (body.data && body.data.status);
    if (phone && db.leads[phone]) {
      if (status === 'READ' || status === 'read' || status === 'SEEN') {
        db.leads[phone].viewed = true;
        db.leads[phone].viewedAt = new Date().toISOString();
        db.leads[phone].status = db.leads[phone].replied ? 'replied' : 'viewed';
        console.log(`[WEBHOOK] 👁️ ${phone} visualizou`);
        saveDB(db);
      }
    }
  }

  res.json({ ok: true });
});

// ── FOLLOW-UP CRON (roda a cada hora) ──────────────────────────
async function processFollowups() {
  const db = loadDB();
  if (!db.followup || !db.followup.enabled) return;
  if (!db.leads || Object.keys(db.leads).length === 0) return;

  const now = new Date();
  const config = db.config;
  const followupMessages = db.followup.messages.filter(m => m && m.trim());
  if (!followupMessages.length) return;

  let sent = 0;
  for (const [phone, lead] of Object.entries(db.leads)) {
    if (lead.blacklisted || lead.replied) continue;
    if (lead.followupCount >= db.followup.maxFollowups) continue;

    const sentAt = new Date(lead.sentAt);
    const daysSinceSent = (now - sentAt) / (1000 * 60 * 60 * 24);
    const daysSinceLastFollowup = lead.lastFollowupAt
      ? (now - new Date(lead.lastFollowupAt)) / (1000 * 60 * 60 * 24)
      : daysSinceSent;

    let shouldSend = false;

    if (lead.viewed && daysSinceLastFollowup >= db.followup.waitDaysAfterView) {
      shouldSend = true;
    } else if (!lead.viewed && daysSinceSent >= db.followup.waitDaysAfterSend) {
      shouldSend = true;
    }

    if (shouldSend) {
      const msgIndex = lead.followupCount;
      const message = followupMessages[msgIndex] || followupMessages[followupMessages.length - 1];
      try {
        await sendMessage(config, phone, message);
        db.leads[phone].followupCount++;
        db.leads[phone].lastFollowupAt = now.toISOString();
        db.leads[phone].status = `followup_${db.leads[phone].followupCount}`;
        sent++;
        console.log(`[FOLLOWUP] ✅ Follow-up ${db.leads[phone].followupCount} enviado para ${phone}`);
        await new Promise(r => setTimeout(r, randomInt(30, 60) * 1000));
        if (sent >= 20) break;
      } catch (e) {
        console.log(`[FOLLOWUP] ❌ Erro ${phone}: ${e.message}`);
      }
    }
  }

  if (sent > 0) { saveDB(db); console.log(`[FOLLOWUP] ${sent} follow-ups enviados`); }
}

const DAYS = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'];

cron.schedule('* * * * *', () => {
  const db = loadDB();
  const now = new Date();

  if (db.dispatch.paused && db.dispatch.pauseReason === 'block' && db.dispatch.blockPauseUntil) {
    if (now >= new Date(db.dispatch.blockPauseUntil)) {
      db.dispatch.paused = false; db.dispatch.pauseReason = null; db.dispatch.blockPauseUntil = null;
      saveDB(db); scheduleNext();
    }
    return;
  }

  if (db.dispatch.paused && db.dispatch.pauseReason === 'daily_limit') {
    resetDailyIfNeeded(db);
    if (db.dispatch.sentToday === 0) { saveDB(db); scheduleNext(); return; }
  }

  if (!db.dispatch.running || db.dispatch.paused) {
    const dayName = DAYS[now.getDay()];
    const hhmm = `${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}`;
    const slots = db.schedule[dayName] || [];
    if (slots.includes(hhmm)) {
      const db2 = loadDB(); resetDailyIfNeeded(db2);
      db2.dispatch.running = true; db2.dispatch.paused = false;
      db2.dispatch.pauseReason = null; db2.dispatch.blockPauseUntil = null;
      saveDB(db2); scheduleNext();
    }
  }
});

cron.schedule('0 10,14,18 * * *', () => { processFollowups(); });

// ── ROUTES ───────────────────────────────────────────────────
app.get('/', (_, res) => res.json({ status: 'O Disparador Magnetico backend online!' }));
app.post('/api/config', (req, res) => { const db = loadDB(); db.config = { ...db.config, ...req.body }; saveDB(db); res.json({ ok: true, config: db.config }); });
app.get('/api/config', (req, res) => res.json(loadDB().config));
app.post('/api/contacts', (req, res) => { const db = loadDB(); db.contacts = req.body.contacts || []; saveDB(db); res.json({ ok: true, total: db.contacts.length }); });
app.post('/api/messages', (req, res) => { const db = loadDB(); db.messages = req.body.messages || []; saveDB(db); res.json({ ok: true }); });
app.post('/api/schedule', (req, res) => { const db = loadDB(); db.schedule = req.body.schedule || {}; saveDB(db); res.json({ ok: true }); });

app.post('/api/followup/config', (req, res) => {
  const db = loadDB();
  db.followup = { ...db.followup, ...req.body };
  saveDB(db); res.json({ ok: true });
});

app.get('/api/followup/leads', (req, res) => {
  const db = loadDB();
  const leads = Object.values(db.leads || {});
  const summary = {
    total: leads.length,
    sent: leads.filter(l => l.status === 'sent').length,
    viewed: leads.filter(l => l.viewed && !l.replied).length,
    replied: leads.filter(l => l.replied).length,
    followup1: leads.filter(l => l.followupCount === 1).length,
    followup2: leads.filter(l => l.followupCount === 2).length,
    followup3: leads.filter(l => l.followupCount >= 3).length,
    blacklisted: leads.filter(l => l.blacklisted).length,
    pending: leads.filter(l => !l.replied && !l.blacklisted && l.followupCount < 3).length
  };
  res.json({ summary, leads: leads.slice(0, 200) });
});

app.post('/api/dispatch/start', (req, res) => {
  const db = loadDB(); resetDailyIfNeeded(db);
  if (dispatchTimer) { clearTimeout(dispatchTimer); dispatchTimer = null; }
  db.dispatch.running = true; db.dispatch.paused = false;
  db.dispatch.pauseReason = null; db.dispatch.blockPauseUntil = null;
  if (req.body && req.body.restart === true) { db.dispatch.currentIndex = 0; db.dispatch.log = []; }
  saveDB(db); scheduleNext();
  res.json({ ok: true, startingFrom: db.dispatch.currentIndex });
});

app.post('/api/dispatch/pause', (req, res) => {
  const db = loadDB();
  db.dispatch.paused = true; db.dispatch.pauseReason = 'manual'; db.dispatch.blockPauseUntil = null;
  if (dispatchTimer) { clearTimeout(dispatchTimer); dispatchTimer = null; }
  countdownValue = 0; saveDB(db);
  res.json({ ok: true, pausedAt: db.dispatch.currentIndex });
});

app.post('/api/dispatch/resume', (req, res) => {
  const db = loadDB();
  db.dispatch.paused = false; db.dispatch.pauseReason = null;
  db.dispatch.blockPauseUntil = null; db.dispatch.running = true;
  saveDB(db); scheduleNext();
  res.json({ ok: true, resumingFrom: db.dispatch.currentIndex });
});

app.post('/api/dispatch/setindex', (req, res) => {
  const db = loadDB();
  const idx = parseInt(req.body.index);
  if (!isNaN(idx) && idx >= 0) { db.dispatch.currentIndex = idx; saveDB(db); res.json({ ok: true, currentIndex: idx }); }
  else res.status(400).json({ ok: false, error: 'Index invalido' });
});

app.get('/api/status', (req, res) => {
  const db = loadDB(); resetDailyIfNeeded(db);
  const { dispatch, config, contacts, leads } = db;
  let blockPauseMinutesLeft = null;
  if (dispatch.pauseReason === 'block' && dispatch.blockPauseUntil) {
    blockPauseMinutesLeft = Math.max(0, Math.ceil((new Date(dispatch.blockPauseUntil) - new Date()) / 60000));
  }
  const leadsArr = Object.values(leads || {});
  res.json({ running: dispatch.running, paused: dispatch.paused, pauseReason: dispatch.pauseReason || null,
    blockPauseMinutesLeft, currentIndex: dispatch.currentIndex, total: contacts.length,
    sentToday: dispatch.sentToday, maxPerDay: config.maxPerDay,
    blockSize: config.blockSize, blockPause: config.blockPause,
    countdown: countdownValue, log: dispatch.log.slice(0, 50),
    percent: contacts.length ? Math.round((dispatch.currentIndex / contacts.length) * 100) : 0,
    followupSummary: {
      total: leadsArr.length,
      replied: leadsArr.filter(l => l.replied).length,
      viewed: leadsArr.filter(l => l.viewed && !l.replied).length,
      pending: leadsArr.filter(l => !l.replied && !l.blacklisted).length
    }
  });
});

app.get('/api/history', (req, res) => res.json(loadDB().dispatch.history || []));

app.listen(PORT, () => {
  console.log(`Disparador Magnetico + FollowUp backend rodando na porta ${PORT}`);
  const db = loadDB();
  resetDailyIfNeeded(db); saveDB(db);
  if (db.dispatch.running && !db.dispatch.paused) { scheduleNext(); }
});

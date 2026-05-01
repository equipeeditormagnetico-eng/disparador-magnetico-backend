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
    config: { token:'', instanceId:'', clientToken:'', intervalMin:45, intervalMax:90, blockSize:10, blockPause:15, maxPerDay:30 },
    contacts: [], messages: ['','','','',''], schedule: {},
    flow: { enabled: true, message1: '', message2: '', message3: '' },
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

// ── WEBHOOK Z-API ─────────────────────────────────────────────
app.post('/api/webhook', async (req, res) => {
  res.json({ ok: true }); // Responde imediatamente

  try {
    const db = loadDB();
    if (!db.leads) db.leads = {};
    const body = req.body;

    // Log completo para debug
    console.log('[WEBHOOK RAW]', JSON.stringify(body).substring(0, 400));

    // Verificar se é mensagem enviada por mim (ignorar)
    const fromMe = body.fromMe === true ||
                   (body.data && body.data.fromMe === true);

    if (fromMe) {
      console.log('[WEBHOOK] Ignorando — mensagem enviada por mim');
      return;
    }

    // Extrair phone
    const phone = String(body.phone || body.from ||
                  (body.data && (body.data.phone || body.data.from)) || '').trim();

    if (!phone) {
      console.log('[WEBHOOK] Sem phone identificado');
      return;
    }

    // Extrair texto
    const rawText = body.text || body.caption || body.message ||
                   (body.data && (body.data.text || body.data.caption || body.data.message)) || '';
    const text = (typeof rawText === 'string' ? rawText : JSON.stringify(rawText)).trim().toLowerCase();

    console.log(`[WEBHOOK] 📩 Mensagem de ${phone}: "${text.substring(0, 100)}"`);

    // Blacklist
    if (['pare', 'sair', 'remover', 'stop', 'cancelar', 'nao quero', 'não quero'].some(w => text.includes(w))) {
      if (db.leads[phone]) {
        db.leads[phone].blacklisted = true;
        db.leads[phone].flowStep = 'blacklisted';
        saveDB(db);
        console.log(`[WEBHOOK] 🚫 ${phone} blacklisted`);
      }
      return;
    }

    const lead = db.leads[phone];
    if (!lead) {
      console.log(`[WEBHOOK] Lead ${phone} nao encontrado no banco`);
      return;
    }

    if (lead.blacklisted) {
      console.log(`[WEBHOOK] ${phone} esta na blacklist`);
      return;
    }

    const config = db.config;
    const flow = db.flow;

    // PASSO 1: Respondeu msg1 → envia msg2
    if (lead.flowStep === 'msg1_sent') {
      console.log(`[FLOW] ${phone} respondeu msg1 — enviando msg2`);
      try {
        await sendMessage(config, phone, flow.message2);
        db.leads[phone].flowStep = 'msg2_sent';
        db.leads[phone].msg2SentAt = new Date().toISOString();
        db.leads[phone].replied = true;
        saveDB(db);
        console.log(`[FLOW] ✅ Msg2 enviada para ${phone}`);
      } catch(e) { console.log(`[FLOW] ❌ Erro msg2 ${phone}: ${e.message}`); }
      return;
    }

    // PASSO 2: Respondeu msg2 → envia msg3
    if (lead.flowStep === 'msg2_sent') {
      console.log(`[FLOW] ${phone} respondeu msg2 — enviando msg3`);
      try {
        await sendMessage(config, phone, flow.message3);
        db.leads[phone].flowStep = 'completed';
        db.leads[phone].completedAt = new Date().toISOString();
        saveDB(db);
        console.log(`[FLOW] ✅ Msg3 enviada para ${phone} — FLUXO COMPLETO!`);
      } catch(e) { console.log(`[FLOW] ❌ Erro msg3 ${phone}: ${e.message}`); }
      return;
    }

    console.log(`[FLOW] ${phone} flowStep atual: ${lead.flowStep} — nenhuma acao`);

  } catch(err) {
    console.log('[WEBHOOK] Erro geral:', err.message);
  }
});

// ── DISPARO ───────────────────────────────────────────────────
async function runDispatch() {
  const db = loadDB();
  resetDailyIfNeeded(db);
  if (!db.dispatch.running || db.dispatch.paused) return;

  const { config, contacts, dispatch, flow } = db;

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
  const phone = String(contact.numero).trim();

  let message = '';
  if (flow && flow.enabled && flow.message1) {
    message = flow.message1;
  } else {
    const messages = db.messages || [];
    const validMessages = messages.filter(m => m && m.trim());
    if (!validMessages.length) { dispatch.running = false; saveDB(db); return; }
    const msgTemplate = validMessages[Math.floor(Math.random() * validMessages.length)];
    message = msgTemplate;
    if (contact.nome && String(contact.nome).trim() && String(contact.nome).trim() !== '0') {
      message = message.replace(/{nome}/gi, String(contact.nome).trim());
    } else {
      message = message.replace(/,?\s*{nome}\s*/gi, '').replace(/\s+/g, ' ').trim();
    }
  }

  let status = 'success', error = null;
  try {
    await sendMessage(config, phone, message);
    dispatch.sentToday++;
    if (!db.leads) db.leads = {};
    db.leads[phone] = {
      numero: phone, nome: contact.nome || '',
      sentAt: new Date().toISOString(),
      flowStep: 'msg1_sent',
      viewed: false, replied: false, blacklisted: false,
      msg2SentAt: null, completedAt: null
    };
    console.log(`[DISPATCH] ✅ ${phone} (${dispatch.currentIndex + 1}/${contacts.length})`);
  } catch(e) { status = 'error'; error = e.message; console.log(`[DISPATCH] ❌ ${phone}: ${e.message}`); }

  dispatch.log.unshift({ time: new Date().toISOString(), nome: contact.nome || '', numero: phone, message, status, error });
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

// ── ROUTES ────────────────────────────────────────────────────
app.get('/', (_, res) => res.json({ status: 'O Disparador Magnetico + Fluxo v2 online!' }));
app.post('/api/config', (req, res) => { const db = loadDB(); db.config = { ...db.config, ...req.body }; saveDB(db); console.log('[CONFIG]', db.config); res.json({ ok: true, config: db.config }); });
app.get('/api/config', (req, res) => res.json(loadDB().config));
app.post('/api/contacts', (req, res) => { const db = loadDB(); db.contacts = req.body.contacts || []; saveDB(db); res.json({ ok: true, total: db.contacts.length }); });
app.post('/api/messages', (req, res) => { const db = loadDB(); db.messages = req.body.messages || []; saveDB(db); res.json({ ok: true }); });
app.post('/api/schedule', (req, res) => { const db = loadDB(); db.schedule = req.body.schedule || {}; saveDB(db); res.json({ ok: true }); });

app.post('/api/flow/config', (req, res) => {
  const db = loadDB();
  db.flow = { ...db.flow, ...req.body };
  saveDB(db); console.log('[FLOW] Config:', db.flow.enabled);
  res.json({ ok: true });
});
app.get('/api/flow/config', (req, res) => res.json(loadDB().flow || {}));
app.get('/api/flow/stats', (req, res) => {
  const db = loadDB();
  const leads = Object.values(db.leads || {});
  res.json({
    total: leads.length,
    msg1_sent: leads.filter(l => l.flowStep === 'msg1_sent').length,
    msg2_sent: leads.filter(l => l.flowStep === 'msg2_sent').length,
    completed: leads.filter(l => l.flowStep === 'completed').length,
    blacklisted: leads.filter(l => l.blacklisted).length,
    viewed: leads.filter(l => l.viewed).length,
    leads: leads.slice(0, 100)
  });
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
  const { dispatch, config, contacts, leads, flow } = db;
  let blockPauseMinutesLeft = null;
  if (dispatch.pauseReason === 'block' && dispatch.blockPauseUntil) {
    blockPauseMinutesLeft = Math.max(0, Math.ceil((new Date(dispatch.blockPauseUntil) - new Date()) / 60000));
  }
  const leadsArr = Object.values(leads || {});
  res.json({
    running: dispatch.running, paused: dispatch.paused, pauseReason: dispatch.pauseReason || null,
    blockPauseMinutesLeft, currentIndex: dispatch.currentIndex, total: contacts.length,
    sentToday: dispatch.sentToday, maxPerDay: config.maxPerDay,
    blockSize: config.blockSize, blockPause: config.blockPause,
    countdown: countdownValue, log: dispatch.log.slice(0, 50),
    percent: contacts.length ? Math.round((dispatch.currentIndex / contacts.length) * 100) : 0,
    flowEnabled: flow && flow.enabled,
    flowStats: {
      total: leadsArr.length,
      aguardando: leadsArr.filter(l => l.flowStep === 'msg1_sent').length,
      msg2_enviada: leadsArr.filter(l => l.flowStep === 'msg2_sent').length,
      concluidos: leadsArr.filter(l => l.flowStep === 'completed').length,
      blacklisted: leadsArr.filter(l => l.blacklisted).length
    }
  });
});

app.get('/api/history', (req, res) => res.json(loadDB().dispatch.history || []));
app.delete('/api/history', (req, res) => { const db = loadDB(); db.dispatch.history = []; saveDB(db); res.json({ ok: true }); });

// Endpoint de debug do webhook
app.post('/api/webhook/test', (req, res) => {
  console.log('[WEBHOOK TEST]', JSON.stringify(req.body));
  res.json({ ok: true, received: req.body });
});

app.listen(PORT, () => {
  console.log(`Disparador Magnetico + Fluxo v2 rodando na porta ${PORT}`);
  const db = loadDB();
  resetDailyIfNeeded(db); saveDB(db);
  if (db.dispatch.running && !db.dispatch.paused) { scheduleNext(); }
  else if (db.dispatch.paused && db.dispatch.pauseReason === 'block' && db.dispatch.blockPauseUntil) {
    if (new Date() >= new Date(db.dispatch.blockPauseUntil)) {
      db.dispatch.paused = false; db.dispatch.pauseReason = null; db.dispatch.blockPauseUntil = null;
      saveDB(db); scheduleNext();
    }
  }
});

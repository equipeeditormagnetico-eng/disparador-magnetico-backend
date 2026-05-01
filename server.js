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

const DEFAULT_FLOW = {
  enabled: true,
  message1: `Você está nos meus grupos de devocional e eu vim aqui te contar algo especial 💖
*Durante todo o mês de maio, meus livros estão com FRETE GRÁTIS* 🙌
E são leituras que ajudam a alinhar o coração com Deus no dia a dia.
Você gostaria de saber mais? 
Digite:
1️⃣ Sim, quero.   
2️⃣ Me conta como funciona.`,
  message2: `📚Tenho 3 livros que têm abençoado muitas mulheres:
✨ *Desconectadas* – para mulheres que desejam sair do raso e superficial. 
✨ *Saindo da Gaiola* – Cura Emocional a Luz da Bíblia 
✨ *Mulheres da Bíblia* – 328 Conselhos práticos para os dias atuais
Qual mais fala com você hoje?
Digite:
1️⃣ Quero me aproximar mais de Deus.
2️⃣ Preciso de cura.
3️⃣ Quero Conselhos para o dia a dia.`,
  message3: `❤️‍🔥*Este livro é muito profundo e tenho convicção que Deus vai falar ao seu coração.*
Para garantir o seu, entra no meu site e garanta o seu com frete grátis 👇🏻
http://danielasantosoficial.com.br
Se preferir comprar por aqui, me avisa que logo já lhe oriento.`
};

const DEFAULT_CONFIG = {
  token: 'F77F594F7E5C002D7C34983F',
  instanceId: '3F20B7C2424BC14EE41AB61024DC65E0',
  clientToken: 'Fe0074d5b816c40c59256907dd6fe40eaS',
  intervalMin: 45, intervalMax: 90,
  blockSize: 10, blockPause: 15, maxPerDay: 30
};

app.use(cors({ origin: '*' }));
app.use(express.json());

function loadDB() {
  if (!fs.existsSync(DB_FILE)) return defaultDB();
  try { return JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); }
  catch { return defaultDB(); }
}
function defaultDB() {
  return { config: { ...DEFAULT_CONFIG }, contacts: [], messages: ['','','','',''], schedule: {},
    flow: { ...DEFAULT_FLOW }, leads: {},
    dispatch: { running:false, paused:false, pauseReason:null, blockPauseUntil:null, currentIndex:0, sentToday:0, lastDate:'', log:[], history:[] } };
}
function saveDB(data) { fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2)); }
function ensureDB() {
  const db = loadDB();
  if (!db.flow || !db.flow.message1) { db.flow = { ...DEFAULT_FLOW }; saveDB(db); }
  if (!db.config.token) { db.config = { ...DEFAULT_CONFIG, ...db.config }; saveDB(db); }
  return db;
}
function findLead(db, phone) {
  if (db.leads && db.leads[phone]) return phone;
  if (phone.length === 12 && phone.startsWith('55')) {
    const with9 = phone.slice(0,4) + '9' + phone.slice(4);
    if (db.leads && db.leads[with9]) return with9;
  }
  if (phone.length === 13 && phone.startsWith('55')) {
    const without9 = phone.slice(0,4) + phone.slice(5);
    if (db.leads && db.leads[without9]) return without9;
  }
  return null;
}

let dispatchTimer = null;
let countdownValue = 0;

function resetDailyIfNeeded(db) {
  const today = new Date().toISOString().split('T')[0];
  if (db.dispatch.lastDate !== today) {
    db.dispatch.sentToday = 0; db.dispatch.lastDate = today;
    if (db.dispatch.paused && db.dispatch.pauseReason === 'daily_limit') {
      db.dispatch.paused = false; db.dispatch.pauseReason = null;
    }
  }
}
function randomInt(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

async function sendMessage(config, phone, message) {
  if (!message || !message.trim()) throw new Error('Mensagem vazia');
  const url = `https://api.z-api.io/instances/${config.instanceId}/token/${config.token}/send-text`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Client-Token': config.clientToken },
    body: JSON.stringify({ phone, message })
  });
  if (!res.ok) { const err = await res.text(); throw new Error(`HTTP ${res.status}: ${err}`); }
  return await res.json();
}

app.post('/api/webhook', async (req, res) => {
  res.json({ ok: true });
  try {
    const body = req.body;
    console.log('[WEBHOOK]', JSON.stringify(body).substring(0, 300));

    // IGNORAR status callbacks (SENT, DELIVERED, READ, etc)
    const isStatusCallback =
      body.type === 'MessageStatusCallback' ||
      body.event === 'message-status' ||
      (body.ids && Array.isArray(body.ids)) ||
      ['SENT','DELIVERED','READ','READ_BY_ME','RECEIVED','PLAYED'].includes(body.status);
    if (isStatusCallback) { console.log('[WEBHOOK] Ignorando status:', body.status || body.type); return; }

    // IGNORAR fromMe
    const fromMe = body.fromMe === true || (body.data && body.data.fromMe === true);
    if (fromMe) { console.log('[WEBHOOK] Ignorando fromMe'); return; }

    // Extrair e normalizar phone
    const rawPhone = String(body.phone || body.from || (body.data && (body.data.phone || body.data.from)) || '').trim();
    const phone = rawPhone.replace(/@.*/g, '').replace(/[^0-9]/g, '').trim();
    if (!phone) { console.log('[WEBHOOK] Sem phone'); return; }

    // Extrair texto
    const rawText = body.text || body.caption || body.message ||
                   (body.data && (body.data.text || body.data.caption || body.data.message)) || '';
    const text = (typeof rawText === 'string' ? rawText : JSON.stringify(rawText)).trim();
    if (!text) { console.log('[WEBHOOK] Sem texto'); return; }

    console.log(`[WEBHOOK] 📩 ${phone}: "${text.substring(0,80)}"`);

    const db = ensureDB();
    if (!db.leads) db.leads = {};

    // Blacklist
    const tl = text.toLowerCase();
    if (['pare','sair','remover','stop','cancelar'].some(w => tl.includes(w))) {
      const lp = findLead(db, phone);
      if (lp) { db.leads[lp].blacklisted = true; db.leads[lp].flowStep = 'blacklisted'; saveDB(db); }
      return;
    }

    const leadPhone = findLead(db, phone);
    const lead = leadPhone ? db.leads[leadPhone] : null;
    if (!lead || lead.blacklisted) { console.log(`[WEBHOOK] Lead ${phone} nao encontrado`); return; }

    const flow = db.flow || DEFAULT_FLOW;

    if (lead.flowStep === 'msg1_sent') {
      console.log(`[FLOW] ${phone} respondeu msg1 → enviando msg2`);
      try {
        await sendMessage(db.config, leadPhone, flow.message2 || DEFAULT_FLOW.message2);
        db.leads[leadPhone].flowStep = 'msg2_sent';
        db.leads[leadPhone].msg2SentAt = new Date().toISOString();
        db.leads[leadPhone].replied = true;
        saveDB(db); console.log(`[FLOW] ✅ Msg2 → ${leadPhone}`);
      } catch(e) { console.log(`[FLOW] ❌ Msg2 erro: ${e.message}`); }
      return;
    }

    if (lead.flowStep === 'msg2_sent') {
      console.log(`[FLOW] ${phone} respondeu msg2 → enviando msg3`);
      try {
        await sendMessage(db.config, leadPhone, flow.message3 || DEFAULT_FLOW.message3);
        db.leads[leadPhone].flowStep = 'completed';
        db.leads[leadPhone].completedAt = new Date().toISOString();
        saveDB(db); console.log(`[FLOW] ✅ Msg3 → ${leadPhone} COMPLETO!`);
      } catch(e) { console.log(`[FLOW] ❌ Msg3 erro: ${e.message}`); }
      return;
    }

    console.log(`[FLOW] ${phone} step:${lead.flowStep} — nenhuma acao`);
  } catch(err) { console.log('[WEBHOOK] Erro:', err.message); }
});

async function runDispatch() {
  const db = ensureDB();
  resetDailyIfNeeded(db);
  if (!db.dispatch.running || db.dispatch.paused) return;
  const { config, contacts, dispatch, flow } = db;

  if (dispatch.currentIndex >= contacts.length) {
    dispatch.running = false; dispatch.paused = false; dispatch.pauseReason = null;
    dispatch.history.unshift({ date: new Date().toISOString(), total: contacts.length,
      sent: dispatch.log.filter(l=>l.status==='success').length,
      errors: dispatch.log.filter(l=>l.status==='error').length });
    if (dispatch.history.length > 50) dispatch.history = dispatch.history.slice(0,50);
    saveDB(db); console.log('[DISPATCH] Finalizado!'); return;
  }

  if (dispatch.sentToday >= config.maxPerDay) {
    dispatch.running = true; dispatch.paused = true; dispatch.pauseReason = 'daily_limit';
    saveDB(db); return;
  }

  const contact = contacts[dispatch.currentIndex];
  const phone = String(contact.numero).trim();
  const message = flow.message1 || DEFAULT_FLOW.message1;
  let status = 'success', error = null;

  try {
    await sendMessage(config, phone, message);
    dispatch.sentToday++;
    if (!db.leads) db.leads = {};
    db.leads[phone] = { numero:phone, nome:contact.nome||'', sentAt:new Date().toISOString(),
      flowStep:'msg1_sent', viewed:false, replied:false, blacklisted:false, msg2SentAt:null, completedAt:null };
    console.log(`[DISPATCH] ✅ ${phone} (${dispatch.currentIndex+1}/${contacts.length}) hoje:${dispatch.sentToday}/${config.maxPerDay}`);
  } catch(e) { status='error'; error=e.message; console.log(`[DISPATCH] ❌ ${phone}: ${e.message}`); }

  dispatch.log.unshift({ time:new Date().toISOString(), nome:contact.nome||'', numero:phone, message:message.substring(0,100), status, error });
  if (dispatch.log.length > 500) dispatch.log = dispatch.log.slice(0,500);
  dispatch.currentIndex++;

  const blockSize = config.blockSize || 10;
  const blockPause = config.blockPause || 15;
  if (dispatch.currentIndex % blockSize === 0 && dispatch.currentIndex < contacts.length) {
    const resumeAt = new Date(Date.now() + blockPause*60*1000).toISOString();
    dispatch.paused = true; dispatch.pauseReason = 'block'; dispatch.blockPauseUntil = resumeAt;
    saveDB(db); return;
  }
  saveDB(db); scheduleNext();
}

function scheduleNext() {
  const db = loadDB();
  if (!db.dispatch.running || db.dispatch.paused) return;
  if (dispatchTimer) clearTimeout(dispatchTimer);
  const delay = randomInt(db.config.intervalMin, db.config.intervalMax) * 1000;
  countdownValue = Math.ceil(delay/1000);
  const tick = setInterval(()=>{ if(countdownValue>0)countdownValue--; }, 1000);
  dispatchTimer = setTimeout(()=>{ clearInterval(tick); runDispatch(); }, delay);
  console.log(`[DISPATCH] Proximo em ${countdownValue}s`);
}

const DAYS = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'];

cron.schedule('* * * * *', () => {
  const db = loadDB();
  const now = new Date();
  if (db.dispatch.paused && db.dispatch.pauseReason === 'block' && db.dispatch.blockPauseUntil) {
    if (now >= new Date(db.dispatch.blockPauseUntil)) {
      db.dispatch.paused=false; db.dispatch.pauseReason=null; db.dispatch.blockPauseUntil=null;
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
      const db2 = ensureDB(); resetDailyIfNeeded(db2);
      db2.dispatch.running=true; db2.dispatch.paused=false;
      db2.dispatch.pauseReason=null; db2.dispatch.blockPauseUntil=null;
      saveDB(db2); scheduleNext();
    }
  }
});

app.get('/', (_,res)=>res.json({status:'O Disparador Magnetico FINAL v2 online!'}));
app.post('/api/config', (req,res)=>{ const db=loadDB(); db.config={...db.config,...req.body}; saveDB(db); res.json({ok:true,config:db.config}); });
app.get('/api/config', (req,res)=>res.json(loadDB().config));
app.post('/api/contacts', (req,res)=>{ const db=loadDB(); db.contacts=req.body.contacts||[]; saveDB(db); res.json({ok:true,total:db.contacts.length}); });
app.post('/api/messages', (req,res)=>{ const db=loadDB(); db.messages=req.body.messages||[]; saveDB(db); res.json({ok:true}); });
app.post('/api/schedule', (req,res)=>{ const db=loadDB(); db.schedule=req.body.schedule||{}; saveDB(db); res.json({ok:true}); });
app.post('/api/flow/config', (req,res)=>{ const db=loadDB(); db.flow={...DEFAULT_FLOW,...db.flow,...req.body}; saveDB(db); res.json({ok:true}); });
app.get('/api/flow/config', (req,res)=>res.json(ensureDB().flow));
app.get('/api/flow/stats', (req,res)=>{
  const db=loadDB(); const leads=Object.values(db.leads||{});
  res.json({ total:leads.length, msg1_sent:leads.filter(l=>l.flowStep==='msg1_sent').length,
    msg2_sent:leads.filter(l=>l.flowStep==='msg2_sent').length,
    completed:leads.filter(l=>l.flowStep==='completed').length,
    blacklisted:leads.filter(l=>l.blacklisted).length, leads:leads.slice(0,100) });
});
app.post('/api/dispatch/start', (req,res)=>{
  const db=ensureDB(); resetDailyIfNeeded(db);
  if(dispatchTimer){clearTimeout(dispatchTimer);dispatchTimer=null;}
  db.dispatch.running=true; db.dispatch.paused=false;
  db.dispatch.pauseReason=null; db.dispatch.blockPauseUntil=null;
  if(req.body&&req.body.restart===true){db.dispatch.currentIndex=0;db.dispatch.log=[];}
  saveDB(db); scheduleNext();
  res.json({ok:true,startingFrom:db.dispatch.currentIndex});
});
app.post('/api/dispatch/pause', (req,res)=>{
  const db=loadDB(); db.dispatch.paused=true; db.dispatch.pauseReason='manual'; db.dispatch.blockPauseUntil=null;
  if(dispatchTimer){clearTimeout(dispatchTimer);dispatchTimer=null;}
  countdownValue=0; saveDB(db); res.json({ok:true,pausedAt:db.dispatch.currentIndex});
});
app.post('/api/dispatch/resume', (req,res)=>{
  const db=ensureDB(); db.dispatch.paused=false; db.dispatch.pauseReason=null;
  db.dispatch.blockPauseUntil=null; db.dispatch.running=true;
  saveDB(db); scheduleNext(); res.json({ok:true,resumingFrom:db.dispatch.currentIndex});
});
app.post('/api/dispatch/setindex', (req,res)=>{
  const db=loadDB(); const idx=parseInt(req.body.index);
  if(!isNaN(idx)&&idx>=0){db.dispatch.currentIndex=idx;saveDB(db);res.json({ok:true,currentIndex:idx});}
  else res.status(400).json({ok:false,error:'Index invalido'});
});
app.get('/api/status', (req,res)=>{
  const db=ensureDB(); resetDailyIfNeeded(db);
  const {dispatch,config,contacts,leads,flow}=db;
  let blockPauseMinutesLeft=null;
  if(dispatch.pauseReason==='block'&&dispatch.blockPauseUntil){
    blockPauseMinutesLeft=Math.max(0,Math.ceil((new Date(dispatch.blockPauseUntil)-new Date())/60000));
  }
  const leadsArr=Object.values(leads||{});
  res.json({ running:dispatch.running, paused:dispatch.paused, pauseReason:dispatch.pauseReason||null,
    blockPauseMinutesLeft, currentIndex:dispatch.currentIndex, total:contacts.length,
    sentToday:dispatch.sentToday, maxPerDay:config.maxPerDay,
    blockSize:config.blockSize, blockPause:config.blockPause,
    countdown:countdownValue, log:dispatch.log.slice(0,50),
    percent:contacts.length?Math.round((dispatch.currentIndex/contacts.length)*100):0,
    flowEnabled:flow&&flow.enabled, msg1Preview:(flow&&flow.message1||'').substring(0,50),
    flowStats:{ total:leadsArr.length,
      aguardando:leadsArr.filter(l=>l.flowStep==='msg1_sent').length,
      msg2_enviada:leadsArr.filter(l=>l.flowStep==='msg2_sent').length,
      concluidos:leadsArr.filter(l=>l.flowStep==='completed').length,
      blacklisted:leadsArr.filter(l=>l.blacklisted).length } });
});
app.get('/api/history', (req,res)=>res.json(loadDB().dispatch.history||[]));
app.delete('/api/history', (req,res)=>{ const db=loadDB(); db.dispatch.history=[]; saveDB(db); res.json({ok:true}); });

app.listen(PORT, ()=>{
  console.log(`Disparador Magnetico FINAL v2 rodando na porta ${PORT}`);
  const db=ensureDB(); resetDailyIfNeeded(db); saveDB(db);
  console.log(`[STARTUP] Flow:${db.flow.enabled} | Msg1:${(db.flow.message1||'').substring(0,50)}`);
  if(db.dispatch.running&&!db.dispatch.paused){scheduleNext();}
  else if(db.dispatch.paused&&db.dispatch.pauseReason==='block'&&db.dispatch.blockPauseUntil){
    if(new Date()>=new Date(db.dispatch.blockPauseUntil)){
      db.dispatch.paused=false; db.dispatch.pauseReason=null; db.dispatch.blockPauseUntil=null;
      saveDB(db); scheduleNext();
    }
  }
});

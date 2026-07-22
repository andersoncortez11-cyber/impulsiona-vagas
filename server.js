const express = require('express');
const http = require('http');
const https = require('https');
const { Server } = require('socket.io');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs');

const ROOT = __dirname;
const PORT = process.env.PORT || 3000;

// ===== DATABASE =====
const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && process.env.DATABASE_URL.includes('render.com') ? { rejectUnauthorized: false } : false
});

async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      type TEXT NOT NULL DEFAULT 'candidate',
      name TEXT,
      company TEXT,
      cnpj TEXT,
      subscription TEXT,
      "createdAt" TEXT
    );
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      "companyId" TEXT NOT NULL,
      company TEXT,
      title TEXT NOT NULL,
      description TEXT,
      location TEXT,
      category TEXT,
      experience TEXT,
      regime TEXT,
      type TEXT,
      "salaryMin" REAL,
      "salaryMax" REAL,
      requirements TEXT,
      benefits TEXT,
      status TEXT DEFAULT 'active',
      "createdAt" TEXT
    );
    CREATE TABLE IF NOT EXISTS applications (
      id TEXT PRIMARY KEY,
      "jobId" TEXT NOT NULL,
      "userId" TEXT NOT NULL,
      "resumeId" TEXT,
      "coverLetter" TEXT,
      "resumePdf" TEXT,
      "resumePdfName" TEXT,
      status TEXT DEFAULT 'pending',
      "appliedAt" TEXT,
      "updatedAt" TEXT
    );
    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      "from" TEXT NOT NULL,
      "to" TEXT NOT NULL,
      "jobId" TEXT,
      text TEXT,
      timestamp TEXT,
      read INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS notifications (
      id TEXT PRIMARY KEY,
      "userId" TEXT NOT NULL,
      text TEXT,
      read INTEGER DEFAULT 0,
      "createdAt" TEXT
    );
    CREATE TABLE IF NOT EXISTS resumes (
      id TEXT PRIMARY KEY,
      "userId" TEXT UNIQUE NOT NULL,
      name TEXT, email TEXT, phone TEXT,
      title TEXT, summary TEXT,
      experience TEXT, education TEXT,
      skills TEXT, languages TEXT,
      pdf TEXT, "pdfName" TEXT,
      "createdAt" TEXT, "updatedAt" TEXT
    );
  `);
  console.log('[DB] PostgreSQL inicializado');
}

function genId() { return Date.now().toString(36) + Math.random().toString(36).substr(2, 9); }
function today() { return new Date().toISOString().split('T')[0]; }
function now() { return new Date().toISOString(); }
function safeJson(s) { try { return JSON.parse(s); } catch { return s; } }
function safeJsonArr(s) { try { return JSON.parse(s); } catch { return []; } }

// ===== EXPRESS + SOCKET.IO =====
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.json({ limit: '50mb' }));

// ===== STATIC FILES =====
const MIME = { '.html':'text/html','.css':'text/css','.js':'application/javascript','.json':'application/json','.png':'image/png','.jpg':'image/jpeg','.gif':'image/gif','.svg':'image/svg+xml','.ico':'image/x-icon','.pdf':'application/pdf','.woff':'font/woff','.woff2':'font/woff2','.ttf':'font/ttf' };
app.use((req, res, next) => {
  if (req.url.startsWith('/api/')) return next();
  let fp = path.join(ROOT, req.url === '/' ? 'index.html' : req.url);
  fs.readFile(fp, (err, data) => {
    if (err) { res.status(404).send('404'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(fp).toLowerCase()] || 'application/octet-stream' });
    res.end(data);
  });
});

// ===== SOCKET.IO REAL-TIME =====
io.on('connection', (socket) => {
  console.log(`[WS] Conectado: ${socket.id}`);
  socket.on('disconnect', () => console.log(`[WS] Desconectado: ${socket.id}`));
});

function broadcast(table, action, data) {
  io.emit('db_change', { table, action, data, timestamp: Date.now() });
}

// ===== AUTH API =====
app.post('/api/auth/register', async (req, res) => {
  try {
    const { email, password, type, name, company, cnpj, subscription } = req.body;
    const exists = await db.query('SELECT id FROM users WHERE email = $1', [email]);
    if (exists.rows.length) return res.json({ success: false, error: 'Este email já está cadastrado' });
    const id = genId();
    await db.query('INSERT INTO users (id,email,password,type,name,company,cnpj,subscription,"createdAt") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
      [id, email, password, type || 'candidate', name, company || '', cnpj || '', JSON.stringify(subscription || null), today()]);
    const user = (await db.query('SELECT * FROM users WHERE id = $1', [id])).rows[0];
    user.subscription = safeJson(user.subscription);
    broadcast('users', 'create', { id });
    res.json({ success: true, user });
  } catch(e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = (await db.query('SELECT * FROM users WHERE email = $1 AND password = $2', [email, password])).rows[0];
    if (!user) return res.json({ success: false, error: 'Email ou senha incorretos' });
    user.subscription = safeJson(user.subscription);
    res.json({ success: true, user });
  } catch(e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.get('/api/users', async (req, res) => {
  try {
    const users = (await db.query('SELECT * FROM users')).rows.map(u => { u.subscription = safeJson(u.subscription); return u; });
    res.json(users);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/users/:id', async (req, res) => {
  try {
    const user = (await db.query('SELECT * FROM users WHERE id = $1', [req.params.id])).rows[0];
    if (!user) return res.status(404).json({ error: 'not found' });
    user.subscription = safeJson(user.subscription);
    res.json(user);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/users/:id', async (req, res) => {
  try {
    const fields = req.body;
    const sets = []; const vals = [];
    let i = 1;
    Object.keys(fields).forEach(k => { if (k !== 'id') { sets.push(`"${k}" = $${i}`); vals.push(typeof fields[k] === 'object' ? JSON.stringify(fields[k]) : fields[k]); i++; } });
    if (sets.length) { vals.push(req.params.id); await db.query(`UPDATE users SET ${sets.join(',')} WHERE id = $${i}`, vals); }
    broadcast('users', 'update', { id: req.params.id });
    const user = (await db.query('SELECT * FROM users WHERE id = $1', [req.params.id])).rows[0];
    user.subscription = safeJson(user.subscription);
    res.json(user);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/users/:id', async (req, res) => {
  try {
    const uid = req.params.id;
    const user = (await db.query('SELECT * FROM users WHERE id = $1', [uid])).rows[0];
    if (!user) return res.status(404).json({ error: 'not found' });
    if (user.type === 'company') {
      const jobIds = (await db.query('SELECT id FROM jobs WHERE "companyId" = $1', [uid])).rows.map(j => j.id);
      if (jobIds.length) {
        const ph = jobIds.map((_, i) => `$${i + 1}`).join(',');
        await db.query(`DELETE FROM applications WHERE "jobId" IN (${ph})`, jobIds);
        await db.query(`DELETE FROM messages WHERE "jobId" IN (${ph})`, jobIds);
      }
      await db.query('DELETE FROM jobs WHERE "companyId" = $1', [uid]);
    } else {
      await db.query('DELETE FROM applications WHERE "userId" = $1', [uid]);
      await db.query('DELETE FROM resumes WHERE "userId" = $1', [uid]);
    }
    await db.query('DELETE FROM messages WHERE "from" = $1 OR "to" = $1', [uid]);
    await db.query('DELETE FROM notifications WHERE "userId" = $1', [uid]);
    await db.query('DELETE FROM users WHERE id = $1', [uid]);
    broadcast('users', 'delete', { id: uid });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== JOBS API =====
app.get('/api/jobs', async (req, res) => {
  try {
    let jobs;
    const q = req.query;
    if (q.all) {
      jobs = (await db.query('SELECT * FROM jobs')).rows;
    } else {
      jobs = (await db.query('SELECT * FROM jobs WHERE status = $1', ['active'])).rows;
    }
    if (q.search) { const s = q.search.toLowerCase(); jobs = jobs.filter(j => (j.title+' '+j.company+' '+j.location).toLowerCase().includes(s)); }
    if (q.location) jobs = jobs.filter(j => (j.location||'').toLowerCase().includes(q.location.toLowerCase()));
    if (q.category) jobs = jobs.filter(j => j.category === q.category);
    if (q.experience) jobs = jobs.filter(j => j.experience === q.experience);
    if (q.regime) jobs = jobs.filter(j => j.regime === q.regime);
    if (q.type) jobs = jobs.filter(j => j.type === q.type);
    if (q.companyId) jobs = jobs.filter(j => j.companyId === q.companyId);
    jobs.forEach(j => { j.requirements = safeJsonArr(j.requirements); j.benefits = safeJsonArr(j.benefits); });
    jobs.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json(jobs);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/jobs/:id', async (req, res) => {
  try {
    const job = (await db.query('SELECT * FROM jobs WHERE id = $1', [req.params.id])).rows[0];
    if (!job) return res.status(404).json({ error: 'not found' });
    job.requirements = safeJsonArr(job.requirements);
    job.benefits = safeJsonArr(job.benefits);
    res.json(job);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/jobs', async (req, res) => {
  try {
    const d = req.body;
    const id = genId();
    await db.query('INSERT INTO jobs (id,"companyId",company,title,description,location,category,experience,regime,type,"salaryMin","salaryMax",requirements,benefits,status,"createdAt") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)',
      [id, d.companyId, d.company, d.title, d.description||'', d.location||'', d.category||'', d.experience||'', d.regime||'', d.type||'', d.salaryMin||0, d.salaryMax||0, JSON.stringify(d.requirements||[]), JSON.stringify(d.benefits||[]), 'active', today()]);
    broadcast('jobs', 'create', { id });
    const job = (await db.query('SELECT * FROM jobs WHERE id = $1', [id])).rows[0];
    res.json(job);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/jobs/:id', async (req, res) => {
  try {
    const d = req.body; const sets = []; const vals = []; let i = 1;
    ['title','description','location','category','experience','regime','type','salaryMin','salaryMax','status'].forEach(k => {
      if (d[k] !== undefined) { sets.push(`"${k}" = $${i}`); vals.push(d[k]); i++; }
    });
    if (d.requirements !== undefined) { sets.push(`"requirements" = $${i}`); vals.push(JSON.stringify(d.requirements)); i++; }
    if (d.benefits !== undefined) { sets.push(`"benefits" = $${i}`); vals.push(JSON.stringify(d.benefits)); i++; }
    if (sets.length) { vals.push(req.params.id); await db.query(`UPDATE jobs SET ${sets.join(',')} WHERE id = $${i}`, vals); }
    broadcast('jobs', 'update', { id: req.params.id });
    const job = (await db.query('SELECT * FROM jobs WHERE id = $1', [req.params.id])).rows[0];
    res.json(job);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/jobs/:id', async (req, res) => {
  try {
    await db.query('DELETE FROM applications WHERE "jobId" = $1', [req.params.id]);
    await db.query('DELETE FROM messages WHERE "jobId" = $1', [req.params.id]);
    await db.query('DELETE FROM jobs WHERE id = $1', [req.params.id]);
    broadcast('jobs', 'delete', { id: req.params.id });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== APPLICATIONS API =====
app.get('/api/applications', async (req, res) => {
  try {
    let apps = (await db.query('SELECT * FROM applications')).rows;
    const q = req.query;
    if (q.userId) apps = apps.filter(a => a.userId === q.userId);
    if (q.jobId) apps = apps.filter(a => a.jobId === q.jobId);
    if (q.companyId) {
      const jobIds = (await db.query('SELECT id FROM jobs WHERE "companyId" = $1', [q.companyId])).rows.map(j => j.id);
      apps = apps.filter(a => jobIds.includes(a.jobId));
    }
    res.json(apps);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/applications', async (req, res) => {
  try {
    const d = req.body;
    const exists = (await db.query('SELECT id FROM applications WHERE "jobId" = $1 AND "userId" = $2', [d.jobId, d.userId])).rows[0];
    if (exists) return res.json({ success: false, error: 'Você já se candidatou para esta vaga' });
    const id = genId();
    await db.query('INSERT INTO applications (id,"jobId","userId","resumeId","coverLetter","resumePdf","resumePdfName",status,"appliedAt","updatedAt") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',
      [id, d.jobId, d.userId, d.resumeId||'', d.coverLetter||'', d.resumePdf||'', d.resumePdfName||'', 'pending', today(), today()]);
    await db.query('INSERT INTO notifications (id,"userId",text,read,"createdAt") VALUES ($1,$2,$3,$4,$5)',
      [genId(), d.userId, 'Candidatura enviada com sucesso!', 0, now()]);
    broadcast('applications', 'create', { id });
    const app = (await db.query('SELECT * FROM applications WHERE id = $1', [id])).rows[0];
    res.json({ success: true, application: app });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/applications/:id', async (req, res) => {
  try {
    const { status } = req.body;
    await db.query('UPDATE applications SET status = $1, "updatedAt" = $2 WHERE id = $3', [status, today(), req.params.id]);
    const app = (await db.query('SELECT * FROM applications WHERE id = $1', [req.params.id])).rows[0];
    if (app) {
      const statusText = { pending:'Pendente',viewed:'Visualizado',accepted:'Aceito',rejected:'Rejeitado',cancelled:'Cancelado' };
      await db.query('INSERT INTO notifications (id,"userId",text,read,"createdAt") VALUES ($1,$2,$3,$4,$5)',
        [genId(), app.userId, `Sua candidatura foi atualizada para: ${statusText[status]||status}`, 0, now()]);
      broadcast('applications', 'update', { id: req.params.id });
    }
    res.json(app);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/applications/:id', async (req, res) => {
  try {
    await db.query('DELETE FROM applications WHERE id = $1', [req.params.id]);
    broadcast('applications', 'delete', { id: req.params.id });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== MESSAGES API =====
app.get('/api/messages', async (req, res) => {
  try {
    let msgs = (await db.query('SELECT * FROM messages')).rows;
    const q = req.query;
    if (q.userId) msgs = msgs.filter(m => m.from === q.userId || m.to === q.userId);
    if (q.conversation) {
      const [uid1, uid2, jobId] = q.conversation.split('_');
      msgs = msgs.filter(m => m.jobId === jobId && ((m.from===uid1&&m.to===uid2)||(m.from===uid2&&m.to===uid1)));
    }
    if (q.from && q.to && q.jobId) {
      msgs = msgs.filter(m => m.jobId===q.jobId && ((m.from===q.from&&m.to===q.to)||(m.from===q.to&&m.to===q.from)));
    }
    if (q.unreadFor) msgs = msgs.filter(m => m.to === q.unreadFor && !m.read);
    msgs.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    res.json(msgs);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/messages', async (req, res) => {
  try {
    const { from, to, jobId, text } = req.body;
    const id = genId();
    await db.query('INSERT INTO messages (id,"from","to","jobId",text,timestamp,read) VALUES ($1,$2,$3,$4,$5,$6,$7)',
      [id, from, to, jobId, text, now(), 0]);
    const sender = (await db.query('SELECT name FROM users WHERE id = $1', [from])).rows[0];
    if (sender) {
      const msgPreview = text.length > 60 ? text.substring(0, 60) + '...' : text;
      await db.query('INSERT INTO notifications (id,"userId",text,read,"createdAt") VALUES ($1,$2,$3,$4,$5)',
        [genId(), to, `Nova mensagem de ${sender.name}: "${msgPreview}"`, 0, now()]);
    }
    broadcast('messages', 'create', { id, from, to, jobId });
    const msg = (await db.query('SELECT * FROM messages WHERE id = $1', [id])).rows[0];
    res.json(msg);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/messages/read', async (req, res) => {
  try {
    const { userId, otherId, jobId } = req.body;
    await db.query('UPDATE messages SET read = 1 WHERE "to" = $1 AND "from" = $2 AND "jobId" = $3 AND read = 0', [userId, otherId, jobId]);
    broadcast('messages', 'read', { userId, otherId, jobId });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/messages/conversation', async (req, res) => {
  try {
    const { userId, otherId, jobId } = req.body;
    await db.query('DELETE FROM messages WHERE "jobId" = $1 AND (("from" = $2 AND "to" = $3) OR ("from" = $3 AND "to" = $2))', [jobId, userId, otherId]);
    broadcast('messages', 'delete', { jobId });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== NOTIFICATIONS API =====
app.get('/api/notifications', async (req, res) => {
  try {
    let notifs;
    if (req.query.userId) {
      notifs = (await db.query('SELECT * FROM notifications WHERE "userId" = $1', [req.query.userId])).rows;
    } else {
      notifs = (await db.query('SELECT * FROM notifications')).rows;
    }
    notifs.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json(notifs);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/notifications/read', async (req, res) => {
  try {
    const { userId } = req.body;
    await db.query('UPDATE notifications SET read = 1 WHERE "userId" = $1', [userId]);
    broadcast('notifications', 'read', { userId });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== RESUMES API =====
app.get('/api/resumes', async (req, res) => {
  try {
    let resumes = (await db.query('SELECT * FROM resumes')).rows;
    resumes.forEach(r => { r.experience = safeJsonArr(r.experience); r.education = safeJsonArr(r.education); r.skills = safeJsonArr(r.skills); r.languages = safeJsonArr(r.languages); });
    if (req.query.userId) resumes = resumes.filter(r => r.userId === req.query.userId);
    res.json(resumes);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/resumes', async (req, res) => {
  try {
    const d = req.body;
    const existing = (await db.query('SELECT id FROM resumes WHERE "userId" = $1', [d.userId])).rows[0];
    if (existing) {
      await db.query('UPDATE resumes SET name=$1,email=$2,phone=$3,title=$4,summary=$5,experience=$6,education=$7,skills=$8,languages=$9,pdf=$10,"pdfName"=$11,"updatedAt"=$12 WHERE "userId"=$13',
        [d.name,d.email,d.phone,d.title,d.summary,JSON.stringify(d.experience||[]),JSON.stringify(d.education||[]),JSON.stringify(d.skills||[]),JSON.stringify(d.languages||[]),d.pdf||'',d.pdfName||'',today(),d.userId]);
      broadcast('resumes', 'update', { userId: d.userId });
    } else {
      await db.query('INSERT INTO resumes (id,"userId",name,email,phone,title,summary,experience,education,skills,languages,pdf,"pdfName","createdAt","updatedAt") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)',
        [genId(),d.userId,d.name,d.email,d.phone,d.title,d.summary,JSON.stringify(d.experience||[]),JSON.stringify(d.education||[]),JSON.stringify(d.skills||[]),JSON.stringify(d.languages||[]),d.pdf||'',d.pdfName||'',today(),today()]);
      broadcast('resumes', 'create', { userId: d.userId });
    }
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== SYNC API =====
app.post('/api/sync/push', async (req, res) => {
  try {
    const data = req.body;
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      if (data.users) {
        for (const u of data.users) {
          await client.query(`INSERT INTO users (id,email,password,type,name,company,cnpj,subscription,"createdAt")
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET email=$2,password=$3,type=$4,name=$5,company=$6,cnpj=$7,subscription=$8,"createdAt"=$9`,
            [u.id, u.email, u.password, u.type||'candidate', u.name||'', u.company||'', u.cnpj||'', JSON.stringify(u.subscription||null), u.createdAt||today()]);
        }
      }
      if (data.jobs) {
        for (const j of data.jobs) {
          await client.query(`INSERT INTO jobs (id,"companyId",company,title,description,location,category,experience,regime,type,"salaryMin","salaryMax",requirements,benefits,status,"createdAt")
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            ON CONFLICT (id) DO UPDATE SET "companyId"=$2,company=$3,title=$4,description=$5,location=$6,category=$7,experience=$8,regime=$9,type=$10,"salaryMin"=$11,"salaryMax"=$12,requirements=$13,benefits=$14,status=$15,"createdAt"=$16`,
            [j.id, j.companyId, j.company||'', j.title, j.description||'', j.location||'', j.category||'', j.experience||'', j.regime||'', j.type||'', j.salaryMin||0, j.salaryMax||0, JSON.stringify(j.requirements||[]), JSON.stringify(j.benefits||[]), j.status||'active', j.createdAt||today()]);
        }
      }
      if (data.applications) {
        for (const a of data.applications) {
          await client.query(`INSERT INTO applications (id,"jobId","userId","resumeId","coverLetter","resumePdf","resumePdfName",status,"appliedAt","updatedAt")
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (id) DO UPDATE SET "jobId"=$2,"userId"=$3,"resumeId"=$4,"coverLetter"=$5,"resumePdf"=$6,"resumePdfName"=$7,status=$8,"appliedAt"=$9,"updatedAt"=$10`,
            [a.id, a.jobId, a.userId, a.resumeId||'', a.coverLetter||'', a.resumePdf||'', a.resumePdfName||'', a.status||'pending', a.appliedAt||today(), a.updatedAt||today()]);
        }
      }
      if (data.messages) {
        for (const m of data.messages) {
          await client.query(`INSERT INTO messages (id,"from","to","jobId",text,timestamp,read)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (id) DO UPDATE SET "from"=$2,"to"=$3,"jobId"=$4,text=$5,timestamp=$6,read=$7`,
            [m.id, m.from, m.to, m.jobId||'', m.text||'', m.timestamp||now(), m.read ? 1 : 0]);
        }
      }
      if (data.notifications) {
        for (const n of data.notifications) {
          await client.query(`INSERT INTO notifications (id,"userId",text,read,"createdAt")
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (id) DO UPDATE SET "userId"=$2,text=$3,read=$4,"createdAt"=$5`,
            [n.id, n.userId, n.text||'', n.read ? 1 : 0, n.createdAt||now()]);
        }
      }
      if (data.resumes) {
        for (const r of data.resumes) {
          await client.query(`INSERT INTO resumes (id,"userId",name,email,phone,title,summary,experience,education,skills,languages,pdf,"pdfName","createdAt","updatedAt")
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            ON CONFLICT (id) DO UPDATE SET "userId"=$2,name=$3,email=$4,phone=$5,title=$6,summary=$7,experience=$8,education=$9,skills=$10,languages=$11,pdf=$12,"pdfName"=$13,"createdAt"=$14,"updatedAt"=$15`,
            [r.id, r.userId, r.name||'', r.email||'', r.phone||'', r.title||'', r.summary||'', JSON.stringify(r.experience||[]), JSON.stringify(r.education||[]), JSON.stringify(r.skills||[]), JSON.stringify(r.languages||[]), r.pdf||'', r.pdfName||'', r.createdAt||today(), r.updatedAt||today()]);
        }
      }
      await client.query('COMMIT');
    } catch(e) { await client.query('ROLLBACK'); throw e; }
    finally { client.release(); }
    io.emit('db_change', { table: 'all', action: 'sync', timestamp: Date.now() });
    res.json({ success: true });
  } catch(e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.get('/api/sync/pull', async (req, res) => {
  try {
    const data = {
      users: (await db.query('SELECT * FROM users')).rows.map(u => { u.subscription = safeJson(u.subscription); return u; }),
      jobs: (await db.query('SELECT * FROM jobs')).rows.map(j => { j.requirements = safeJsonArr(j.requirements); j.benefits = safeJsonArr(j.benefits); return j; }),
      applications: (await db.query('SELECT * FROM applications')).rows,
      messages: (await db.query('SELECT * FROM messages')).rows,
      notifications: (await db.query('SELECT * FROM notifications')).rows,
      resumes: (await db.query('SELECT * FROM resumes')).rows.map(r => { r.experience = safeJsonArr(r.experience); r.education = safeJsonArr(r.education); r.skills = safeJsonArr(r.skills); r.languages = safeJsonArr(r.languages); return r; })
    };
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ===== MERCADO PAGO PROXY =====
const mpRouter = express.Router();
mpRouter.use((req, res) => {
  const mpPath = req.originalUrl.replace(/^\/api\/mp/, '');
  const accessToken = req.headers['x-mp-access-token'];
  if (!accessToken) return res.status(401).json({ error: 'missing access token' });

  const mpUrl = `https://api.mercadopago.com${mpPath}`;
  console.log(`[MP] ${req.method} ${mpUrl}`);

  const body = req.body ? JSON.stringify(req.body) : '';
  const mpReq = https.request(mpUrl, {
    method: req.method,
    headers: { 'Authorization': `Bearer ${accessToken}`, 'Content-Type': 'application/json', 'Accept': 'application/json', 'Content-Length': Buffer.byteLength(body) },
  }, (mpRes) => {
    let mpBody = '';
    mpRes.on('data', chunk => { mpBody += chunk; });
    mpRes.on('end', () => {
      console.log(`[MP] Response ${mpRes.statusCode}: ${mpBody.substring(0, 200)}`);
      res.writeHead(mpRes.statusCode, { 'Content-Type': 'application/json' });
      res.end(mpBody);
    });
  });
  mpReq.on('error', (err) => { res.status(502).json({ error: err.message }); });
  if (body) mpReq.write(body);
  mpReq.end();
});
app.use('/api/mp', mpRouter);

// ===== WEBHOOK =====
app.post('/api/webhook/mercadopago', (req, res) => {
  console.log('[WEBHOOK] MP notification received');
  res.json({ received: true });
});

// ===== ADMIN AUTH =====
app.post('/api/admin/auth', (req, res) => {
  const { password } = req.body;
  const adminPass = process.env.ADMIN_PASSWORD || 'impulsiona2401';
  if (password === adminPass) {
    res.json({ success: true });
  } else {
    res.json({ success: false, error: 'Senha incorreta' });
  }
});

app.post('/api/webhook/test', (req, res) => {
  console.log('[WEBHOOK TEST] OK');
  res.json({ success: true });
});

// ===== START =====
initDB().then(() => {
  server.listen(PORT, () => {
    console.log('');
    console.log('==============================================');
    console.log('  Servidor Impulsiona Vagas (PostgreSQL + Socket.io)');
    console.log(`  http://localhost:${PORT}`);
    console.log('  Ctrl+C para parar');
    console.log('==============================================');
    console.log('');
  });
}).catch(e => {
  console.error('');
  console.error('[FATAL] Erro ao conectar no PostgreSQL');
  console.error('  Erro:', e.message);
  console.error('  DATABASE_URL:', process.env.DATABASE_URL ? 'definida (' + process.env.DATABASE_URL.substring(0, 30) + '...)' : 'NAO DEFINIDA');
  console.error('');
  console.error('  Como resolver:');
  console.error('  1. No Render, va no SERVICO WEB (nao no banco)');
  console.error('  2. Aba Environment');
  console.error('  3. Adicione: Key=DATABASE_URL Value=<sua URL do banco>');
  console.error('  4. Save and redeploy');
  console.error('');
  process.exit(1);
});

// ===== SOCKET.IO =====
const socket = io();
socket.on('connect', () => console.log('[WS] Conectado'));
socket.on('disconnect', () => console.log('[WS] Desconectado'));

// ===== CACHE (localStorage-backed + API sync) =====
const Cache = {
  _dirty: new Set(),
  get(key) {
    try { return JSON.parse(localStorage.getItem('impulsiona_' + key)); } catch { return null; }
  },
  set(key, val) {
    localStorage.setItem('impulsiona_' + key, JSON.stringify(val));
    this._dirty.add(key);
  },
  async syncToServer(key) {
    const data = this.get(key);
    if (!data) return;
    try {
      await fetch(`/api/sync/${key}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
      });
      this._dirty.delete(key);
    } catch(e) { console.error('[Cache] sync error:', e); }
  }
};

// ===== SYNC ENGINE =====
const Sync = {
  _channel: null,
  _listeners: {},
  _initialized: false,
  init() {
    if (this._initialized) return;
    this._initialized = true;
    try { this._channel = new BroadcastChannel('impulsiona_sync'); this._channel.onmessage = (e) => this._handleMessage(e.data); } catch(err) {}
    window.addEventListener('storage', (e) => {
      if (e.key && e.key.startsWith('impulsiona_')) {
        const key = e.key.replace('impulsiona_', '');
        this._notifyListeners(key, e.newValue ? JSON.parse(e.newValue) : null);
      }
    });
  },
  broadcast(type, data) {
    this.init();
    const payload = { type, data, timestamp: Date.now() };
    try { this._channel?.postMessage(payload); } catch(e) {}
    this._notifyListeners(type, data);
  },
  on(type, cb) { this.init(); if (!this._listeners[type]) this._listeners[type] = []; this._listeners[type].push(cb); },
  off(type, cb) { if (this._listeners[type]) this._listeners[type] = this._listeners[type].filter(c => c !== cb); },
  _handleMessage(p) { this._notifyListeners(p.type, p.data); this._notifyListeners('*', p.data); },
  _notifyListeners(key, data) {
    (this._listeners[key]||[]).forEach(cb => { try { cb(data); } catch(e) {} });
    (this._listeners['*']||[]).forEach(cb => { try { cb(key, data); } catch(e) {} });
  }
};

// ===== LIVE REFRESH ENGINE =====
const Live = {
  _callbacks: {}, _paused: false, _interval: 3000,
  start(ms = 3000) { this.stop(); this._interval = ms; this._mainTimer = setInterval(() => { if (this._paused || document.hidden) return; Object.values(this._callbacks).forEach(cb => { if (cb?.refresh) cb.refresh(); }); }, ms); },
  register(id, fn) { this._callbacks[id] = fn; },
  unregister(id) { delete this._listeners[id]; },
  pause() { this._paused = true; },
  resume() { this._paused = false; },
  stop() { if (this._mainTimer) clearInterval(this._mainTimer); this._callbacks = {}; },
  showUpdateBanner(message, onClick) {
    let b = document.getElementById('live-update-banner');
    if (b) b.remove();
    b = document.createElement('div'); b.id = 'live-update-banner';
    b.style.cssText = 'position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:var(--primary);color:#fff;padding:12px 24px;border-radius:8px;box-shadow:0 4px 20px rgba(0,0,0,0.2);cursor:pointer;z-index:9999;font-size:0.9rem;font-weight:600;';
    b.innerHTML = '🔄 ' + message;
    b.onclick = () => { b.remove(); if (onClick) onClick(); };
    document.body.appendChild(b);
    setTimeout(() => { if (b.parentElement) b.remove(); }, 8000);
  }
};

// ===== DB LAYER (localStorage + socket.io sync) =====
const DB = {
  get(key) {
    try { return JSON.parse(localStorage.getItem('impulsiona_' + key)) || null; } catch { return null; }
  },
  set(key, value) {
    localStorage.setItem('impulsiona_' + key, JSON.stringify(value));
    Sync.broadcast(key, value);
  },
  remove(key) { localStorage.removeItem('impulsiona_' + key); },
  getUsers() { return this.get('users') || []; },
  setUsers(u) { this.set('users', u); },
  getCurrentUser() { try { return JSON.parse(localStorage.getItem('impulsiona_currentUser')); } catch { return null; } },
  setCurrentUser(u) { if (u) localStorage.setItem('impulsiona_currentUser', JSON.stringify(u)); else localStorage.removeItem('impulsiona_currentUser'); },
  getJobs() { return this.get('jobs') || []; },
  setJobs(j) { this.set('jobs', j); },
  getResumes() { return this.get('resumes') || []; },
  setResumes(r) { this.set('resumes', r); },
  getApplications() { return this.get('applications') || []; },
  setApplications(a) { this.set('applications', a); },
  getMessages() { return this.get('messages') || []; },
  setMessages(m) { this.set('messages', m); },
  getNotifications() { return this.get('notifications') || []; },
  setNotifications(n) { this.set('notifications', n); },
  generateId() { return Date.now().toString(36) + Math.random().toString(36).substr(2, 9); }
};

// ===== SEED DATA =====
function seedData() {
  const SEED_VERSION = 4;
  if (DB.get('seeded') === SEED_VERSION) return;
  const SEED_IDS = ['c1', 'c2', 'c3', 'c4', 'c5', 'u1', 'u2', 'u3'];
  const existingUsers = DB.getUsers() || [];
  const realUsers = existingUsers.filter(u => !SEED_IDS.includes(u.id));
  DB.setUsers(realUsers);
  DB.setJobs(DB.getJobs() || []);
  DB.setResumes(DB.getResumes() || []);
  DB.setApplications(DB.getApplications() || []);
  DB.setMessages(DB.getMessages() || []);
  DB.setNotifications(DB.getNotifications() || []);
  DB.set('seeded', SEED_VERSION);
}
seedData();

// ===== SERVER SYNC =====
let _isSyncing = false;

async function syncToServer() {
  if (_isSyncing) return;
  try {
    const data = {
      users: DB.getUsers(),
      jobs: DB.getJobs(),
      applications: DB.getApplications(),
      messages: DB.getMessages(),
      notifications: DB.getNotifications(),
      resumes: DB.getResumes()
    };
    await fetch('/api/sync/push', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
  } catch(e) {}
}

async function syncFromServer() {
  if (_isSyncing) return;
  _isSyncing = true;
  try {
    const r = await fetch('/api/sync/pull');
    const data = await r.json();
    if (data.users && data.users.length) DB.setUsers(data.users);
    if (data.jobs && data.jobs.length) DB.setJobs(data.jobs);
    if (data.applications && data.applications.length) DB.setApplications(data.applications);
    if (data.messages && data.messages.length) DB.setMessages(data.messages);
    if (data.notifications && data.notifications.length) DB.setNotifications(data.notifications);
    if (data.resumes && data.resumes.length) DB.setResumes(data.resumes);
    console.log('[SYNC] Dados sincronizados do servidor');
  } catch(e) { console.error('[SYNC] Erro:', e); }
  _isSyncing = false;
  Sync.broadcast('sync_complete', true);
}

// Listen for server pushes
socket.on('db_change', (payload) => {
  syncFromServer();
});

// Initial sync + periodic push
(async function() {
  await syncFromServer();
  await syncToServer();
  setInterval(syncToServer, 3000);
})();

// Push to server on every DB write (debounced)
let _syncTimeout = null;
const _origSet = DB.set.bind(DB);
DB.set = function(key, value) {
  _origSet(key, value);
  if (_isSyncing) return;
  if (_syncTimeout) clearTimeout(_syncTimeout);
  _syncTimeout = setTimeout(() => syncToServer(), 500);
};

// ===== AUTH MODULE =====
const Auth = {
  login(email, password) {
    const users = DB.getUsers();
    const user = users.find(u => u.email === email && u.password === password);
    if (user) { DB.setCurrentUser(user); return { success: true, user }; }
    return { success: false, error: 'Email ou senha incorretos' };
  },
  register(data) {
    const users = DB.getUsers();
    if (users.find(u => u.email === data.email)) return { success: false, error: 'Este email já está cadastrado' };
    const user = { id: DB.generateId(), ...data, createdAt: new Date().toISOString().split('T')[0] };
    users.push(user);
    DB.setUsers(users);
    DB.setCurrentUser(user);
    return { success: true, user };
  },
  logout() { DB.remove('currentUser'); localStorage.removeItem('impulsiona_currentUser'); window.location.href = 'index.html'; },
  deleteAccount() {
    const user = this.getUser();
    if (!user) return;
    if (!confirm('Tem certeza que deseja excluir sua conta? Todos os seus dados serão perdidos permanentemente.')) return;

    fetch('/api/users/' + user.id, { method: 'DELETE' }).then(() => {
      const users = DB.getUsers().filter(u => u.id !== user.id);
      DB.setUsers(users);
      if (user.type === 'company') {
        DB.setJobs(DB.getJobs().filter(j => j.companyId !== user.id));
        DB.setApplications(DB.getApplications().filter(a => { const job = (DB.getJobs() || []).find(j => j.id === a.jobId); return job && job.companyId !== user.id; }));
      } else {
        DB.setApplications(DB.getApplications().filter(a => a.userId !== user.id));
        DB.setResumes(DB.getResumes().filter(r => r.userId !== user.id));
      }
      DB.setMessages(DB.getMessages().filter(m => m.from !== user.id && m.to !== user.id));
      DB.setNotifications(DB.getNotifications().filter(n => n.userId !== user.id));
      DB.remove('currentUser');
      localStorage.removeItem('impulsiona_currentUser');
      showToast('Conta excluída com sucesso', 'success');
      setTimeout(() => window.location.href = 'index.html', 600);
    }).catch(e => {
      showToast('Erro ao excluir: ' + e.message, 'error');
    });
  },
  getUser() { return DB.getCurrentUser(); },
  isLoggedIn() { return !!DB.getCurrentUser(); },
  isCompany() { const u = this.getUser(); return u && u.type === 'company'; },
  isCandidate() { const u = this.getUser(); return u && u.type === 'candidate'; },
  hasActiveSubscription() {
    const u = this.getUser();
    if (!u || u.type !== 'company') return false;
    if (u.subscription && (u.subscription.status === 'authorized' || u.subscription.status === 'active')) {
      if (u.subscription.expiresAt) return new Date(u.subscription.expiresAt) > new Date();
      return true;
    }
    return false;
  },
  updateSubscription(subscriptionData) {
    const user = this.getUser();
    if (!user) return false;
    user.subscription = subscriptionData;
    DB.setCurrentUser(user);
    const users = DB.getUsers();
    const idx = users.findIndex(u => u.id === user.id);
    if (idx >= 0) { users[idx] = user; DB.setUsers(users); }
    return true;
  },
};

// ===== JOBS MODULE =====
const Jobs = {
  getAll(filters = {}) {
    let jobs = DB.getJobs().filter(j => j.status === 'active');
    if (filters.search) {
      const s = filters.search.toLowerCase();
      jobs = jobs.filter(j => j.title.toLowerCase().includes(s) || j.company.toLowerCase().includes(s) || j.location.toLowerCase().includes(s) || (j.requirements||[]).some(r => r.toLowerCase().includes(s)));
    }
    if (filters.location) jobs = jobs.filter(j => (j.location||'').toLowerCase().includes(filters.location.toLowerCase()));
    if (filters.category) jobs = jobs.filter(j => j.category === filters.category);
    if (filters.experience) jobs = jobs.filter(j => j.experience === filters.experience);
    if (filters.regime) jobs = jobs.filter(j => j.regime === filters.regime);
    if (filters.type) jobs = jobs.filter(j => j.type === filters.type);
    if (filters.salaryMin) jobs = jobs.filter(j => j.salaryMax >= parseInt(filters.salaryMin));
    if (filters.companyId) jobs = jobs.filter(j => j.companyId === filters.companyId);
    return jobs.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  },
  getById(id) { return DB.getJobs().find(j => j.id === id); },
  create(data) {
    const jobs = DB.getJobs();
    const user = Auth.getUser();
    const job = { id: DB.generateId(), companyId: user.id, company: user.company || user.name, ...data, createdAt: new Date().toISOString().split('T')[0], status: 'active' };
    jobs.push(job);
    DB.setJobs(jobs);
    return job;
  },
  update(id, data) {
    const jobs = DB.getJobs();
    const idx = jobs.findIndex(j => j.id === id);
    if (idx >= 0) { jobs[idx] = { ...jobs[idx], ...data }; DB.setJobs(jobs); return jobs[idx]; }
    return null;
  },
  delete(id) {
    DB.setJobs(DB.getJobs().filter(j => j.id !== id));
    DB.setApplications(DB.getApplications().filter(a => a.jobId !== id));
    DB.setMessages(DB.getMessages().filter(m => m.jobId !== id));
  },
  getCompanyJobs(companyId) { return DB.getJobs().filter(j => j.companyId === companyId).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt)); },
  getStats(companyId) {
    const jobs = this.getCompanyJobs(companyId);
    const apps = DB.getApplications();
    return { totalJobs: jobs.length, activeJobs: jobs.filter(j => j.status === 'active').length, totalApplications: apps.filter(a => jobs.some(j => j.id === a.jobId)).length, pendingApplications: apps.filter(a => jobs.some(j => j.id === a.jobId) && a.status === 'pending').length };
  },
  getCategoryCount() { const jobs = DB.getJobs().filter(j => j.status === 'active'); const counts = {}; jobs.forEach(j => { counts[j.category] = (counts[j.category] || 0) + 1; }); return counts; },
  getStatsGlobal() {
    const jobs = DB.getJobs().filter(j => j.status === 'active');
    const users = DB.getUsers().filter(u => u.type === 'candidate');
    return { totalJobs: jobs.length, totalCompanies: DB.getUsers().filter(u => u.type === 'company').length, totalCandidates: users.length, totalApplications: DB.getApplications().length };
  }
};

// ===== RESUME MODULE =====
const Resume = {
  getByUserId(userId) { return DB.getResumes().find(r => r.userId === userId); },
  save(data) {
    const resumes = DB.getResumes();
    const user = Auth.getUser();
    const existing = resumes.findIndex(r => r.userId === user.id);
    const resume = { ...data, userId: user.id, updatedAt: new Date().toISOString().split('T')[0] };
    if (existing >= 0) { resumes[existing] = { ...resumes[existing], ...resume }; }
    else { resume.id = DB.generateId(); resume.createdAt = new Date().toISOString().split('T')[0]; resumes.push(resume); }
    DB.setResumes(resumes);
    return resume;
  },
  getById(id) { return DB.getResumes().find(r => r.id === id); },
  getCompletion(resume) {
    if (!resume) return 0;
    let fields = 0, filled = 0;
    const check = (v) => { fields++; if (v && (Array.isArray(v) ? v.length > 0 : true)) filled++; };
    check(resume.name); check(resume.email); check(resume.phone); check(resume.title); check(resume.summary);
    check(resume.experience); check(resume.education); check(resume.skills); check(resume.languages);
    return fields > 0 ? Math.round((filled / fields) * 100) : 0;
  }
};

// ===== APPLICATIONS MODULE =====
const Applications = {
  apply(jobId, resumeId, coverLetter, pdfData, pdfName) {
    const apps = DB.getApplications();
    const user = Auth.getUser();
    if (apps.find(a => a.jobId === jobId && a.userId === user.id)) return { success: false, error: 'Você já se candidatou para esta vaga' };
    const app = { id: DB.generateId(), jobId, userId: user.id, resumeId, coverLetter: coverLetter || '', resumePdf: pdfData || null, resumePdfName: pdfName || '', status: 'pending', appliedAt: new Date().toISOString().split('T')[0], updatedAt: new Date().toISOString().split('T')[0] };
    apps.push(app);
    DB.setApplications(apps);
    Notifications.add(user.id, 'Candidatura enviada com sucesso!');
    return { success: true, application: app };
  },
  getByUser(userId) { return DB.getApplications().filter(a => a.userId === userId); },
  getByJob(jobId) { return DB.getApplications().filter(a => a.jobId === jobId); },
  getByCompany(companyId) { const jobs = Jobs.getCompanyJobs(companyId); const jobIds = jobs.map(j => j.id); return DB.getApplications().filter(a => jobIds.includes(a.jobId)); },
  updateStatus(appId, status) {
    const apps = DB.getApplications();
    const idx = apps.findIndex(a => a.id === appId);
    if (idx >= 0) { apps[idx].status = status; apps[idx].updatedAt = new Date().toISOString().split('T')[0]; DB.setApplications(apps); Notifications.add(apps[idx].userId, `Sua candidatura foi atualizada para: ${getStatusText(status)}`); return apps[idx]; }
    return null;
  },
  hasApplied(jobId, userId) { return DB.getApplications().some(a => a.jobId === jobId && a.userId === userId); },
  getStats(userId) {
    const apps = this.getByUser(userId);
    return { total: apps.length, pending: apps.filter(a => a.status === 'pending').length, viewed: apps.filter(a => a.status === 'viewed').length, accepted: apps.filter(a => a.status === 'accepted').length, rejected: apps.filter(a => a.status === 'rejected').length };
  },
  delete(appId) { DB.setApplications(DB.getApplications().filter(a => a.id !== appId)); }
};

// ===== MESSAGES MODULE =====
const Messages = {
  send(from, to, jobId, text) {
    const msgs = DB.getMessages();
    const msg = { id: DB.generateId(), from, to, jobId, text, timestamp: new Date().toISOString(), read: false };
    msgs.push(msg);
    DB.setMessages(msgs);
    const sender = (DB.getUsers() || []).find(u => u.id === from);
    if (sender) Notifications.add(to, `Nova mensagem de ${sender.name}: "${text.substring(0, 60)}${text.length > 60 ? '...' : ''}"`);
    return msg;
  },
  getConversation(userId1, userId2, jobId) {
    return DB.getMessages().filter(m => m.jobId === jobId && ((m.from === userId1 && m.to === userId2) || (m.from === userId2 && m.to === userId1))).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  },
  getUnreadCount(userId) { return DB.getMessages().filter(m => m.to === userId && !m.read).length; },
  markRead(userId, otherId, jobId) {
    const msgs = DB.getMessages();
    msgs.forEach(m => { if (m.to === userId && m.from === otherId && m.jobId === jobId && !m.read) m.read = true; });
    DB.setMessages(msgs);
  },
  getConversations(userId) {
    const msgs = DB.getMessages();
    const convos = {};
    msgs.forEach(m => {
      if (m.from === userId || m.to === userId) {
        const otherId = m.from === userId ? m.to : m.from;
        const key = `${otherId}_${m.jobId}`;
        if (!convos[key] || new Date(m.timestamp) > new Date(convos[key].timestamp)) convos[key] = { otherId, jobId: m.jobId, text: m.text, timestamp: m.timestamp, read: m.read };
      }
    });
    return Object.values(convos).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  },
  deleteByJob(jobId) { DB.setMessages(DB.getMessages().filter(m => m.jobId !== jobId)); },
  deleteConversation(userId, otherId, jobId) { DB.setMessages(DB.getMessages().filter(m => !(m.jobId === jobId && ((m.from === userId && m.to === otherId) || (m.from === otherId && m.to === userId))))); }
};

// ===== NOTIFICATIONS MODULE =====
const Notifications = {
  add(userId, text) { const notifs = DB.getNotifications(); notifs.push({ id: DB.generateId(), userId, text, read: false, createdAt: new Date().toISOString() }); DB.setNotifications(notifs); },
  getByUser(userId) { return DB.getNotifications().filter(n => n.userId === userId).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt)); },
  getUnreadCount(userId) { return DB.getNotifications().filter(n => n.userId === userId && !n.read).length; },
  markAllRead(userId) { const notifs = DB.getNotifications(); notifs.forEach(n => { if (n.userId === userId) n.read = true; }); DB.setNotifications(notifs); }
};

// ===== HELPERS =====
function getStatusText(status) { const map = { pending: 'Pendente', viewed: 'Visualizado', accepted: 'Aceito', rejected: 'Rejeitado', cancelled: 'Cancelado' }; return map[status] || status; }
function getStatusClass(status) { const map = { pending: 'tag-orange', viewed: 'tag-blue', accepted: 'tag-green', rejected: 'tag-red', cancelled: 'tag-gray' }; return map[status] || 'tag-gray'; }
function timeAgo(dateStr) { const now = new Date(); const date = new Date(dateStr); const diff = Math.floor((now - date) / 1000); if (diff < 60) return 'agora mesmo'; if (diff < 3600) return `há ${Math.floor(diff/60)} min`; if (diff < 86400) return `há ${Math.floor(diff/3600)}h`; if (diff < 604800) return `há ${Math.floor(diff/86400)} dias`; return date.toLocaleDateString('pt-BR'); }
function formatDate(dateStr) { return dateStr ? new Date(dateStr).toLocaleDateString('pt-BR') : ''; }
function formatMoney(value) { return value.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }); }
function getInitials(name) { return name ? name.split(' ').map(w => w[0]).join('').substring(0, 2).toUpperCase() : '?'; }
function getCategoryIcon(cat) { const map = {'Tecnologia':'💻','Marketing':'📢','Design':'🎨','Gestão':'📊','Financeiro':'💰','Saúde':'🏥','Educação':'📚','Engenharia':'⚙️','Jurídico':'⚖️','Vendas':'🤝','RH':'👥','Logística':'🚚'}; return map[cat] || '💼'; }
function getRegimeIcon(r) { const map = {'Remoto':'🏠','Híbrido':'🔄','Presencial':'🏢'}; return map[r] || '📍'; }

// ===== TOAST =====
function showToast(message, type = 'info') {
  let container = document.getElementById('toast-container');
  if (!container) { container = document.createElement('div'); container.id = 'toast-container'; container.className = 'toast-container'; document.body.appendChild(container); }
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.innerHTML = message;
  container.appendChild(toast);
  setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 3500);
}

// ===== NAV =====
function renderNavbar() {
  const navbar = document.getElementById('navbar');
  if (!navbar) return;
  const user = Auth.getUser();
  const unreadMsgs = user ? Messages.getUnreadCount(user.id) : 0;
  const unreadNotifs = user ? Notifications.getUnreadCount(user.id) : 0;
  let navLinks = '<a href="index.html">Início</a><a href="jobs.html">Vagas</a>';
  if (!user) { navLinks += '<a href="login.html" class="btn-nav">Entrar</a><a href="register.html" style="background:rgba(255,255,255,0.15);">Cadastrar</a>'; }
  else if (user.type === 'candidate') { navLinks += '<a href="candidate-dashboard.html">Dashboard</a><a href="my-applications.html">Candidaturas</a>'; }
  else { navLinks += '<a href="company-dashboard.html">Dashboard</a><a href="company-jobs.html">Minhas Vagas</a><a href="create-job.html" class="btn-nav">+ Nova Vaga</a>'; }
  let userMenu = '';
  if (user) {
    const nb = unreadNotifs > 0 ? `<span style="background:#dc3545;color:#fff;border-radius:50%;padding:1px 6px;font-size:0.65rem;position:absolute;top:-4px;right:-4px;">${unreadNotifs}</span>` : '';
    userMenu = `<div class="nav-dropdown"><div class="nav-user" onclick="toggleDropdown(this)"><div class="avatar">${getInitials(user.name)}</div><span style="font-size:0.85rem;">${user.name.split(' ')[0]}</span>${nb}</div><div class="nav-dropdown-menu" id="user-dropdown">${user.type === 'candidate' ? `<a href="candidate-dashboard.html">Meu Painel</a><a href="resume.html">Meu Currículo</a><a href="my-applications.html">Candidaturas</a><a href="messages.html">Mensagens ${unreadMsgs > 0 ? `<span class="badge badge-urgent" style="margin-left:4px;">${unreadMsgs}</span>` : ''}</a><a href="notifications.html">Notificações ${unreadNotifs > 0 ? `<span class="badge badge-urgent" style="margin-left:4px;">${unreadNotifs}</span>` : ''}</a>` : `<a href="company-dashboard.html">Meu Painel</a><a href="company-jobs.html">Minhas Vagas</a><a href="company-applications.html">Candidaturas Recebidas</a><a href="messages.html">Mensagens ${unreadMsgs > 0 ? `<span class="badge badge-urgent" style="margin-left:4px;">${unreadMsgs}</span>` : ''}</a>`}<div class="divider"></div><button onclick="Auth.logout()">Sair</button></div></div>`;
  }
  navbar.innerHTML = `<div class="logo" onclick="location.href='index.html'">Impulsiona <span>Vagas</span></div><nav>${navLinks}${userMenu}</nav>`;
}

function toggleDropdown(el) {
  const menu = el.nextElementSibling;
  menu.classList.toggle('show');
  document.addEventListener('click', function close(e) {
    if (!el.contains(e.target) && !menu.contains(e.target)) { menu.classList.remove('show'); document.removeEventListener('click', close); }
  });
}

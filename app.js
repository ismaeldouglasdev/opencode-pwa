// O server injeta o valor real de APP_AUTH ao servir este arquivo.
const APP_AUTH = '__APP_AUTH__';

function app() {
  return {
    // --- views: 'home' (dashboard atividade), 'sessions', 'session' ---
    view: 'home',
    projects: [],
    sessions: [],
    messages: [],
    currentProject: null,
    currentSession: null,
    prompt: '',
    loading: false,
    error: '',

    // --- Nova sessão ---
    newSessionModal: false,
    newSessionTitle: '',
    creating: false,

    // --- Modelo por sessão ---
    providers: [],        // providers carregados de /config/providers
    modelGroups: [],      // agrupados para o <select>
    selectedModel: '',    // id do modelo escolhido p/ próxima mensagem (string "providerID|modelID")

    // --- Tempo real ---
    pollTimer: null,
    eventSources: {},       // sessionId -> EventSource
    live: {},               // sessionId -> estado ao vivo (streaming)
    homePollTimer: null,
    optimistic: {},         // msgId -> { text } mensagens otimistas ainda não confirmadas pelo servidor

    // --- Logs do frontend ---
    logBuffer: [],          // anel: últimos 300 eventos (exibidos no painel Logs)
    logsOpen: false,

    log(level, event, data) {
      const entry = { ts: Date.now(), level: level, event: event, data: data || null };
      this.logBuffer.push(entry);
      if (this.logBuffer.length > 300) this.logBuffer = this.logBuffer.slice(-300);
      try { console.log('[app:' + level + '] ' + event, data || ''); } catch (e) {}
      return entry;
    },
    toggleLogs() { this.logsOpen = !this.logsOpen; },
    fmtLogTime(ts) {
      const d = new Date(ts);
      return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0');
    },

    // ================= INICIALIZAÇÃO =================
    async init() {
      this.log('info', 'app.init');
      this.error = '';
      try {
        this.projects = await this.fetchProjects();
      } catch (e) { this.error = 'Erro ao carregar projetos: ' + e.message; this.log('error', 'projects.load.failed', { message: e.message }); }
      this.loadProviders();
      this.startHomeMonitor();
    },

    async api(path, method, body) {
      // O proxy exige auth Basic (mesma do auth.local.json); o server injeta o
      // valor real no lugar de APP_AUTH ao servir este arquivo.
      const opts = {
        method: method || 'GET',
        headers: { 'Authorization': APP_AUTH },
      };
      if (body) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
      const t0 = Date.now();
      try {
        const res = await fetch(path, opts);
        if (!res.ok) throw new Error('HTTP ' + res.status);
        const json = await res.json();
        this.log('debug', 'api.ok', { method: opts.method, path, durationMs: Date.now() - t0 });
        return json;
      } catch (e) {
        this.log('warn', 'api.error', { method: opts.method, path, message: e.message, durationMs: Date.now() - t0 });
        throw e;
      }
    },

    async fetchProjects() {
      const data = await this.api('/project');
      return Array.isArray(data) ? data : (data.data || []);
    },

    async fetchSessions(projectId) {
      const data = await this.api('/api/session?limit=50');
      const list = Array.isArray(data) ? data : (data.data || []);
      if (projectId && projectId !== 'global') {
        const filtered = list.filter(s => s.projectID === projectId);
        if (filtered.length) return filtered;
      }
      // ordena por atividade (updated desc)
      return list.sort((a, b) => (b.time?.updated || 0) - (a.time?.updated || 0));
    },

    async fetchMessages(sessionId) {
      const data = await this.api(`/api/session/${sessionId}/message?limit=100`);
      let list = Array.isArray(data) ? data : (data.data || []);
      return list.reverse(); // ordem cronológica
    },

    // Mescla mensagens do servidor com as otimistas ainda não confirmadas.
    // Evita que o poll de 2s apague a mensagem do usuário antes do servidor confirmá-la.
    mergeMessages(fetched) {
      const ids = new Set(fetched.map(m => m.id));
      const pending = Object.keys(this.optimistic).filter(id => !ids.has(id));
      if (!pending.length) return fetched;
      const extra = pending.map(id => ({
        id: id,
        type: 'user',
        role: 'user',
        text: this.optimistic[id].text,
        time: { created: this.optimistic[id].created },
        pending: true
      }));
      return fetched.concat(extra);
    },

    // ================= MODELOS / PROVIDERS =================
    // Carrega /config/providers e monta os grupos do seletor.
    // Cada opção tem value "providerID|modelID" e label "model (provider)".
    async loadProviders() {
      try {
        const data = await this.api('/config/providers');
        const provs = Array.isArray(data) ? data : (data.providers || []);
        this.providers = provs;
        this.modelGroups = provs.map(p => {
          const models = Object.values(p.models || {});
          return {
            provider: p.id,
            label: `${p.name || p.id} (${models.length})`,
            models: models.map(m => ({
              id: m.id,
              value: p.id + '|' + m.id,
              label: m.name || m.id
            }))
          };
        });
      } catch (e) { /* seletor fica vazio, sem quebrar a app */ }
    },

    // Modelo escolhido para a sessão atual (ou null)
    currentModelChoice() {
      if (!this.selectedModel) return null;
      const [providerID, modelID] = this.selectedModel.split('|');
      if (!providerID || !modelID) return null;
      return { providerID, modelID };
    },

    onModelChange() {
      // ao trocar, guarda no live (só para refletir na UI)
      if (this.currentSession) {
        this.live = Object.assign({}, this.live);
      }
    },

    // ================= DASHBOARD ATIVIDADE =================
    // Polling de /api/session a cada 3s + SSE para as top-5 sessões ativas
    startHomeMonitor() {
      if (this.homePollTimer) clearInterval(this.homePollTimer);
      this.homePollTimer = setInterval(async () => {
        try {
          const sessions = await this.fetchSessions(null);
          if (this.view === 'home' || this.view === 'sessions') this.sessions = sessions;
          this.ensureLiveStreams(sessions);
        } catch (e) { /* silencioso */ }
      }, 3000);
      // primeira execução imediata
      this.fetchSessions(null).then(s => {
        this.sessions = s;
        this.ensureLiveStreams(s);
      }).catch(() => {});
    },

    // Mantém SSE aberto para as sessões mais ativas (até 5)
    ensureLiveStreams(sessions) {
      const now = Date.now();
      // sessões com atividade nos últimos 3 minutos (ou top 5)
      const active = sessions
        .filter(s => now - (s.time?.updated || 0) < 180000)
        .slice(0, 5);
      const ids = new Set(active.map(s => s.id));

      // fecha SSE de sessões que saíram da lista
      for (const sid of Object.keys(this.eventSources)) {
        if (!ids.has(sid) && sid !== (this.currentSession?.id || '')) {
          this.eventSources[sid].close();
          delete this.eventSources[sid];
          if (this.live[sid]) this.live[sid].stale = true;
        }
      }

      // abre SSE para novas sessões ativas
      for (const s of active) {
        if (!this.eventSources[s.id]) {
          this.openEventStream(s.id);
        }
      }
    },

    // Abre o SSE de UMA sessão e alimenta this.live[sessionId]
    openEventStream(sessionId) {
      try {
        const es = new EventSource(`/api/session/${sessionId}/event`);
        this.eventSources[sessionId] = es;
        if (!this.live[sessionId]) {
          this.live[sessionId] = { id: sessionId, title: '', state: 'idle', events: [] };
        }
        // Marca o momento da conexão — eventos mais antigos que isso são replay e devem ser IGNORADOS
        this.live[sessionId].connectedAt = Date.now();
        this.log('debug', 'sse.open', { sessionId: sessionId.slice(0, 12) });
        es.onmessage = (msg) => {
          try {
            const ev = JSON.parse(msg.data);
            this.handleEvent(sessionId, ev);
          } catch (e) { this.log('warn', 'sse.parse.error', { message: e.message }); }
        };
        es.onerror = () => { this.log('warn', 'sse.error', { sessionId: sessionId.slice(0, 12) }); };
      } catch (e) { this.log('error', 'sse.open.failed', { sessionId: sessionId.slice(0, 12), message: e.message }); }
    },

    // Processa um evento SSE e atualiza o estado ao vivo
    handleEvent(sessionId, ev) {
      const type = ev.type || '';
      const d = ev.data || {};
      const L = this.live[sessionId] || (this.live[sessionId] = { id: sessionId, title: '', state: 'idle', events: [] });

      // FILTRO DE REPLAY: ignora eventos com timestamp anterior à conexão do SSE.
      // O servidor faz replay do histórico completo ao conectar — processá-los
      // sobrescreveria o estado ao vivo com dados antigos.
      if (L.connectedAt && d.timestamp && d.timestamp < L.connectedAt) {
        return;
      }

      this.log('debug', 'sse.event', { sessionId: sessionId.slice(0, 12), type });
      L.lastEventAt = Date.now();
      L.stale = false;

      // título da sessão vem do polling; fallback: sessionID curto
      if (!L.title) L.title = sessionId.slice(0, 16);

      if (type === 'session.next.prompt.admitted' || type === 'session.next.prompted') {
        L.state = 'waiting';
        L.lastPrompt = d.prompt?.text || '';
        L.step = null; L.reasoning = null; L.tools = []; L.text = null; L.stepEnded = null;
        L.stepStartedAt = Date.now();
      }
      else if (type === 'session.next.step.started') {
        L.state = 'thinking';
        L.step = { agent: d.agent, model: d.model, startedAt: Date.now() };
        L.assistantMessageID = d.assistantMessageID;
        L.reasoning = null; L.tools = []; L.text = null; L.stepEnded = null;
        L.stepStartedAt = Date.now();
      }
      else if (type === 'session.next.reasoning.started') {
        L.state = 'thinking';
        if (!L.reasoning) L.reasoning = { startedAt: Date.now(), text: '' };
      }
      else if (type === 'session.next.reasoning.ended') {
        L.state = 'thinking';
        L.reasoning = { startedAt: L.reasoning?.startedAt || Date.now(), text: d.text || '', endedAt: Date.now() };
      }
      else if (type === 'session.next.tool.input.started') {
        L.state = 'running';
        if (!L.tools) L.tools = [];
        L.tools.push({ name: d.name || 'tool', status: 'running', startedAt: Date.now(), input: null, error: null });
        L.currentTool = L.tools[L.tools.length - 1];
      }
      else if (type === 'session.next.tool.called') {
        L.state = 'running';
        if (L.currentTool) {
          L.currentTool.name = d.tool || L.currentTool.name;
          L.currentTool.input = d.input;
        }
      }
      else if (type === 'session.next.tool.input.ended') {
        if (L.currentTool && !L.currentTool.input && d.text) {
          try { L.currentTool.input = JSON.parse(d.text); } catch (e) { L.currentTool.input = d.text; }
        }
      }
      else if (type === 'session.next.tool.failed') {
        L.state = 'running';
        if (L.currentTool) {
          L.currentTool.status = 'error';
          L.currentTool.error = d.error?.message || (typeof d.error === 'string' ? d.error : JSON.stringify(d.error));
          L.currentTool.endedAt = Date.now();
        }
      }
      else if (type === 'session.next.tool.success') {
        L.state = 'running';
        if (L.currentTool) {
          L.currentTool.status = 'completed';
          L.currentTool.endedAt = Date.now();
        }
      }
      else if (type === 'session.next.text.started') {
        L.state = 'responding';
        L.text = { startedAt: Date.now(), text: '' };
      }
      else if (type === 'session.next.text.ended') {
        L.state = 'done';
        L.text = { startedAt: L.text?.startedAt || Date.now(), text: d.text || '', endedAt: Date.now() };
        // Resposta completa: atualiza o chat IMEDIATAMENTE (não espera o poll de 2s)
        if (this.view === 'session' && this.currentSession?.id === sessionId) {
          this.fetchMessages(sessionId).then(m => { this.messages = this.mergeMessages(m); }).catch(() => {});
        }
      }
      else if (type === 'session.next.step.ended') {
        L.stepEnded = { tokens: d.tokens, cost: d.cost, finish: d.finish, endedAt: Date.now() };
        // não rebaixa 'done' para 'idle': resposta concluída deve continuar exibindo ✅
        if (L.state !== 'done') L.state = 'idle';
        if (L.stepStartedAt) L.durationMs = Date.now() - L.stepStartedAt;
      }

      // força re-render do Alpine
      this.live = Object.assign({}, this.live);
    },

    // ================= RENDERIZAÇÃO =================
    escapeHtml(s) {
      return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    },

    fmtDuration(ms) {
      if (!ms) return '';
      if (ms < 1000) return ms + 'ms';
      if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
      return Math.floor(ms / 60000) + 'm ' + Math.floor((ms % 60000) / 1000) + 's';
    },

    fmtModel(model) {
      if (!model) return '';
      if (typeof model === 'string') return model;
      const id = model.id || '';
      const v = model.variant && model.variant !== 'default' ? ' (' + model.variant + ')' : '';
      return id + v;
    },

    fmtTokens(tokens) {
      if (!tokens) return '';
      const parts = [];
      if (tokens.input) parts.push('in ' + tokens.input);
      if (tokens.output) parts.push('out ' + tokens.output);
      if (tokens.reasoning) parts.push('re ' + tokens.reasoning);
      return parts.join(' · ');
    },

    fmtAgo(ts) {
      if (!ts) return '';
      const s = Math.floor((Date.now() - ts) / 1000);
      if (s < 5) return 'agora';
      if (s < 60) return s + 's atrás';
      if (s < 3600) return Math.floor(s / 60) + 'm atrás';
      return Math.floor(s / 3600) + 'h atrás';
    },

    // Status da sessão: ativa (últimos 3 min) ou inativa
    sessionStatus(session) {
      if (!session) return { label: '', cls: '' };
      const s = session.time?.updated ? Math.floor((Date.now() - session.time.updated) / 1000) : Infinity;
      const liveState = this.live[session.id]?.state;
      if (liveState && ['thinking', 'running', 'responding', 'waiting', 'done'].includes(liveState) && !this.live[session.id]?.stale) {
        if (liveState === 'done') return { label: '● Concluído', cls: 'badge-done' };
        return { label: '● Ativo', cls: 'badge-active' };
      }
      if (s < 180) return { label: '● Ativo', cls: 'badge-active' };
      if (s < 3600) return { label: '○ ' + Math.floor(s / 60) + 'm', cls: 'badge-idle' };
      return { label: '○ ' + Math.floor(s / 3600) + 'h', cls: 'badge-idle' };
    },

    liveSummary(L) {
      if (!L) return { label: '', emoji: '' };
      if (L.state === 'thinking') return { label: 'Pensando...', emoji: '🧠' };
      if (L.state === 'running') return { label: 'Executando ' + (L.currentTool?.name || 'tool'), emoji: '⚙️' };
      if (L.state === 'responding') return { label: 'Escrevendo resposta...', emoji: '✍️' };
      if (L.state === 'waiting') return { label: 'Aguardando...', emoji: '⏳' };
      if (L.state === 'done') return { label: 'Concluído', emoji: '✅' };
      return { label: 'Inativo', emoji: '💤' };
    },

    renderUserMsg(m) {
      if (typeof m.text === 'string' && m.text) return this.escapeHtml(m.text);
      if (Array.isArray(m.content)) {
        return m.content.filter(p => p.type === 'text' && p.text).map(p => this.escapeHtml(p.text)).join('\n');
      }
      return '';
    },

    renderAssistantMsg(m) {
      if (!Array.isArray(m.content)) return '<div class="part part-empty">(sem conteúdo)</div>';
      let html = '';
      for (const p of m.content) {
        if (p.type === 'text' && p.text) {
          html += `<div class="part part-text">${this.escapeHtml(p.text)}</div>`;
        } else if (p.type === 'reasoning' && p.text) {
          html += `<details class="part part-reasoning" open><summary>🧠 reasoning</summary><div>${this.escapeHtml(p.text)}</div></details>`;
        } else if (p.type === 'tool') {
          const st = p.state || {};
          const status = st.status || 'running';
          const inputStr = JSON.stringify(st.input || {});
          let outputStr = '';
          if (st.output !== undefined && st.output !== null) outputStr = typeof st.output === 'string' ? st.output : JSON.stringify(st.output);
          else if (st.error) outputStr = 'ERRO: ' + (typeof st.error === 'string' ? st.error : JSON.stringify(st.error));
          else if (st.content) outputStr = typeof st.content === 'string' ? st.content : JSON.stringify(st.content);
          const icon = status === 'completed' ? '✅' : status === 'error' ? '❌' : '⏳';
          html += `<details class="part part-tool" data-status="${status}" open>
            <summary>${icon} <b>${this.escapeHtml(p.name || 'tool')}</b> <span class="tool-status">${status}</span></summary>
            <div class="tool-input">${this.escapeHtml(inputStr.slice(0, 500))}</div>
            ${outputStr ? `<div class="tool-output">${this.escapeHtml(outputStr.slice(0, 800))}</div>` : ''}
          </details>`;
        }
      }
      return html || '<div class="part part-empty">(sem conteúdo)</div>';
    },

    // Card do dashboard com streaming ao vivo
    liveCardHtml(L) {
      if (!L) return '';
      const sum = this.liveSummary(L);
      const agent = L.step?.agent || '';
      const model = this.fmtModel(L.step?.model);
      const duration = L.durationMs ? this.fmtDuration(L.durationMs) : (L.stepStartedAt ? this.fmtDuration(Date.now() - L.stepStartedAt) : '');
      let html = `<div class="live-card" data-state="${L.state}">`;
      html += `<div class="live-card-head"><span class="live-emoji">${sum.emoji}</span> <span class="live-title">${this.escapeHtml(L.title)}</span> <span class="live-state">${sum.label}</span></div>`;
      if (L.lastPrompt) html += `<div class="live-prompt">💬 ${this.escapeHtml(L.lastPrompt.slice(0, 80))}</div>`;
      if (agent || model) html += `<div class="live-meta">🤖 ${this.escapeHtml(agent)} · ⚡ ${this.escapeHtml(model)}</div>`;
      if (L.reasoning?.text) {
        html += `<details class="part part-reasoning" open><summary>🧠 reasoning ${L.reasoning.endedAt ? '' : '(em andamento)'}</summary><div>${this.escapeHtml(L.reasoning.text.slice(0, 300))}</div></details>`;
      } else if (L.reasoning?.startedAt && !L.reasoning.text) {
        html += `<div class="live-thinking">🧠 pensando...</div>`;
      }
      for (const t of (L.tools || []).slice(-3)) {
        const icon = t.status === 'error' ? '❌' : t.status === 'running' ? '⏳' : '✅';
        html += `<div class="live-tool ${t.status}">${icon} ${this.escapeHtml(t.name)} ${t.error ? '— ' + this.escapeHtml(String(t.error).slice(0, 80)) : ''}</div>`;
      }
      if (L.text?.text) {
        html += `<div class="live-text">💬 ${this.escapeHtml(L.text.text.slice(0, 200))}</div>`;
      } else if (L.state === 'responding') {
        html += `<div class="live-thinking">✍️ escrevendo...</div>`;
      }
      if (L.stepEnded) {
        const toks = this.fmtTokens(L.stepEnded.tokens);
        html += `<div class="live-meta">⏱️ ${this.escapeHtml(duration)} · 📊 ${this.escapeHtml(toks)} · ${this.escapeHtml(L.stepEnded.finish || '')}</div>`;
      }
      html += `</div>`;
      return html;
    },

    // ================= NAVEGAÇÃO =================
    async openSessionById(sessionId) {
      const s = this.sessions.find(x => x.id === sessionId) || { id: sessionId, title: sessionId.slice(0, 16) };
      await this.openSession(s);
    },

    showHome() {
      this.view = 'home';
      this.stopSessionRealTime();
    },

    async showSessions() {
      this.error = '';
      this.loading = true;
      try {
        this.sessions = await this.fetchSessions(this.currentProject ? this.currentProject.id : null);
        this.view = 'sessions';
      } catch (e) { this.error = 'Erro ao carregar sessões: ' + e.message; }
      finally { this.loading = false; }
    },

    async selectProject(project) {
      this.currentProject = project;
      await this.showSessions();
    },

    async openSession(session) {
      this.currentSession = session;
      this.selectedModel = '';
      this.error = '';
      this.loading = true;
      this.log('info', 'session.open', { sessionId: session.id.slice(0, 12), title: session.title || '' });
      try {
        this.messages = await this.fetchMessages(session.id);
        this.view = 'session';
        this.startSessionRealTime(session.id);
      } catch (e) { this.error = 'Erro ao carregar mensagens: ' + e.message; this.log('error', 'session.open.failed', { message: e.message }); }
      finally { this.loading = false; }
    },

    // ================= NOVA SESSÃO =================
    openNewSessionModal() {
      this.newSessionTitle = '';
      this.newSessionModal = true;
    },

    async createSession() {
      if (this.creating) return;
      this.creating = true;
      this.error = '';
      try {
        const body = { directory: '/home/ismaeldev' };
        if (this.newSessionTitle.trim()) body.title = this.newSessionTitle.trim();
        this.log('info', 'session.create', { title: body.title || '', directory: body.directory });
        const data = await this.api('/session', 'POST', body);
        const sess = data.data || data;
        this.newSessionModal = false;
        this.log('info', 'session.created', { sessionId: sess.id.slice(0, 12) });
        // adiciona à lista e abre
        this.sessions = [sess, ...this.sessions];
        await this.openSession(sess);
      } catch (e) {
        this.error = 'Erro ao criar sessão: ' + e.message;
        this.log('error', 'session.create.failed', { message: e.message });
      } finally {
        this.creating = false;
      }
    },

    startSessionRealTime(sessionId) {
      this.stopSessionRealTime();
      this.pollTimer = setInterval(async () => {
        try {
          this.messages = this.mergeMessages(await this.fetchMessages(sessionId));
          if (!this.eventSources[sessionId]) this.openEventStream(sessionId);
        } catch (e) { /* silencioso */ }
      }, 2000);
      if (!this.eventSources[sessionId]) this.openEventStream(sessionId);
    },

    stopSessionRealTime() {
      if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
    },

    async sendMessage() {
      if (!this.prompt.trim()) return;
      const sessionId = this.currentSession.id;
      const text = this.prompt.trim();
      const mid = 'msg_' + (crypto.randomUUID ? crypto.randomUUID().replace(/-/g, '') : Math.random().toString(36).slice(2));

      // 1. Otimista: mostra a mensagem do usuário IMEDIATAMENTE (e registra para o merge do poll)
      this.messages.push({ id: mid, type: 'user', role: 'user', text: text, time: { created: Date.now() } });
      this.optimistic[mid] = { text: text, created: Date.now() };
      this.prompt = '';
      this.error = '';

      // 2. Garante SSE aberto ANTES do envio (feedback em tempo real)
      if (!this.eventSources[sessionId]) this.openEventStream(sessionId);

      // 3. Monta o body (modelo escolhido, se houver — define o provider/modelo deste prompt)
      const body = { id: mid, prompt: { text } };
      const modelChoice = this.currentModelChoice();
      if (modelChoice) body.model = modelChoice;
      this.log('info', 'message.send', { sessionId: sessionId.slice(0, 12), model: modelChoice ? modelChoice.providerID + '/' + modelChoice.modelID : 'default', textLength: text.length });

      // 4. Dispara o POST (não bloqueia a UI)
      try {
        await this.api(`/api/session/${sessionId}/prompt`, 'POST', body);
      } catch (e) {
        this.error = 'Erro ao enviar: ' + e.message;
        this.log('error', 'message.send.failed', { message: e.message });
        // remove a mensagem otimista que nunca chegou ao servidor
        delete this.optimistic[mid];
        this.messages = this.messages.filter(m => m.id !== mid);
        return;
      }
    },
  };
}

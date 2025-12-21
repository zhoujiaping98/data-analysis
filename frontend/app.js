const apiBase = "/api";
let token = localStorage.getItem("token") || "";
let activeConversationId = localStorage.getItem("convId") || "";
let currentDatasourceId = localStorage.getItem("datasourceId") || "";
let datasourceList = [];
let activeDsInModal = "";
let chart = null;
let analysisStreaming = "";
let analysisMsgBodyEl = null;
let lastAnalysisText = "";
let lastUserQuestion = "";
let reportContext = { question: "", analysis: "" };
let tableModalState = { query: "", page: 1, pageSize: 50 };
const messageArtifacts = new Map();
const messageIdToQuestion = new Map();
let activeMessageId = 0;
let lastUserMsgEl = null;
let lastChartOption = null;

let lastTableColumns = [];
let lastTableRows = [];


const el = (id) => document.getElementById(id);

function setAuthStatus(text, isError=false) {
  const s = el("authStatus");
  s.textContent = text;
  s.style.color = isError ? "var(--danger)" : "var(--muted)";
}

async function login() {
  const username = el("username").value.trim();
  const password = el("password").value;
  const resp = await fetch(`${apiBase}/auth/login`, {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({username, password})
  });
  if (!resp.ok) {
    setAuthStatus("登录失败", true);
    return;
  }
  const data = await resp.json();
  token = data.access_token;
  localStorage.setItem("token", token);
  setAuthStatus(`已登录：${username}`);
  await refreshDatasources();
  await refreshSchemaTables();
  await refreshUploads();
  await refreshConversations();
  if (!activeConversationId) {
    await newConversation();
  } else {
    await loadConversation(activeConversationId);
  }
}

async function apiFetch(path, options={}) {
  const headers = options.headers || {};
  headers["Authorization"] = `Bearer ${token}`;
  if (currentDatasourceId) headers["X-Datasource-Id"] = currentDatasourceId;
  options.headers = headers;
  const resp = await fetch(`${apiBase}${path}`, options);
  if (!resp.ok) throw new Error(await resp.text());
  return resp;
}

async function refreshDatasources() {
  const sel = el("datasourceSelect");
  if (!sel) return;
  if (!token) {
    sel.innerHTML = "";
    return;
  }
  try {
    const resp = await apiFetch("/datasources");
    const list = await resp.json();
    datasourceList = list || [];
    sel.innerHTML = "";
    if (!list || list.length === 0) return;
    let activeId = currentDatasourceId;
    if (!activeId || !list.find(d => d.id === activeId)) {
      const def = list.find(d => d.is_default);
      activeId = def ? def.id : list[0].id;
      currentDatasourceId = activeId;
      localStorage.setItem("datasourceId", activeId);
    }
    list.forEach(d => {
      const opt = document.createElement("option");
      opt.value = d.id;
      const status = d.training_ok === 0 ? "训练失败" : (d.training_ok === 1 ? "已训练" : "未训练");
      opt.textContent = `${d.name || d.id} (${status})`;
      if (d.id === activeId) opt.selected = true;
      sel.appendChild(opt);
    });
  } catch (e) {
    // ignore
  }
}


function openExportModal() {
  const modal = el("exportModal");
  if (!modal) return;
  modal.classList.add("open");
  updateExportHint();
}

function closeExportModal() {
  const modal = el("exportModal");
  if (!modal) return;
  modal.classList.remove("open");
}

function openSqlModal() {
  const modal = el("sqlModal");
  if (!modal) return;
  const editor = el("sqlEditor");
  if (editor) editor.value = el("sqlBox")?.textContent || "";
  const status = el("sqlEditStatus");
  if (status) status.textContent = "";
  modal.classList.add("open");
}

function closeSqlModal() {
  const modal = el("sqlModal");
  if (!modal) return;
  modal.classList.remove("open");
}

async function runSql() {
  const editor = el("sqlEditor");
  const status = el("sqlEditStatus");
  const sql = (editor?.value || "").trim();
  if (!sql) {
    if (status) status.textContent = "请输入 SQL";
    return;
  }
  if (!activeConversationId || !activeMessageId) {
    if (status) status.textContent = "当前问题未就绪";
    return;
  }
  if (status) status.textContent = "执行中...";
  try {
    const resp = await apiFetch("/sql/execute", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({
        conversation_id: activeConversationId,
        message_id: activeMessageId,
        sql,
        with_analysis: true
      })
    });
    const data = await resp.json();
    el("sqlBox").textContent = data.sql || sql;
    renderTable(data.columns || [], data.rows || []);
    if (!chart) chart = echarts.init(el("chart"));
    chart.clear();
    if (data.chart) {
      lastChartOption = data.chart;
      chart.setOption(data.chart);
      el("chartHint").textContent = "";
    } else {
      lastChartOption = null;
      el("chartHint").textContent = "该问题未生成图表";
    }
    const question = messageIdToQuestion.get(activeMessageId) || lastUserQuestion || "";
    if (activeMessageId && question) messageIdToQuestion.set(activeMessageId, question);
    reportContext.question = question;
    if (data.analysis) {
      addChatMessage("assistant", data.analysis);
      lastAnalysisText = data.analysis;
      reportContext.analysis = data.analysis;
    }
    messageArtifacts.set(activeMessageId, {
      sql: data.sql || sql,
      columns: data.columns || [],
      rows: data.rows || [],
      chart: data.chart || null,
      analysis: data.analysis || "",
      question,
      message_id: activeMessageId
    });
    if (status) status.textContent = "执行完成";
  } catch (e) {
    let msg = e?.message || String(e);
    try {
      const parsed = JSON.parse(msg);
      if (parsed && parsed.detail) msg = parsed.detail;
    } catch (_) {}
    if (status) status.textContent = `执行失败：${msg}`;
  }
}

function updateExportHint() {
  const hint = el("exportHint");
  if (!hint) return;
  const total = lastTableRows.length;
  if (total === 0) {
    hint.textContent = "当前无结果可导出";
    return;
  }
  if (total > 5000) {
    hint.textContent = `当前结果 ${total} 行，建议导出前 500 行（避免浏览器卡顿）`;
  } else if (total > 1000) {
    hint.textContent = `当前结果 ${total} 行，导出可能耗时，请耐心等待`;
  } else {
    hint.textContent = `当前结果 ${total} 行`;
  }
}

function getExportRows(rangeValue) {
  if (rangeValue === "all") return lastTableRows;
  const limit = Number(rangeValue || 0);
  if (!limit) return lastTableRows;
  return lastTableRows.slice(0, limit);
}

function getExportFilename(ext) {
  const base = (el("exportFilename")?.value || "result").trim() || "result";
  return base.replace(/[^A-Za-z0-9_一-龥-]+/g, "_") + ext;
}

function openDsModal() {
  const modal = el("dsModal");
  if (!modal) return;
  modal.classList.add("open");
  renderDsList();
}

function closeDsModal() {
  const modal = el("dsModal");
  if (!modal) return;
  modal.classList.remove("open");
}

function renderDsList() {
  const listEl = el("dsList");
  const detailEl = el("dsDetail");
  if (!listEl || !detailEl) return;
  listEl.innerHTML = "";
  if (!datasourceList || datasourceList.length === 0) {
    listEl.innerHTML = "<div class='muted'>暂无数据源</div>";
    detailEl.textContent = "请选择一个数据源";
    return;
  }
  datasourceList.forEach(ds => {
    const item = document.createElement("div");
    item.className = "ds-item" + (ds.id === activeDsInModal ? " active" : "");
    const meta = document.createElement("div");
    meta.className = "ds-meta";
    const name = document.createElement("div");
    name.className = "ds-name";
    name.textContent = ds.name || ds.id;
    const status = document.createElement("div");
    status.className = "ds-status";
    const s = ds.training_ok === 0 ? "训练失败" : (ds.training_ok === 1 ? "已训练" : "未训练");
    status.textContent = `${ds.type} · ${s}`;
    meta.appendChild(name);
    meta.appendChild(status);
    const badge = document.createElement("div");
    badge.className = "ds-badge";
    badge.textContent = ds.is_default ? "默认" : "可选";
    item.appendChild(meta);
    item.appendChild(badge);
    item.onclick = () => selectDsInModal(ds.id);
    listEl.appendChild(item);
  });
  if (!activeDsInModal) {
    selectDsInModal(datasourceList[0].id);
  }
}

function selectDsInModal(dsId) {
  activeDsInModal = dsId;
  renderDsList();
  const ds = datasourceList.find(d => d.id === dsId);
  const detailEl = el("dsDetail");
  if (!detailEl || !ds) return;
  const s = ds.training_ok === 0 ? "训练失败" : (ds.training_ok === 1 ? "已训练" : "未训练");
  const last = ds.last_trained_at ? `，上次训练：${ds.last_trained_at}` : "";
  detailEl.textContent = `${ds.name || ds.id} (${ds.type}) · ${s}${last}`;
}

async function createDatasource() {
  const statusEl = el("dsFormStatus");
  if (statusEl) statusEl.textContent = "提交中...";
  const payload = {
    name: el("dsName").value.trim(),
    type: el("dsType").value,
    host: el("dsHost").value.trim(),
    port: Number(el("dsPort").value || 3306),
    database: el("dsDb").value.trim(),
    user: el("dsUser").value.trim(),
    password: el("dsPass").value,
    is_default: el("dsDefault").checked
  };
  try {
    const resp = await apiFetch("/datasources", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify(payload)
    });
    const data = await resp.json();
    if (statusEl) statusEl.textContent = data.training_ok ? "创建成功并已训练" : `创建成功但训练失败：${data.training_error || ""}`;
    await refreshDatasources();
    renderDsList();
    await refreshSchemaTables();
  } catch (e) {
    if (statusEl) statusEl.textContent = "创建失败";
  }
}

async function testDatasource() {
  if (!activeDsInModal) return;
  const detailEl = el("dsDetail");
  if (detailEl) detailEl.textContent = "连接测试中...";
  try {
    await apiFetch(`/datasources/${encodeURIComponent(activeDsInModal)}/test`, {method: "POST"});
    if (detailEl) detailEl.textContent = "连接成功";
  } catch (e) {
    if (detailEl) detailEl.textContent = "连接失败";
  }
}

async function trainDatasourceSelected() {
  if (!activeDsInModal) return;
  const detailEl = el("dsDetail");
  if (detailEl) detailEl.textContent = "训练中...";
  try {
    const resp = await apiFetch(`/datasources/${encodeURIComponent(activeDsInModal)}/train`, {method: "POST"});
    const data = await resp.json();
    if (detailEl) detailEl.textContent = data.ok ? "训练完成" : `训练失败：${data.error || ""}`;
    await refreshDatasources();
    renderDsList();
    await refreshSchemaTables();
  } catch (e) {
    if (detailEl) detailEl.textContent = "训练失败";
  }
}

async function setDefaultDatasource() {
  if (!activeDsInModal) return;
  try {
    await apiFetch(`/datasources/${encodeURIComponent(activeDsInModal)}/default`, {method: "PUT"});
    await refreshDatasources();
    renderDsList();
  } catch (e) {
    // ignore
  }
}


function renderTableInto(wrapEl, columns, rows) {
  if (!wrapEl) return;
  if (!columns || columns.length === 0) {
    wrapEl.innerHTML = "<div class='pad muted'>无数据</div>";
    return;
  }
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  columns.forEach(c => {
    const th = document.createElement("th");
    th.textContent = c;
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach(r => {
    const tr = document.createElement("tr");
    r.forEach(v => {
      const td = document.createElement("td");
      td.textContent = (v === null || v === undefined) ? "" : String(v);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  wrapEl.innerHTML = "";
  wrapEl.appendChild(table);
}

async function refreshSchemaTables() {
  const listEl = el("tableList");
  const previewEl = el("previewWrap");
  if (!listEl || !previewEl) return;

  if (!token) {
    listEl.innerHTML = "<div class='pad muted'>登录后可查看表列表</div>";
    previewEl.innerHTML = "<div class='pad muted'>请选择一个表进行预览</div>";
    return;
  }

  listEl.innerHTML = "<div class='pad muted'>加载中...</div>";
  try {
    const resp = await apiFetch("/schema/tables");
    const tables = await resp.json();
    if (!tables || tables.length === 0) {
      listEl.innerHTML = "<div class='pad muted'>未发现表</div>";
      previewEl.innerHTML = "<div class='pad muted'>无可预览数据</div>";
      return;
    }
    listEl.innerHTML = "";
    tables.forEach(t => {
      const item = document.createElement("div");
      item.className = "table-item";

      const meta = document.createElement("div");
      meta.className = "table-meta";
      const name = document.createElement("div");
      name.className = "table-name";
      name.textContent = t.name;
      const comment = document.createElement("div");
      comment.className = "table-comment";
      comment.textContent = (t.comment || t.type || "").trim();
      meta.appendChild(name);
      meta.appendChild(comment);

      const btn = document.createElement("button");
      btn.className = "btn-mini";
      btn.type = "button";
      btn.textContent = "预览";
      btn.onclick = () => previewTable(t.name);

      item.appendChild(meta);
      item.appendChild(btn);
      listEl.appendChild(item);
    });
    previewEl.innerHTML = "<div class='pad muted'>请选择一个表进行预览</div>";
  } catch (e) {
    listEl.innerHTML = "<div class='pad error'>加载表列表失败</div>";
    previewEl.innerHTML = "<div class='pad muted'>请检查后端日志</div>";
  }
}

async function previewTable(tableName) {
  const previewEl = el("previewWrap");
  if (!previewEl) return;
  previewEl.innerHTML = "<div class='pad muted'>加载预览...</div>";
  try {
    const resp = await apiFetch(`/schema/tables/${encodeURIComponent(tableName)}/preview?limit=10`);
    const data = await resp.json();
    renderTableInto(previewEl, data.columns, data.rows);
  } catch (e) {
    let msg = e?.message || String(e);
    try {
      const parsed = JSON.parse(msg);
      if (parsed && parsed.detail) msg = parsed.detail;
    } catch (_) {}
    previewEl.innerHTML = `<div class='pad error'>预览失败：${msg}</div>`;
  }
}

async function refreshUploads() {
  const listEl = el("uploadList");
  const titleEl = el("uploadSectionTitle");
  if (!listEl) return;
  if (!token) {
    listEl.innerHTML = "";
    if (titleEl) titleEl.style.display = "none";
    listEl.style.display = "none";
    return;
  }
  listEl.innerHTML = "<div class='pad muted'>加载中...</div>";
  listEl.style.display = "";
  if (titleEl) titleEl.style.display = "";
  try {
    const resp = await apiFetch("/files");
    const files = await resp.json();
    if (!files || files.length === 0) {
      listEl.innerHTML = "";
      listEl.style.display = "none";
      if (titleEl) titleEl.style.display = "none";
      return;
    }
    listEl.innerHTML = "";
    files.forEach(f => {
      const item = document.createElement("div");
      item.className = "table-item upload-item";

      const meta = document.createElement("div");
      meta.className = "table-meta";
      const name = document.createElement("div");
      name.className = "table-name";
      name.textContent = f.filename || f.table_name;
      const comment = document.createElement("div");
      comment.className = "table-comment";
      const sheet = f.sheet_name ? ` / ${f.sheet_name}` : "";
      comment.textContent = `${f.table_name}${sheet} | ${f.row_count || 0} rows`;
      meta.appendChild(name);
      meta.appendChild(comment);

      const actions = document.createElement("div");
      actions.className = "upload-actions";
      const previewBtn = document.createElement("button");
      previewBtn.className = "btn-mini";
      previewBtn.type = "button";
      previewBtn.textContent = "预览";
      previewBtn.onclick = () => previewTable(f.table_name);
      const delBtn = document.createElement("button");
      delBtn.className = "btn-mini danger";
      delBtn.type = "button";
      delBtn.textContent = "删除";
      delBtn.onclick = () => deleteUpload(f.id);
      actions.appendChild(previewBtn);
      actions.appendChild(delBtn);

      item.appendChild(meta);
      item.appendChild(actions);
      listEl.appendChild(item);
    });
  } catch (e) {
    listEl.innerHTML = "";
    listEl.style.display = "none";
    if (titleEl) titleEl.style.display = "none";
  }
}

async function deleteUpload(fileId) {
  if (!confirm("确定删除该上传文件吗？")) return;
  try {
    await apiFetch(`/files/${encodeURIComponent(fileId)}`, { method: "DELETE" });
    await refreshUploads();
    await refreshSchemaTables();
  } catch (e) {
    addChatMessage("assistant", "删除失败");
  }
}

async function loadSheetNames(file) {
  const select = el("sheetSelect");
  const status = el("uploadStatus");
  if (!select) return;
  select.innerHTML = "";
  select.disabled = true;
  if (!file) return;
  if (!token) {
    if (status) status.textContent = "请先登录";
    return;
  }
  const name = (file.name || "").toLowerCase();
  if (name.endsWith(".csv")) {
    const opt = document.createElement("option");
    opt.value = "(csv)";
    opt.textContent = "(csv)";
    opt.selected = true;
    select.appendChild(opt);
    select.disabled = true;
    return;
  }

  const form = new FormData();
  form.append("file", file);
  try {
    const resp = await fetch(`${apiBase}/files/sheets`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "X-Datasource-Id": currentDatasourceId || ""
      },
      body: form
    });
    if (!resp.ok) {
      const detail = await resp.text();
      throw new Error(detail || "sheet list error");
    }
    const data = await resp.json();
    const sheets = data.sheets || [];
    if (sheets.length === 0) return;
    sheets.forEach((s, idx) => {
      const opt = document.createElement("option");
      opt.value = s;
      opt.textContent = s;
      if (idx === 0) opt.selected = true;
      select.appendChild(opt);
    });
    if (sheets.length === 1 && sheets[0] === "(csv)") {
      select.disabled = true;
    } else {
      select.disabled = false;
    }
  } catch (e) {
    if (status) status.textContent = `获取 Sheet 失败：${e.message || e}`;
  }
}

async function uploadFile() {
  const input = el("fileInput");
  const status = el("uploadStatus");
  if (!input || !status) return;
  if (!token) {
    status.textContent = "请先登录";
    return;
  }
  const file = input.files && input.files[0];
  if (!file) {
    status.textContent = "请选择文件";
    return;
  }
  status.textContent = "上传中...";
  const form = new FormData();
  form.append("file", file);
  const sheetSelect = el("sheetSelect");
  const sheetName = sheetSelect && !sheetSelect.disabled ? sheetSelect.value : "";
  if (sheetName && sheetName !== "(csv)") {
    form.append("sheet_name", sheetName);
  }
  try {
    const resp = await fetch(`${apiBase}/files/upload`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${token}`,
        "X-Datasource-Id": currentDatasourceId || ""
      },
      body: form
    });
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    status.textContent = `已上传：${data.table_name}，可在对话中直接查询该表（如：统计按地区的销售额）`;
    input.value = "";
    if (sheetSelect) {
      sheetSelect.innerHTML = "";
      sheetSelect.disabled = true;
    }
    await refreshUploads();
    await refreshSchemaTables();
    if (data.table_name) {
      await previewTable(data.table_name);
    }
  } catch (e) {
    status.textContent = "上传失败";
  }
}

async function refreshConversations() {
  const resp = await apiFetch("/conversations");
  const list = await resp.json();
  const wrap = el("convList");
  wrap.innerHTML = "";
  list.forEach(c => {
    const chip = document.createElement("div");
    chip.className = "chip" + (c.id === activeConversationId ? " active": "");
    const title = document.createElement("span");
    title.className = "chip-title";
    title.textContent = truncateTitle(c.title || "新会话", 14);
    const del = document.createElement("button");
    del.className = "chip-del";
    del.type = "button";
    del.title = "删除会话";
    del.textContent = "×";
    del.onclick = async (e) => {
      e.stopPropagation();
      if (!confirm("确定删除该会话吗？此操作不可恢复。")) return;
      try {
        await apiFetch(`/conversations/${c.id}`, { method: "DELETE" });
        if (c.id === activeConversationId) {
          activeConversationId = "";
          localStorage.removeItem("convId");
        }
        await refreshConversations();
        if (!activeConversationId) {
          const next = list.filter(x => x.id !== c.id)[0];
          if (next) await loadConversation(next.id);
          else await newConversation();
        }
      } catch (err) {
        addChatMessage("assistant", "❌ 删除失败: " + (err?.message || String(err)));
      }
    };
    chip.onclick = () => loadConversation(c.id);
    chip.appendChild(title);
    chip.appendChild(del);
    wrap.appendChild(chip);
  });
}

function truncateTitle(text, maxLen) {
  const t = (text || "").trim();
  if (!t || t === "New Conversation") return "新会话";
  if (t.length <= maxLen) return t;
  return t.slice(0, maxLen) + "…";
}

async function newConversation() {
  const resp = await apiFetch("/conversations", {method:"POST"});
  const data = await resp.json();
  await loadConversation(data.conversation_id);
}

function addChatMessage(role, content, options = {}) {
  const box = el("chatHistory");
  const m = document.createElement("div");
  m.className = "msg " + (role === "user" ? "user" : "assistant");
  const r = document.createElement("div");
  r.className = "role";
  r.textContent = role;
  const b = document.createElement("div");
  b.textContent = content;
  m.appendChild(r);
  m.appendChild(b);
  if (role === "user") {
    lastUserMsgEl = m;
    if (options.messageId) m.dataset.messageId = String(options.messageId);
  }
  if (options.artifact && role === "user") {
    if (options.messageId) messageArtifacts.set(options.messageId, options.artifact);
    m.classList.add("clickable");
    m.title = "点击回放 SQL/结果/图表";
    m.addEventListener("click", () => {
      const mid = Number(m.dataset.messageId || options.artifact.message_id || 0);
      if (mid && messageArtifacts.has(mid)) {
        const latest = messageArtifacts.get(mid);
        const question = messageIdToQuestion.get(mid) || options.artifact.question || "";
        showArtifact({ ...latest, question, message_id: mid });
        return;
      }
      showArtifact(options.artifact);
    });
  }
  box.appendChild(m);
  box.scrollTop = box.scrollHeight;
  return { container: m, body: b };
}

async function loadConversation(convId) {
  activeConversationId = convId;
  localStorage.setItem("convId", convId);

  await refreshConversations();

  // clear UI panels
  el("chatHistory").innerHTML = "";
  el("sqlBox").textContent = "";
  el("tableWrap").innerHTML = "";
  el("statusLine").textContent = "";
  el("chartHint").textContent = "";
  analysisStreaming = "";
  analysisMsgBodyEl = null;
  lastAnalysisText = "";
  lastUserQuestion = "";
  messageArtifacts.clear();
  messageIdToQuestion.clear();
  activeMessageId = 0;
  lastUserMsgEl = null;
  lastChartOption = null;
  if (!chart) chart = echarts.init(el("chart"));
  chart.clear();

  const resp = await apiFetch(`/conversations/${convId}/messages`);
  const msgs = await resp.json();
  let latestUser = null;
  msgs.forEach(m => {
    if (m.role === "user") {
      messageIdToQuestion.set(m.id, m.content || "");
    }
    if (m.role === "user" && m.artifact) {
      messageArtifacts.set(m.id, m.artifact);
      addChatMessage(m.role, m.content, { messageId: m.id, artifact: { ...m.artifact, question: m.content, message_id: m.id } });
      latestUser = m;
    } else {
      addChatMessage(m.role, m.content, m.role === "user" ? { messageId: m.id } : {});
      if (m.role === "user") latestUser = m;
    }
  });
  if (latestUser) {
    activeMessageId = latestUser.id || 0;
    lastUserQuestion = latestUser.content || "";
    if (latestUser.artifact && latestUser.artifact.analysis) {
      lastAnalysisText = latestUser.artifact.analysis;
      reportContext.question = latestUser.content || "";
      reportContext.analysis = latestUser.artifact.analysis;
    }
  }
}

function renderTable(columns, rows) {
  lastTableColumns = columns || [];
  lastTableRows = rows || [];
  updateExportHint();
  renderTableInto(el("tableWrap"), columns, rows);
  renderTableModal();
}

function renderTableModal() {
  const wrap = el("tableModalWrap");
  const info = el("tablePageInfo");
  if (!wrap || !info) return;
  const query = (tableModalState.query || "").toLowerCase();
  const pageSize = tableModalState.pageSize || 50;
  const rows = (lastTableRows || []).filter(r => {
    if (!query) return true;
    return r.some(v => String(v ?? "").toLowerCase().includes(query));
  });
  const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  if (tableModalState.page > totalPages) tableModalState.page = totalPages;
  if (tableModalState.page < 1) tableModalState.page = 1;
  const start = (tableModalState.page - 1) * pageSize;
  const pageRows = rows.slice(start, start + pageSize);
  renderTableInto(wrap, lastTableColumns || [], pageRows);
  info.textContent = `${tableModalState.page} / ${totalPages}`;
  const prev = el("btnTablePrev");
  const next = el("btnTableNext");
  if (prev) prev.disabled = tableModalState.page <= 1;
  if (next) next.disabled = tableModalState.page >= totalPages;
}

async function sendMessage() {
  const text = el("chatInput").value.trim();
  if (!text) return;
  lastUserQuestion = text;
  reportContext.question = text;
  if (!activeConversationId) await newConversation();
  el("chatInput").value = "";
  addChatMessage("user", text);
  await refreshConversations();

  // Start SSE via fetch streaming
  const resp = await fetch(`${apiBase}/chat/sse`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`,
      "X-Datasource-Id": currentDatasourceId || ""
    },
    body: JSON.stringify({conversation_id: activeConversationId, message: text})
  });

  if (!resp.ok) {
    const err = await resp.text();
    addChatMessage("assistant", "❌ " + err);
    return;
  }

  const reader = resp.body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  const onEvent = (eventName, data) => {
    if (eventName === "message") {
      activeMessageId = data.user_message_id || 0;
      if (activeMessageId) {
        messageIdToQuestion.set(activeMessageId, lastUserQuestion || "");
        if (lastUserMsgEl) lastUserMsgEl.dataset.messageId = String(activeMessageId);
      }
    } else if (eventName === "status") {
      el("statusLine").textContent = "阶段: " + data.stage;
    } else if (eventName === "sql") {
      el("sqlBox").textContent = data.sql || "";
    } else if (eventName === "table") {
      renderTable(data.columns, data.rows);
    } else if (eventName === "chart") {
      if (!chart) chart = echarts.init(el("chart"));
      chart.clear();
      if (data.echarts_option) {
        lastChartOption = data.echarts_option;
        chart.setOption(data.echarts_option);
        el("chartHint").textContent = "";
      } else {
        lastChartOption = null;
        el("chartHint").textContent = "无法从该结果自动推断合适的图表（你可以调整 SQL 让结果更适合可视化，例如：维度列 + 数值列）。";
      }
    } else if (eventName === "analysis") {
      if (data.delta) {
        analysisStreaming += data.delta;
        if (!analysisMsgBodyEl) {
          analysisMsgBodyEl = addChatMessage("assistant", "").body;
        }
        analysisMsgBodyEl.textContent = analysisStreaming;
      } else {
        const text = data.text || "";
        if (analysisMsgBodyEl) {
          analysisMsgBodyEl.textContent = text;
          analysisMsgBodyEl = null;
          analysisStreaming = "";
          lastAnalysisText = text;
          reportContext.analysis = text;
          if (activeMessageId) {
            messageArtifacts.set(activeMessageId, {
              sql: el("sqlBox").textContent || "",
              columns: lastTableColumns || [],
              rows: lastTableRows || [],
              chart: lastChartOption,
              analysis: text,
              question: lastUserQuestion || "",
              message_id: activeMessageId
            });
          }
        } else if (text) {
          addChatMessage("assistant", text);
          lastAnalysisText = text;
          reportContext.analysis = text;
          if (activeMessageId) {
            messageArtifacts.set(activeMessageId, {
              sql: el("sqlBox").textContent || "",
              columns: lastTableColumns || [],
              rows: lastTableRows || [],
              chart: lastChartOption,
              analysis: text,
              question: lastUserQuestion || "",
              message_id: activeMessageId
            });
          }
        }
      }
    } else if (eventName === "error") {
      el("statusLine").textContent = "错误";
      addChatMessage("assistant", "❌ " + (data.message || "unknown error"));
      analysisStreaming = "";
      analysisMsgBodyEl = null;
    } else if (eventName === "done") {
      refreshConversations();
    }
  };

  while (true) {
    const {value, done} = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, {stream:true});

    // Parse SSE frames separated by \n\n
    let idx;
    while ((idx = buffer.indexOf("\n\n")) !== -1) {
      const frame = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);

      let eventName = "message";
      let dataLines = [];
      frame.split("\n").forEach(line => {
        if (line.startsWith("event:")) eventName = line.slice(6).trim();
        if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
      });
      const raw = dataLines.join("\n");
      if (!raw) continue;
      let data;
      try { data = JSON.parse(raw); } catch(e) { data = {text: raw}; }
      onEvent(eventName, data);
    }
  }
}

el("btnLogin").onclick = login;
el("btnNewConv").onclick = newConversation;
el("btnSend").onclick = sendMessage;
if (el("datasourceSelect")) el("datasourceSelect").onchange = async (e) => {
  currentDatasourceId = e.target.value || "";
  localStorage.setItem("datasourceId", currentDatasourceId);
  await refreshSchemaTables();
  await refreshUploads();
};
if (el("btnManageDs")) el("btnManageDs").onclick = openDsModal;
if (el("btnCloseDsModal")) el("btnCloseDsModal").onclick = closeDsModal;
if (el("dsModal")) el("dsModal").addEventListener("click", (e) => {
  if (e.target.id === "dsModal") closeDsModal();
});
if (el("btnDsCreate")) el("btnDsCreate").onclick = createDatasource;
if (el("btnDsTest")) el("btnDsTest").onclick = testDatasource;
if (el("btnDsTrain")) el("btnDsTrain").onclick = trainDatasourceSelected;
if (el("btnDsDefault")) el("btnDsDefault").onclick = setDefaultDatasource;
if (el("btnExportOptions")) el("btnExportOptions").onclick = openExportModal;
if (el("btnCloseExport")) el("btnCloseExport").onclick = closeExportModal;
if (el("exportModal")) el("exportModal").addEventListener("click", (e) => {
  if (e.target.id === "exportModal") closeExportModal();
});
if (el("btnEditSql")) el("btnEditSql").onclick = openSqlModal;
if (el("btnCloseSqlModal")) el("btnCloseSqlModal").onclick = closeSqlModal;
if (el("sqlModal")) el("sqlModal").addEventListener("click", (e) => {
  if (e.target.id === "sqlModal") closeSqlModal();
});
if (el("btnRunSql")) el("btnRunSql").onclick = runSql;
if (el("sqlEditor")) el("sqlEditor").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") runSql();
});
if (el("exportRange")) el("exportRange").onchange = updateExportHint;
if (el("btnExportXlsxModal")) el("btnExportXlsxModal").onclick = () => downloadXlsx(lastTableColumns, getExportRows(el("exportRange").value), getExportFilename(".xlsx"));
if (el("btnTableFullscreen")) el("btnTableFullscreen").onclick = () => {
  const modal = el("tableModal");
  tableModalState.page = 1;
  if (modal) modal.classList.add("open");
  renderTableModal();
};
if (el("btnCloseTable")) el("btnCloseTable").onclick = () => {
  const modal = el("tableModal");
  if (modal) modal.classList.remove("open");
};
if (el("tableModal")) el("tableModal").addEventListener("click", (e) => {
  if (e.target.id === "tableModal") e.currentTarget.classList.remove("open");
});
if (el("tableSearch")) el("tableSearch").addEventListener("input", (e) => {
  tableModalState.query = e.target.value || "";
  tableModalState.page = 1;
  renderTableModal();
});
if (el("tablePageSize")) el("tablePageSize").onchange = (e) => {
  tableModalState.pageSize = Number(e.target.value || 50);
  tableModalState.page = 1;
  renderTableModal();
};
if (el("btnTablePrev")) el("btnTablePrev").onclick = () => {
  tableModalState.page -= 1;
  renderTableModal();
};
if (el("btnTableNext")) el("btnTableNext").onclick = () => {
  tableModalState.page += 1;
  renderTableModal();
};
if (el("btnExportChart")) el("btnExportChart").onclick = downloadChart;
if (el("btnExportReport")) el("btnExportReport").onclick = exportHtmlReport;
if (el("btnUpload")) el("btnUpload").onclick = uploadFile;
if (el("btnDrawer")) el("btnDrawer").onclick = () => toggleDrawer(true);
if (el("btnCloseDrawer")) el("btnCloseDrawer").onclick = () => toggleDrawer(false);
if (el("drawerOverlay")) el("drawerOverlay").onclick = () => toggleDrawer(false);
if (el("fileInput")) el("fileInput").addEventListener("change", (e) => {
  const file = e.target.files && e.target.files[0];
  loadSheetNames(file);
});
if (el("btnRefreshSchema")) el("btnRefreshSchema").onclick = async () => {
  await refreshSchemaTables();
  await refreshUploads();
};

el("chatInput").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") sendMessage();
});

// Auto login if token exists
(async () => {
  await refreshSchemaTables();
  await refreshUploads();
  if (token) {
    setAuthStatus("已读取本地 token");
    try {
      await refreshDatasources();
      await refreshSchemaTables();
      await refreshUploads();
      await refreshConversations();
      if (!activeConversationId) await newConversation();
      else await loadConversation(activeConversationId);
    } catch (e) {
      // token invalid
      token = "";
      localStorage.removeItem("token");
    }
  }
})();

function toggleDrawer(open) {
  const drawer = el("drawer");
  const overlay = el("drawerOverlay");
  if (!drawer || !overlay) return;
  if (open) {
    drawer.classList.add("open");
    overlay.classList.add("open");
    document.body.classList.add("drawer-open");
  } else {
    drawer.classList.remove("open");
    overlay.classList.remove("open");
    document.body.classList.remove("drawer-open");
  }
}

if (el("btnToggleSql")) el("btnToggleSql").onclick = () => {
  const box = document.querySelector(".sql-box");
  if (!box) return;
  box.classList.toggle("collapsed");
  const btn = el("btnToggleSql");
  if (btn) btn.textContent = box.classList.contains("collapsed") ? "展开" : "收起";
};

function showArtifact(artifact) {
  if (!artifact) return;
  el("statusLine").textContent = "回放";
  el("sqlBox").textContent = artifact.sql || "";
  renderTable(artifact.columns || [], artifact.rows || []);
  if (!chart) chart = echarts.init(el("chart"));
  chart.clear();
  if (artifact.message_id) activeMessageId = artifact.message_id;
  reportContext.question = artifact.question || lastUserQuestion || "";
  reportContext.analysis = artifact.analysis || "";
  if (artifact.message_id && artifact.question) {
    messageIdToQuestion.set(artifact.message_id, artifact.question);
  }
  if (artifact.question) lastUserQuestion = artifact.question;
  if (artifact.analysis) lastAnalysisText = artifact.analysis;
  if (artifact.chart) {
    lastChartOption = artifact.chart;
    chart.setOption(artifact.chart);
    el("chartHint").textContent = "";
  } else {
    lastChartOption = null;
    el("chartHint").textContent = "该问题未生成图表";
  }
}

async function downloadXlsx(columns, rows, filename = "result.xlsx") {
  if (rows.length > 5000 && !confirm("结果行数较多，导出可能较慢，是否继续？")) return;
  const resp = await apiFetch("/export/xlsx", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ columns, rows, filename })
  });
  const blob = await resp.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function downloadChart() {
  if (!chart) return;
  const url = chart.getDataURL({ type: "png", pixelRatio: 2, backgroundColor: "#ffffff" });
  const a = document.createElement("a");
  a.href = url;
  a.download = "chart.png";
  a.click();
}

function exportHtmlReport() {
  const sql = el("sqlBox").textContent || "";
  const question = (reportContext.question || "").trim() ||
    (el("chatInput")?.value || "").trim() ||
    lastUserQuestion ||
    "（当前问题已发送）";
  const chartUrl = chart ? chart.getDataURL({ type: "png", pixelRatio: 2, backgroundColor: "#ffffff" }) : "";
  const filename = `report_${new Date().toISOString().slice(0,10)}.html`;
  const rows = lastTableRows || [];
  const cols = lastTableColumns || [];

  const escape = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  const tableHead = cols.map(c => `<th>${escape(c)}</th>`).join("");
  const tableBody = rows.map(r => `<tr>${r.map(v => `<td>${escape(v)}</td>`).join("")}</tr>`).join("");

  const html = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<title>分析报告</title>
<style>
body{font-family:Arial,"Noto Sans SC",sans-serif;padding:24px;color:#111;}
.h1{font-size:20px;font-weight:700;margin-bottom:12px;}
.section{margin:18px 0;}
.code{white-space:pre-wrap;background:#0b1220;color:#fff;padding:12px;border-radius:8px;}
img{max-width:100%;border:1px solid #eee;border-radius:8px;}
table{border-collapse:collapse;width:100%;font-size:12px;}
th,td{border:1px solid #eee;padding:6px;}
th{background:#f6f7fb;text-align:left;}
</style>
</head>
<body>
<div class="h1">分析报告</div>
<div class="section">生成时间：${new Date().toLocaleString()}</div>
<div class="section"><b>用户提问</b><div>${escape(question)}</div></div>
<div class="section"><b>SQL</b><div class="code">${escape(sql)}</div></div>
${chartUrl ? `<div class="section"><b>图表</b><br/><img src="${chartUrl}"/></div>` : ''}
<div class="section"><b>结果表</b><table><thead><tr>${tableHead}</tr></thead><tbody>${tableBody}</tbody></table></div>
${(reportContext.analysis || lastAnalysisText) ? `<div class="section"><b>分析结论</b><div>${escape(reportContext.analysis || lastAnalysisText)}</div></div>` : ''}
</body>
</html>`;
const blob = new Blob([html], { type: "text/html;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

const apiBase = "/api";
let token = localStorage.getItem("token") || "";
let activeConversationId = localStorage.getItem("convId") || "";
let chart = null;
let analysisStreaming = "";
let analysisMsgBodyEl = null;

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
  options.headers = headers;
  const resp = await fetch(`${apiBase}${path}`, options);
  if (!resp.ok) throw new Error(await resp.text());
  return resp;
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
  if (!listEl) return;
  if (!token) {
    listEl.innerHTML = "<div class='pad muted'>登录后可查看上传文件</div>";
    return;
  }
  listEl.innerHTML = "<div class='pad muted'>加载中...</div>";
  try {
    const resp = await apiFetch("/files");
    const files = await resp.json();
    if (!files || files.length === 0) {
      listEl.innerHTML = "<div class='pad muted'>无上传文件</div>";
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
    listEl.innerHTML = "<div class='pad error'>加载失败</div>";
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
      headers: { "Authorization": `Bearer ${token}` },
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
      headers: { "Authorization": `Bearer ${token}` },
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
    title.textContent = (c.title || "Conversation").slice(0, 28);
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

async function newConversation() {
  const resp = await apiFetch("/conversations", {method:"POST"});
  const data = await resp.json();
  await loadConversation(data.conversation_id);
}

function addChatMessage(role, content) {
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
  if (!chart) chart = echarts.init(el("chart"));
  chart.clear();

  const resp = await apiFetch(`/conversations/${convId}/messages`);
  const msgs = await resp.json();
  msgs.forEach(m => addChatMessage(m.role, m.content));
}

function renderTable(columns, rows) {
  renderTableInto(el("tableWrap"), columns, rows);
}

async function sendMessage() {
  const text = el("chatInput").value.trim();
  if (!text) return;
  if (!activeConversationId) await newConversation();
  el("chatInput").value = "";
  addChatMessage("user", text);

  // Start SSE via fetch streaming
  const resp = await fetch(`${apiBase}/chat/sse`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${token}`
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
    if (eventName === "status") {
      el("statusLine").textContent = "阶段: " + data.stage;
    } else if (eventName === "sql") {
      el("sqlBox").textContent = data.sql || "";
    } else if (eventName === "table") {
      renderTable(data.columns, data.rows);
    } else if (eventName === "chart") {
      if (!chart) chart = echarts.init(el("chart"));
      chart.clear();
      if (data.echarts_option) {
        chart.setOption(data.echarts_option);
        el("chartHint").textContent = "";
      } else {
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
        } else if (text) {
          addChatMessage("assistant", text);
        }
      }
    } else if (eventName === "error") {
      el("statusLine").textContent = "错误";
      addChatMessage("assistant", "❌ " + (data.message || "unknown error"));
      analysisStreaming = "";
      analysisMsgBodyEl = null;
    } else if (eventName === "done") {
      // ignore
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
  } else {
    drawer.classList.remove("open");
    overlay.classList.remove("open");
  }
}

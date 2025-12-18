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
    previewEl.innerHTML = "<div class='pad error'>预览失败</div>";
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
if (el("btnRefreshSchema")) el("btnRefreshSchema").onclick = refreshSchemaTables;

el("chatInput").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") sendMessage();
});

// Auto login if token exists
(async () => {
  await refreshSchemaTables();
  if (token) {
    setAuthStatus("已读取本地 token");
    try {
      await refreshSchemaTables();
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

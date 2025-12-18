# Vanna2 Analytics Tool (DeepSeek-chat + Qwen3-Embedding-8B)

这是一个“受 Vanna 2.0 思想启发”的数据分析工具：**自然语言 → SQL → 查询结果 → 图表 → 分析总结**。  
支持 **多用户（JWT）**、**多轮对话（conversation_id）**，并在服务启动时对 MySQL 的 schema 做一次“训练/索引”（向量化检索）。

> 说明：本项目把数据库与大模型配置都留成占位符空字符串，你只需要改 `.env` 即可。

## 功能概览

- 后端：FastAPI
- 对话：多轮（SQLite 存储对话历史）
- 目标数据源：MySQL（你要分析的数据）
- LLM：DeepSeek-chat（OpenAI 兼容的 `/v1/chat/completions`）
- Embedding：Qwen3-Embedding-8B（OpenAI 兼容的 `/v1/embeddings`）
- 启动时训练：读取 `information_schema` → 写入 ChromaDB 向量库（用于 schema 检索）
- 前端：纯 HTML/CSS/JS（4 块布局）
  1) 用户输入框 + 对话列表
  2) 生成 SQL + 查询结果表格
  3) 查询结果图形化展示（ECharts）
  4) 大模型对结果的分析

## 目录结构

```
.
├── backend/
│   └── app/
│       ├── main.py
│       ├── api/
│       ├── core/
│       ├── models/
│       ├── schemas/
│       └── services/
└── frontend/
    ├── index.html
    ├── app.js
    └── styles.css
```

## 快速启动（uv）

0)（推荐）使用 Python 3.12（部分依赖在 3.14 上可能没有现成 wheel）：

```bash
uv python install 3.12
uv python pin 3.12
```

1) 安装并同步依赖（会生成 `uv.lock` 并创建/更新 `.venv`）：

```bash
uv sync --group dev
```

2) 复制配置文件并填写（先留空也能启动 UI，但无法实际分析）：

```bash
cp .env.example .env
```

PowerShell（Windows）：

```powershell
Copy-Item .env.example .env
```

3) 启动服务：

```bash
uv run uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```

4) 打开浏览器：

- http://localhost:8000

## 开发常用（uv）

```bash
uv run ruff check .
uv run pytest
```

## 配置说明（必须）

- MySQL（要分析的数据源）：`MYSQL_HOST/MYSQL_DATABASE/MYSQL_USER/MYSQL_PASSWORD`
- DeepSeek-chat：`DEEPSEEK_BASE_URL/DEEPSEEK_API_KEY/DEEPSEEK_MODEL`
- Qwen3 Embedding：`EMBED_BASE_URL/EMBED_API_KEY/EMBED_MODEL`

## 安全说明（默认策略）

- 仅允许 `SELECT` / `WITH` 查询；其他语句直接拒绝
- 默认限制最多返回 `MAX_ROWS` 行（避免前端卡死）
- 可以在 `backend/app/core/config.py` 里调整

## 参考（Vanna 2.0）

- Vanna 2.0 提供 FastAPI 部署与 streaming chat 的整体思路 citeturn1view0
- ToolResult/UiComponent 的“rich UI + simple fallback”理念 citeturn10search1turn10search3
- Vanna 2.0 的 Agent/ToolRegistry 示例（我们这里实现了更可控的自定义 pipeline） citeturn7view0
- 对 OpenAI 兼容 API 的 usage 字段兼容坑（DeepSeek/Qwen 常见） citeturn6view0

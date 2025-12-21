# Vanna2 Analytics Tool

基于 FastAPI + MySQL + LLM 的轻量数据分析工具：自然语言提问 → SQL → 结果 → 图表 → 分析结论。

## 功能概览

- 多数据源管理（MySQL），支持切换/测试/训练/重训与训练状态
- SQL 生成与执行（仅允许 SELECT/WITH），支持 SQL 编辑重跑
- 结果表格、图表可视化与分析结论（流式输出）
- SQL 辅助：解释、建议、安全提示、纠错建议
- 图表手动配置：类型切换、字段拖拽、筛选器、指标/维度管理
- 数据预览（弹框）：库内表列表、结构变更日志、分页预览
- 文件上传（CSV/Excel，多 Sheet），自动建临时表并可预览
- 导出：表格（XLSX）、图表图片、分析报告（HTML）
- 治理与审计：SQL 审计日志、慢查询提示、敏感字段脱敏
- 结构变更检测：每 72 小时检查一次，局部重训并记录变更

## 目录结构

```
.
├── backend/
│   └── app/
│       ├── main.py
│       ├── api/
│       ├── core/
│       ├── schemas/
│       └── services/
└── frontend/
    ├── index.html
    ├── app.js
    └── styles.css
```

## 快速启动（uv）

推荐 Python 3.12：

```bash
uv python install 3.12
uv python pin 3.12
```

安装依赖：

```bash
uv sync --group dev
```

配置环境变量：

```bash
cp .env.example .env
```

PowerShell：

```powershell
Copy-Item .env.example .env
```

启动服务：

```bash
uv run uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```

打开浏览器：

- http://localhost:8000

## 核心配置

必填（用于实际分析）：

- MySQL：`MYSQL_HOST/MYSQL_DATABASE/MYSQL_USER/MYSQL_PASSWORD`
- LLM：`DEEPSEEK_BASE_URL/DEEPSEEK_API_KEY/DEEPSEEK_MODEL`
- Embedding：`EMBED_BASE_URL/EMBED_API_KEY/EMBED_MODEL`

常用可调：

- `MAX_ROWS` 查询最大行数
- `SLOW_QUERY_THRESHOLD_MS` 慢查询阈值
- `SCHEMA_CHECK_INTERVAL_HOURS` 表结构检查周期
- `SENSITIVE_FIELD_KEYWORDS` 敏感字段关键词

更多参数见 `.env.example` 与 `backend/app/core/config.py`。

## 运行说明

- 默认只允许只读 SQL（SELECT/WITH），写操作会被拦截
- 会话与审计信息存储在 SQLite：`data/app.sqlite3`
- 向量库默认存储在：`data/chroma`

## 开发

```bash
uv run ruff check .
uv run pytest
```

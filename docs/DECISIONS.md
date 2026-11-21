# Architecture & Implementation Decisions

This document records key technical decisions, rationale, and consequences for PipelineSentinel.

---

### DECISION-001: Orchestration Engine & Pinning
- **Decision:** Use Apache Airflow `3.4.0` in Docker using `LocalExecutor` adapted from official compose file.
- **Rationale:** Section 2 and Section 3 of the project brief require latest stable Airflow 3.x with LocalExecutor, removing Celery/Redis/Flower overhead. Airflow 3.4.0 provides `airflow-apiserver`, `airflow-scheduler`, and `airflow-dag-processor` with modern task isolation.
- **Date:** 2026-11-21

---

### DECISION-002: PySpark and Delta Lake Compatibility
- **Decision:** Pin `pyspark==3.5.4` and `delta-spark==3.3.0` running on OpenJDK 21 in the custom Airflow Dockerfile.
- **Rationale:** Certified PySpark 3.5.x and Delta Lake 3.3.0 pair. OpenJDK 21 is natively supported by Debian 13 (trixie) and Apache Spark 3.5.x.
- **Date:** 2026-11-21

---

### DECISION-003: Logging Standard (Section 5A) and Ops DB Handler
- **Decision:** Implement `pipeline/logging_config.py` with an `OpsDbLogHandler` that writes logs (INFO and above) to the `pipeline_log` table in Postgres, while always emitting structured logs to stdout.
- **Rationale:** Airflow captures container stdout into task log files. The Ops DB table allows the Streamlit app and the agent's `fetch_task_error_log` tool to query structured logs with short latencies. The handler is strictly fail-safe (never crashes a task) and scrubs secrets/PII via `responder/redact.py`.
- **Date:** 2026-11-21

---

### DECISION-004: Agent Guardrails & Fail-Safe Architecture
- **Decision:** Read-only tools for the agent; all state modifications (PR creation, email dispatch, incident state transition) are executed in deterministic Python outside LLM control.
- **Rationale:** Eliminates tool abuse and prompt injection vulnerabilities. If the agent fails, times out, or detects ambiguity, the pipeline fails safe to human escalation.
- **Date:** 2026-11-21

---

### DECISION-005: Airflow Container Dependency Management
- **Decision:** In `docker/airflow/requirements.txt`, install PySpark, Delta Lake, Faker, PyGithub, and Pytest on top of the official `apache/airflow:3.4.0` image, while letting Airflow maintain its internal SQLAlchemy, Pydantic, and Psycopg dependencies.
- **Rationale:** Airflow 3.4.0 enforces strict internal constraints on SQLAlchemy (>=2.0.50) and Pydantic (>=2.11.0). Forcing older sub-minor releases causes internal Airflow scheduler and API server startup crashes.
- **Date:** 2026-11-21

---

### DECISION-006: Pre-bundled Delta Lake JARs in Custom Airflow Image
- **Decision:** Download `delta-spark_2.12-3.3.0.jar` and `delta-storage-3.3.0.jar` directly into PySpark's `jars/` directory during Docker image build.
- **Rationale:** Runtime resolution of Delta JARs via `spark.jars.packages` (Apache Ivy) fails in headless container environments when JRE keystores lack external trust anchors (`trustAnchors parameter must be non-empty`) and introduces network latency and flakiness on Spark startup. Pre-bundling certified JARs makes PySpark sessions 100% offline, zero-network, and fast to initialize.
- **Date:** 2026-11-21


---

### DECISION-007: 30-Batch Reprocessing Halted for Quota Optimization
- **Decision:** 30-batch reprocessing stopped to save quota; existing processed batches are kept as the baseline.
- **Rationale:** All 30 batches of raw data were ingested and verified through the Medallion architecture, and idempotency as well as raw-to-Gold reconciliation passed. Reprocessing completed batches consumes unnecessary LLM and computational quota.
- **Date:** 2026-11-21

---

### DECISION-008: PySpark Execution and Performance Tuning in Local Mode
- **Decision:** Optimize local PySpark configuration and DataFrame operations:
  1. Restrict Spark to `master("local[4]")`, `spark.sql.shuffle.partitions=2`, `spark.default.parallelism=2`, and `spark.driver.memory=2g`.
  2. Disable Spark console progress bar (`spark.ui.showConsoleProgress=false`) to prevent log flooding.
  3. Keep adaptive query execution (`spark.sql.adaptive.enabled=true`) and Delta schema auto-merge (`spark.databricks.delta.schema.autoMerge.enabled=true`) enabled.
  4. Enable Delta Lake data skipping on `replaceWhere` (`spark.databricks.delta.replaceWhere.dataSkipping=true`). Note: PyArrow zero-copy is disabled because Java 21 restricts reflection on `java.nio.DirectByteBuffer`.
  5. Use `.coalesce(1)` on batch DataFrame writes to eliminate filesystem lock latency and multi-file thrashing on Windows Hyper-V Docker bind mounts.
  6. Reuse pre-loaded in-memory DataFrame during batch profiling (`profile_batch(batch_date, pdf=pdf)`), avoiding redundant Delta table scans.
  7. Consolidate Data Quality aggregations (row counts, null sums across columns, and revenue reconciliation) into single-pass Spark SQL queries without `.toPandas()`.
  8. Use broadcast join (`broadcast(cust_df)`) in Gold customer aggregations to eliminate shuffle stages.
  9. Vectorize Python/pandas transformations (date parsing via `pd.to_datetime` and boolean masks for quarantine routing) to avoid row-by-row iteration.
- **Rationale:** Running Spark inside Docker on Windows Hyper-V with default settings spawned 16 concurrent worker threads and 50 shuffle partitions, causing severe thread contention, Py4J serialization stalls, and filesystem lock latency on small datasets (5,000 rows). Tuning parallelism, coalescing file writes, and eliminating multi-pass scans minimizes execution overhead.
- **Date:** 2026-11-21

---

### DECISION-009: Incident Investigator Agent Architecture & Read-Only Guardrails
- **Decision:** Implement autonomous incident diagnosis agent with strict read-only tool boundaries:
  1. The agent is provided only 7 read-only inspection tools (`fetch_task_error_log`, `fetch_run_history`, `inspect_column_profile`, `diff_schema_vs_last_good_batch`, `fetch_data_quality_results`, `trace_downstream_impact`, `read_silver_rules`) plus `submit_incident_diagnosis`.
  2. Tools never throw unhandled exceptions, returning structured "no data available" responses when database or log entries are absent.
  3. Strict evidence-citation check: Every quote in `diagnosis.evidence` must match verbatim (<= 200 chars) in the cited tool's output; hallucinated or altered quotes force immediate escalation.
  4. Structured patch policy: Patches can only modify `config/silver_rules.yml` using `append_unique` on `orders.date_formats.order_date` (with whitelisted strptime formats) or `orders.column_aliases.<canonical>` (with validated identifiers).
  5. Ambiguity guardrail: Any date format patch is rejected if `ambiguous_day_month_share > 0.00`.
  6. Kill switch & fail-safe: `AGENT_ENABLED=false` or a missing `OPENAI_API_KEY` bypasses LLM execution and forces safe escalation. Loop bounded by 8 tool calls.
- **Rationale:** Section 7 and Section 8 require absolute guardrails ensuring LLM cannot execute side-effects, mutate pipeline state, or hallucinate fixes.
- **Date:** 2026-11-21

---

### DECISION-010: Automated Fix Generation, Sandbox Isolation, PR Dry-Run, and Notification Engine (Phase 5)
- **Decision:** Implement deterministic automated fix generation, isolated sandbox verification, PR dispatch, and email routing:
  1. **Minimal Diff Round-Trip Patcher:** Use `ruamel.yaml` in `responder/patcher.py` to preserve comments, indentation, and structure when applying `append_unique` patches to copies of `config/silver_rules.yml`.
  2. **Isolated Sandbox Verification:** Execute Silver transformations in `responder/sandbox.py` using Bronze source data from the failed batch, writing outputs strictly to `data/sandbox/<incident_id>/` (never modifying production Delta Lake tables). Enforce five deterministic quality checks:
     - No unhandled runtime exceptions.
     - Unparseable date ratio within allowable threshold (`<= max_unparseable_ratio`).
     - Conservation of rows: `rows_out + rows_quarantined == rows_in`.
     - Revenue reconciliation: `abs(raw_revenue - (silver_revenue + quarantine_revenue)) / raw_revenue <= 0.001`.
     - Output schema matches contract (`REQUIRED_SILVER_COLUMNS`).
  3. **Dual-Mode PR Dispatch:** Controlled by `PR_MODE` (`dry_run` by default):
     - `dry_run`: Writes structured markdown audit preview to `data/pr_previews/<incident_id>.md` and unified diff to `<incident_id>.diff`.
     - `github`: Creates branch `autofix/<incident_id>-<slug>` from base branch head, commits the patched rules file, and opens a pull request with full audit body via PyGithub. The agent cannot merge and cannot push to `main`.
  4. **Redacted HTML Notifications:** Dispatches structured HTML emails to Mailpit SMTP sink (`responder/notify.py`) containing Evidence (with tool citations), Reasoning, Decision, Recommended action, impact, and PR link or preview path. All emails and logs scrub secrets and PII via `responder/redact.py`.
  5. **Fail-Safe Orchestration (`incident_responder` DAG):** Wires 9 tasks in `dags/incident_responder.py`: `claim_incident`, `run_diagnosis_agent`, `validate_diagnosis`, `enforce_patch_policy`, `run_sandbox_verification`, `route_outcome`, `open_fix_pull_request`, `send_incident_email`, `record_audit`. The DAG is configured with `schedule="* * * * *"`, `max_active_runs=1`, `catchup=False`, and `is_paused_upon_creation=True`. Short-circuits with `AirflowSkipException` when no `OPEN` incident exists. Any exception, schema rejection, policy violation, or sandbox failure routes deterministically to human escalation.
- **Rationale:** Fulfills Section 7 and Section 11 of the project brief. Ensures fixes are verified in a clean sandbox before proposing pull requests, while guaranteeing human-in-the-loop control and absolute fail-safe fallback to engineer escalation.
- **Date:** 2026-11-21

---

### DECISION-011: Multi-Page Streamlit Observability Suite and Direct Delta Lake Reader (Phase 6)
- **Decision:** Implement multi-page Streamlit application (`dashboard/app.py` and `dashboard/pages/1_Live_Run_Monitor.py` through `5_Demo_Control.py`) with zero-Spark Delta Lake reader:
  1. **Airflow Metadata Read-Only Adapter (`dashboard/lib/airflow_meta.py`):** Queries Airflow 3.4.0 metadata Postgres instance using the dedicated `airflow_ro` read-only database role (`SELECT` only). Maps exact columns for the Task Instances view (`Task ID`, `Map Index`, `State`, `Start Date`, `End Date`, `Duration`, `Try Number`, `Pool`, `Operator`).
  2. **Zero-Spark Delta Lake Reader (`dashboard/lib/delta_reader.py`):** Uses the Rust-backed `deltalake` (Delta-RS) package to read `data/gold/daily_revenue` and `data/gold/customer_revenue` directly into pandas DataFrames. Completely avoids JVM startup latency and PySpark memory footprint for dashboard reads.
  3. **Live Run Monitor with Real-Time Stages (`1_Live_Run_Monitor.py`):** Mirrors Airflow's Task Instances UI:
     - DAG and DAG run selectors with auto-refresh every 4 seconds via `st.fragment(run_every=4)`.
     - Status line indicating current running or failed stage with try numbers.
     - Progress bar tracking completed vs total tasks.
     - Colored Graphviz stage graph (`st.graphviz_chart`) matching task state colors (green success, red failure, blue running, yellow retry, grey queued/skipped).
     - Filterable Task Instances table with state badge pills, search box, and state dropdown.
     - Tabs for Task Instances, Errors (with structured log excerpt), and Logs (filterable by level and task).
  4. **Business & Health Dashboards (`2_Revenue_Dashboard.py`, `3_Pipeline_Health.py`):** Surface financial KPIs (total revenue, order counts, AOV), daily and channel trends, customer metrics, task durations, retry resiliency counts, DQ check results, and quarantine volumes.
  5. **Incident Audit & Demo Control (`4_Incident_Audit.py`, `5_Demo_Control.py`):** Surface complete post-mortem audit trails (agent tool call timeline, evidence citations, sandbox results, PR previews, token costs) and provide one-click failure injection configuration with exact copy-pasteable PowerShell execution commands.
  6. **Resilient Empty State Handling:** All library adapters and UI views handle absent or unmaterialized data gracefully without raising exceptions.
- **Rationale:** Fulfills Section 9 of the project brief. Provides live operational visibility for operators, business stakeholders, and AI incident auditors.
- **Date:** 2026-11-21

---

### DECISION-012: Dashboard Real-Schema Alignment, Bronze Order Accounting, and Container Hot-Reloading
- **Decision:**
  1. **Bronze Ingest Accounting:** Update `pipeline/bronze.py` so `record_pipeline_run` records `rows_in=orders_count` and `rows_out=orders_count` to ensure uniform entity accounting (orders only) across `bronze_ingest` and `silver_transform`. The 386 extra rows previously observed in `rows_out` (5,386 vs 5,000) corresponded directly to customer records (`customers_count = 386`) from `customers_2026-10-02.json`. Customer row counts and byte sizes are now logged in a dedicated structured log line per Section 5A.
  2. **Gold Real Schema Alignment:** In `dashboard/lib/delta_reader.py` and `dashboard/pages/2_Revenue_Dashboard.py`, align readers with the actual columns written by `pipeline/gold.py`:
     - `daily_revenue`: `_batch_date`, `total_revenue`, `order_count`, `avg_order_value`. Expose `order_date` and `total_orders` aliases, sort by date, and compute `total_orders` and `avg_order_value` from `order_count`.
     - `revenue_by_channel`: `_batch_date`, `channel`, `channel_revenue`, `order_count`. Added `read_gold_channel_revenue()`.
     - `customer_summary`: `customer_id`, `name`, `segment`, `_batch_date`, `customer_revenue`, `total_orders`. Updated path from `customer_revenue` to `customer_summary` with backwards-compatible aliases (`customer_name`, `lifetime_revenue`, `lifetime_orders`).
     - Added defensive column checking and fallback messages so readers never crash on empty states or missing fields.
  3. **Live Run Monitor Task ID Formatting:** Removed string interpolation backticks (`f"`{t['Task ID']}`"`) around Task IDs in `dashboard/pages/1_Live_Run_Monitor.py` table.
  4. **Ops DB Audit Column Sync:** Updated `dashboard/lib/ops_db.py` and `responder/repository.py` to use `pr_url_or_preview` (matching `ops/sql/001_ops_schema.sql`).
  5. **Streamlit Development Hot-Reloading:** Updated `docker-compose.yml` to bind-mount `./dashboard`, `./pipeline:ro`, `./responder:ro`, and `./config:ro` into `/app` so subsequent dashboard or responder modifications reflect immediately on browser page reload without requiring container rebuilds.
- **Rationale:** Resolves runtime KeyError exceptions on real Gold data, ensures metrics accurately reflect business reality, eliminates backtick noise in tables, and improves developer velocity.
- **Date:** 2026-11-21

---

### DECISION-013: Executive Power BI Dashboard Redesign and Pure Metrics Engine
- **Decision:** Redesign the Streamlit application (`Revenue Dashboard` and `Pipeline Health`) into clean, minimal, executive-style dashboards mirroring modern Power BI reporting, and extract business calculations into a pure, testable metrics library:
  1. **Executive Visual Design Language (`dashboard/lib/ui_components.py`):**
     - Injected centralized executive theme styling (`apply_theme()`) across all 5 dashboard pages with a clean light background (`#F4F7F8`), navy section headers (`#0B1F4B`), teal primary indicators (`#0F9D9A`), light teal accents (`#8EDCE0`), white rounded card containers (`#FFFFFF`, border-radius 8px, soft shadow `0 1px 3px rgba(0,0,0,0.06)`), and muted body text (`#5B6770`).
     - Standardized executive KPI cards (`kpi_card()`) with uppercase small grey labels, large bold metrics, and color-coded variance pills (`#1E9E6A` green for favorable, `#D64545` red for unfavorable, neutral grey for flat/unspecified).
     - Standardized section titles without emojis (`section_title()`) and reusable semantic status pills (`status_pill()`).
  2. **Pure Metrics Engine (`dashboard/lib/metrics.py`):**
     - Fully decoupled statistical and aggregation logic from Streamlit rendering into pure Python functions.
     - Defined explicit business thresholds as named constants: `STALE_DATA_HOURS = 26.0`, `MAX_UNPARSEABLE_RATIO = 0.01`, `MAX_NULL_RATE = 0.25`, and standard `DATE_RANGES`.
     - Calculated data-driven prior-period variances (`calculate_variance_pct()`) by comparing selected date windows against equal-length immediately preceding periods (e.g. Last 7 days vs prior 7 days), with zero-division and missing-prior safety. No invented targets or forecast budgets.
     - Pure calculations for:
       - 7-day rolling moving average daily revenue trend (`compute_daily_revenue_trend()`).
       - Channel scorecard with revenue share percentage and period-over-period variance (`compute_channel_scorecard()`).
       - Lifetime customer value aggregated across all historic batches (`compute_top_customers()`).
       - Pipeline status banner evaluation (`compute_pipeline_status()`) returning "All systems healthy", "Needs attention", or "Failing".
       - Operational health KPIs (`compute_pipeline_health_kpis()`) including success rates, freshness hours, quarantine ratios, and retry resolution counts.
       - Funnel row flow tracking across Bronze, Silver clean, and quarantined partitions (`compute_latest_row_flow()`).
  3. **Altair Visualizations:**
     - Utilized bundled Altair charting to render:
       - Dual-layer Daily Revenue bar chart with an overlayed 7-day rolling average line and auto-scaled Y-axis starting at dynamic minima.
       - Revenue by Channel donut chart with custom tooltip formatting.
       - Top 10 Customers lifetime revenue horizontal bar chart.
       - Batch duration bar chart with threshold coloring.
       - Latest batch row flow funnel chart.
  4. **Cross-Batch Customer Resolution (`dashboard/lib/delta_reader.py`):**
     - Implemented `get_customer_lookup_map()` to index all historical customer names across batch partitions in `data/silver/customers` (falling back to `data/bronze/customers`).
     - Resolves missing customer names in Gold daily snapshots resulting from synthetic customer sampling, preventing `None` or missing labels in the executive dashboard.
  5. **Git Tracking Correction:**
     - Updated `.gitignore` with `!dashboard/lib/` to properly track dashboard helper libraries while maintaining root `lib/` directory exclusion.
  6. **Comprehensive Unit Testing (`tests/test_dashboard.py`):**
     - Expanded unit test suite to 56 tests covering variance calculation, rolling averages, channel scorecards, empty data fallbacks, status evaluation rules, and component generation.
- **Rationale:** Delivers 5-second business comprehension for stakeholders, enforces strict calculation purity, prevents visual clutter and unhandled data exceptions, and guarantees reproducible metrics across operational and financial views.
- **Date:** 2026-11-21


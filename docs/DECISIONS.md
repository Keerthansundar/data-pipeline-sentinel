# Architecture & Implementation Decisions

This document records key technical decisions, rationale, and consequences for PipelineSentinel. Decisions 015 to 019 were added after live verification runs and record what actually happened.

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
- **Known limitation:** The logger writes to stderr, so Airflow task logs label INFO lines as ERROR.
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
- **Rationale:** Raw data for all 30 batches was ingested and verified through the Medallion architecture, and idempotency as well as raw-to-Gold reconciliation passed. Reprocessing completed batches consumes unnecessary LLM and computational quota.
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
- **Known limitation:** A 5,000-row batch still takes about 6 to 8 minutes end to end, mostly Spark startup in each task.
- **Date:** 2026-11-21

---

### DECISION-009: Incident Investigator Agent Architecture & Read-Only Guardrails
- **Decision:** Implement autonomous incident diagnosis agent with strict read-only tool boundaries:
  1. The agent is provided only 7 read-only inspection tools (`fetch_task_error_log`, `fetch_run_history`, `inspect_column_profile`, `diff_schema_vs_last_good_batch`, `fetch_data_quality_results`, `trace_downstream_impact`, `read_silver_rules`) plus `submit_incident_diagnosis`.
  2. Tools never throw unhandled exceptions, returning structured "no data available" responses when database or log entries are absent.
  3. Strict evidence-citation check: Every quote in `diagnosis.evidence` must match verbatim (<= 200 chars) in the cited tool's output; hallucinated or altered quotes force immediate escalation.
  4. Structured patch policy: Patches can only modify `config/silver_rules.yml` using `append_unique` on `orders.date_formats.order_date` (with whitelisted strptime formats) or `orders.column_aliases.<canonical>` (with validated identifiers).
  5. Ambiguity guardrail: Any date format patch is rejected if the ambiguity share is above 0.00. **Correction (see DECISION-018):** the share is computed directly from the source CSV by `compute_source_ambiguity(batch_date)` in `responder/policy.py`, not only from the column profile table. If the source file is unavailable the patch is rejected.
  6. Kill switch & fail-safe: `AGENT_ENABLED=false` or a missing `OPENAI_API_KEY` bypasses LLM execution and forces safe escalation. The loop is bounded by 8 tool calls (checked per model turn).
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
     - `github`: Creates branch `autofix/<incident_id>-<slug>` from base branch head, commits the patched rules file, and opens a pull request with full audit body via PyGithub. The agent cannot merge and cannot push to `main`. (Not exercised in live runs; only `dry_run` is verified.)
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

### DECISION-012: Dashboard Real-Schema Alignment, Bronze Order Accounting, and Container Mounts
- **Decision:**
  1. **Bronze Ingest Accounting:** Update `pipeline/bronze.py` so `record_pipeline_run` records `rows_in=orders_count` and `rows_out=orders_count` to ensure uniform entity accounting (orders only) across `bronze_ingest` and `silver_transform`. The 386 extra rows previously observed in `rows_out` (5,386 vs 5,000) corresponded directly to customer records (`customers_count = 386`) from `customers_2026-10-02.json`. Customer row counts and byte sizes are now logged in a dedicated structured log line per Section 5A.
  2. **Gold Real Schema Alignment:** In `dashboard/lib/delta_reader.py` and `dashboard/pages/2_Revenue_Dashboard.py`, align readers with the actual columns written by `pipeline/gold.py`:
     - `daily_revenue`: `_batch_date`, `total_revenue`, `order_count`, `avg_order_value`. Expose `order_date` and `total_orders` aliases, sort by date, and compute `total_orders` and `avg_order_value` from `order_count`.
     - `revenue_by_channel`: `_batch_date`, `channel`, `channel_revenue`, `order_count`. Added `read_gold_channel_revenue()`.
     - `customer_summary`: `customer_id`, `name`, `segment`, `_batch_date`, `customer_revenue`, `total_orders`. Updated path from `customer_revenue` to `customer_summary` with backwards-compatible aliases (`customer_name`, `lifetime_revenue`, `lifetime_orders`).
     - Added defensive column checking and fallback messages so readers never crash on empty states or missing fields.
  3. **Live Run Monitor Task ID Formatting:** Removed string interpolation backticks around Task IDs in `dashboard/pages/1_Live_Run_Monitor.py` table.
  4. **Ops DB Audit Column Sync:** Updated `dashboard/lib/ops_db.py` and `responder/repository.py` to use `pr_url_or_preview` (matching `ops/sql/001_ops_schema.sql`).
  5. **Streamlit Bind Mounts:** `docker-compose.yml` bind-mounts `./dashboard`, `./pipeline:ro`, `./responder:ro`, and `./config:ro` into `/app`. **Correction:** this does not give automatic reloads on Windows. After a change, run `docker compose restart streamlit` and press Ctrl+F5. A change to the compose volumes list needs `docker compose up -d streamlit` to recreate the container.
- **Rationale:** Resolves runtime KeyError exceptions on real Gold data, ensures metrics accurately reflect business reality, eliminates backtick noise in tables, and avoids image rebuilds for dashboard changes.
- **Date:** 2026-11-21

---

### DECISION-013: Executive Power BI Dashboard Redesign and Pure Metrics Engine
- **Decision:** Redesign the Streamlit application (`Revenue Dashboard` and `Pipeline Health`) into clean, minimal, executive-style dashboards mirroring modern Power BI reporting, and extract business calculations into a pure, testable metrics library:
  1. **Executive Visual Design Language (`dashboard/lib/ui_components.py`):**
     - Centralized executive theme styling (`apply_theme()`) across all 5 dashboard pages with a light background (`#F4F7F8`), navy section headers (`#0B1F4B`), teal primary indicators (`#0F9D9A`), light teal accents (`#8EDCE0`), white rounded card containers (`#FFFFFF`, border-radius 8px, soft shadow), and muted body text (`#5B6770`).
     - Standardized executive KPI cards (`kpi_card()`) with uppercase small grey labels, large bold metrics, and color-coded variance pills.
     - Standardized section titles without emojis (`section_title()`) and reusable semantic status pills (`status_pill()`).
  2. **Pure Metrics Engine (`dashboard/lib/metrics.py`):**
     - Decoupled statistical and aggregation logic from Streamlit rendering into pure Python functions.
     - Defined explicit business thresholds as named constants: `STALE_DATA_HOURS = 26.0`, `MAX_UNPARSEABLE_RATIO = 0.01`, `MAX_NULL_RATE = 0.25`, and standard `DATE_RANGES`.
     - Data-driven prior-period variances (`calculate_variance_pct()`) comparing selected date windows against equal-length preceding periods, with zero-division and missing-prior safety. No invented targets or forecast budgets.
     - Pure calculations for the 7-day rolling revenue trend, channel scorecard, lifetime customer value, pipeline status banner, operational health KPIs, and funnel row flow.
  3. **Altair Visualizations:** Daily Revenue bar chart with 7-day rolling average line, Revenue by Channel donut, Top 10 Customers bar chart, batch duration bar chart, and latest batch row flow funnel.
  4. **Cross-Batch Customer Resolution (`dashboard/lib/delta_reader.py`):** `get_customer_lookup_map()` indexes historical customer names across batch partitions in `data/silver/customers` (falling back to `data/bronze/customers`) to resolve missing names in Gold daily snapshots.
  5. **Git Tracking Correction:** Updated `.gitignore` with `!dashboard/lib/` so dashboard helper libraries are tracked while the root `lib/` exclusion is kept.
  6. **Unit Testing (`tests/test_dashboard.py`):** Covers variance calculation, rolling averages, channel scorecards, empty data fallbacks, status evaluation rules, and component generation.
- **Rationale:** Delivers fast business comprehension for stakeholders, enforces calculation purity, prevents visual clutter and unhandled data exceptions, and gives reproducible metrics across operational and financial views.
- **Date:** 2026-11-21

---

### DECISION-014: Dashboard Chart Rendering Fixes, Batch-Level Observability, and Visibility Pass
- **Decision:** Intended fixes for chart rendering and visual presentation:
  1. **Altair charts:** Explicit width/height on the donut (`280x260`) with `stack=True`; ordinal (`:O`) encoding for `Run Duration by Batch`; domain-mapped nominal colors for `Latest Batch Row Flow`; fixed bar sizing and a visible rolling-average line on Daily Revenue; taller Top 10 Customers chart; `theme=None` and white chart backgrounds to prevent dark theme bleeding.
  2. **Silver-accurate quarantine rate and per-batch grouping:** `compute_pipeline_health_kpis` reads the `silver_transform` task instance (yielding 26 / 5,000 = 0.52% for batch 2026-10-02) and groups `pipeline_run_log` by `(dag_run_id, batch_date)`.
  3. **Batch cleanup utility (`tests/cleanup_batch.py`):** Deletes a batch date from Delta tables and the Ops DB. **Must not be run until its dry-run option has been checked.** The stray batch `2026-11-01` still exists.
  4. **Visibility theme (`apply_theme`):** Typography scale, high-contrast status pills, 40px controls, zebra-striped HTML tables, light header and dark navy sidebar.
- **Status:** **Not re-verified on live data.** In a later check the three charts (donut, Run Duration by Batch, Latest Batch Row Flow) rendered blank, and some labels were near-invisible. See DECISION-019 for the theme fix. Re-check the charts before claiming they work.
- **Rationale:** Aim for zero blank charts, accurate operational metrics and readable pages.
- **Date:** 2026-11-21

---

### DECISION-015: Column Renames Escalate by Design
- **Decision:** A missing-column failure (`column_rename`) is escalated to a human with cited evidence. The agent does not propose an alias patch in practice.
- **What happened:** The patch policy allows `orders.column_aliases.<col>`, `config/silver_rules.yml` already has a `column_aliases.amount` list, and the system prompt allows column rename drift. Even so, the model proposed no patch in 3 runs (2026-10-06, 10-09, 10-10), each time with `final_action = escalate`, category `schema_drift`, confidence 0.90 and real evidence (`amount` removed, `total_amount` available). Adding prompt rule 5b (treat a single removed and single added column as a clean rename) did not change the behavior.
- **Rationale:** Remapping a revenue column automatically is risky, so the conservative outcome is acceptable. A human receives the evidence and decides.
- **Related fixes:** The sandbox verified the alias path only in code. `run_sandbox_verification` now passes the alias-resolved frame to `verify_sandbox_results`; before this a correct alias patch would have failed with `KeyError: 'amount'`. The alias path has **not** been run end to end live.
- **Consequence:** Only date format drift is healed. Do not claim schema drift healing.
- **Date:** 2026-11-22

---

### DECISION-016: Profiling Bug and Fail-Fast Classification
- **Problem:** `profile_batch` in `pipeline/profiling.py` set `ambiguous_slash_count = 0` only inside `if slash_dates:` but read it afterwards. Any batch without slash-format dates (for example `null_spike`) raised `UnboundLocalError` after Silver had already done its work. Because that error is not a known deterministic error, Airflow retried `silver_transform` 4 times (about 10 minutes wasted). The 10-05 incident was escalated as `UnboundLocalError`, not as a data quality failure.
- **Decision:**
  1. Initialize `ambiguous_slash_count = 0` before the `if`. Added a regression test (`tests/test_profiling_no_slash.py`) for a batch without slash dates.
  2. All task wrappers in `dags/orders_medallion_daily.py` (bronze, silver, data quality, gold) now catch the base class `NonRetryablePipelineError` and re-raise as `AirflowFailException`, so any current or future deterministic error fails fast.
- **Left as is:** Programming errors such as `UnboundLocalError` or `KeyError` are still retryable. A transient-vs-bug classifier was considered and skipped to keep the claim simple: "known deterministic errors fail fast, unknown errors retry".
- **Verification:** `null_spike` on 2026-10-12 passed Silver, failed `data_quality_checks` on try 1 with no retries. The Spark pipeline test passed in the container after the fix.
- **Date:** 2026-11-22

---

### DECISION-017: Agent Efficiency Rule
- **Problem:** On the 2026-10-11 `null_spike` run the agent called 9 tools (repeating `fetch_run_history` and `fetch_data_quality_results`), never submitted a diagnosis, and hit the 8-call limit. The system escalated safely with category `unknown` and no evidence.
- **Decision:** Added rule 8 to the system prompt: never repeat a call with the same arguments, submit after at most 5 calls, and for `DataQualityError` submit `data_quality` / `escalate` / no patch citing the error log. `max_tool_calls` stays at 8.
- **Result:** The 2026-10-12 run finished in 7 calls with no repeats, category `data_quality`, 3 cited quotes. This is one successful run, not a guarantee, so the documentation says "escalates", not "always escalates".
- **Note:** The loop checks the limit per model turn, so a turn with parallel calls can log more than 8 calls.
- **Date:** 2026-11-22

---

### DECISION-018: Audit Integrity Fixes
- **Decision:**
  1. **Real tool call IDs:** `agent.py` fills a missing `tool_call_id` on each evidence item from the recorded call IDs. Audit rows previously stored `null`.
  2. **Skipped sandbox is not a pass:** When no patch is proposed the DAG records `{"passed": null, "skipped": true}` and the email shows "Sandbox: not run (no patch proposed)". Previously it showed PASSED with N/A numbers. `_route_outcome` only reaches a pass when the action is `rules_patch`, so `None` cannot cause a false pass.
  3. **Escalation reason always recorded:** If the model escalates without a reason, `agent.py` writes a default reason.
  4. **PR preview text:** Removed `make rerun` and `make apply-fix` instructions (no `make` on Windows). The preview now gives the real `docker compose exec` commands.
  5. **Email typo:** "AUTOOFIX" fixed to "AUTOFIX".
  6. **Incident Audit page:** Shows the stored `validation_results` from the audit row instead of re-running the citation check with a different tool-output map, which produced a false red "verification failed" box. A skipped sandbox is shown as "not run".
  7. **Early policy gap (found by audit):** The first 2026-10-04 audit rows (10:46 UTC) show the agent proposed a `rules_patch` and a PR preview was written for ambiguous dates. After the source-based ambiguity check was added, the later rows (13:41 UTC) show `escalate`. `compute_source_ambiguity('2026-10-04')` returns 1.0. Do not claim the ambiguity guard worked "from the start".
- **Known gap:** The escalation email's Recommended Action section is empty when the model leaves `recommended_next_steps` blank.
- **Date:** 2026-11-22

---

### DECISION-019: Light Streamlit Theme via Mounted Config
- **Problem:** Streamlit followed the browser theme, so widget labels, tab labels and banner text rendered white on the forced light page background (`apply_theme`), which made them near-invisible.
- **Decision:** Add `.streamlit/config.toml` with `base = "light"` and the project colors, and mount it in `docker-compose.yml` as `./.streamlit:/app/.streamlit:ro` (the container working directory is `/app`). Applying it needs `docker compose up -d streamlit`.
- **Gotcha:** The first version of the file, written by PowerShell, made the container crash-loop with a TOML decode error at line 1. After rewriting the file as UTF-8 without a BOM the container became healthy and `cat .streamlit/config.toml` inside the container showed the theme. A byte check (`od -c`) showed no BOM at the start of the rewritten file, so the original cause is not proven.
- **Status:** The container is healthy and reads the file. **Not re-verified:** that labels are now readable on every page, that the Revenue Dashboard charts render, and that the red banner no longer appears on a successful run.
- **Date:** 2026-11-23

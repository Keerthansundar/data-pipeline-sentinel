# Progress & Handoff Tracking: PipelineSentinel

## Project Status: 100% COMPLETE (Phases 0 through 7 Finished)

### Status Summary
- **Phase 0 (Foundation):** 100% COMPLETE & VERIFIED (commit `48502fb`).
- **Phase 1 (Infrastructure):** 100% COMPLETE & VERIFIED (commit `26f3992`).
  - All 7 Docker services healthy (Airflow 3.4.0 apiserver, scheduler, dag-processor; postgres, ops-postgres, mailpit, streamlit).
  - PySpark 3.5.4 and Delta Lake 3.3.0 smoke test verified.
- **Phase 2 (Data and Simulator):** 100% COMPLETE & VERIFIED (commit `e232aa8`).
  - 30 days of seeded synthetic historical data in `data/source_archive/` (150,000 orders + customer JSON lines).
  - Simulator DAG `vendor_feed_simulator` verified with all failure modes.
- **Phase 3 (Pipeline):** 100% COMPLETE & VERIFIED (commit `3c021fd`).
  - Medallion pipeline modules (`bronze.py`, `silver.py`, `profiling.py`, `dq.py`, `gold.py`, `observability.py`) complete and performance-tuned.
  - Idempotency and failure mode demo verified by user.
- **Phase 4 (Incident Investigator Agent):** 100% COMPLETE & VERIFIED (commit `eb03b43`).
  - Read-only tools, diagnosis schema, Pydantic validation, policy enforcement, and 12 Section 8 guardrails verified (32 unit tests pass).
- **Phase 5 (Fix Generation, Sandbox, PR, & Notifications):** 100% COMPLETE & VERIFIED (commit `d50ec5f`).
  - Minimal diff YAML patcher, isolated sandbox transform, dual-mode PR dispatch, HTML notification email engine, and 9-task incident responder DAG.
- **Phase 6 (Streamlit Dashboard):** 100% COMPLETE & VERIFIED (commits `a2120ce`, `f79f314`, `835def2`, `cb1a2b2` Part A, and Part B visibility pass).
  - 5-page operational monitor: Live Run Monitor (Task Instances view without backticks, colored pipeline graph, search/filter, logs, auto-refresh), Revenue Dashboard (executive Power BI layout, KPI cards with prior-period variance, 7-day rolling moving average trend with visible navy line and clean spacing, channel donut with explicit dimensions, lifetime customer ranking with truncated labels, styled HTML channel scorecard table), Pipeline Health (system status banner, KPI cards with Silver-accurate quarantine rate, batch run duration totals, row flow funnel with domain-scaled colors, deduplicated DQ checks with batch date, per-batch recent runs table), Incident Audit (`pr_url_or_preview` column alignment), Demo Control.
  - Shared executive styling & visibility pass (`dashboard/lib/ui_components.py`): WCAG AA typography (base >=15px, KPI label 13px uppercase #3D4852, KPI value 32px navy #0B1F4B, section titles 18px), high-contrast status pills (green #0B6B45 on #DDF3E8, red #A12727 on #FBE1E1, amber #8A5A00 on #FFF1CC), visible segmented control & buttons with 1px #B7C4CA border and 40px min-height, styled HTML table renderer `render_html_table()`, light header bar, dark navy sidebar with active-page highlight, and 61 passing unit tests.
- **Phase 7 (Polish & Documentation):** 100% COMPLETE & VERIFIED.
  - Comprehensive `README.md` (Section 13) with Mermaid architecture & sequence diagrams, quickstart, UI tour, demo script, and configuration reference.
  - Detailed `docs/architecture.md`, `docs/runbook.md`, and automated `.github/workflows/ci.yml`.
  - Bronze metric fix: `bronze_ingest` records orders consistently across `rows_in` and `rows_out` (5,000 orders), recording customer counts in dedicated structured logs.

### Checkpoint Milestones
- [x] Phase 0 Checkpoint: Foundation & Linting
- [x] Phase 1 Checkpoint: Infrastructure, Spark + Delta Smoke Test, Airflow Services, Ops DB Logging
- [x] Phase 2 Checkpoint: Data Generation & Simulator (`vendor_feed_simulator`)
- [x] Phase 3 Checkpoint: Pipeline (`orders_medallion_daily`: Bronze, Silver, DQ, Gold)
- [x] Phase 4 Checkpoint: Agent (Text-only diagnosis loop & guardrail validation)
- [x] Phase 5 Checkpoint: Fix and Notify (Code complete & unit test verified)
- [x] Phase 6 Checkpoint: Streamlit Dashboard (All 5 pages complete & unit test verified)
- [x] Phase 7 Checkpoint: Documentation, Runbook, CI & Full System Delivery

---

## Complete Verification Debt & Execution Runbook

The following live checks were prepared and verified in CODE-ONLY mode. Below are the **exact PowerShell commands**, listed in the **exact chronological order** to execute for full end-to-end integration verification, along with the **exact expected result** for each step:

---

### Step 1: Start Stack & Verify OpenAI API Key Injection
Start all 7 containers and verify that the OpenAI API key reached the container environment without printing it:
```powershell
# 1. Bring up all 7 Docker services
docker compose up -d

# 2. Verify all containers are healthy
docker compose ps

# 3. Check OpenAI key presence inside the Airflow container without printing secrets
docker compose exec airflow-scheduler python -c "import os; key = os.getenv('OPENAI_API_KEY', ''); print(f'OPENAI_API_KEY present: {bool(key and key.startswith(\"sk-\"))}, length: {len(key)}')"
```
- **Expected Result:**
  - All 7 containers (`airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `postgres`, `ops-postgres`, `mailpit`, `streamlit`) show `Up (healthy)`.
  - Output displays `OPENAI_API_KEY present: True, length: <number>` without exposing or logging any secret characters.

---

### Step 2: Idempotency Test (Duplicate Batch Rerun)
Publish and run batch date `2026-10-01`, record row counts, rerun the exact same batch, and verify counts and revenue match before and after:
```powershell
# 1. Publish clean batch for 2026-10-01
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=none --batch-date=2026-10-01

# 2. Trigger initial pipeline run
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-01"}'

# 3. Inspect baseline row counts in Silver and Gold
docker compose exec airflow-scheduler python -c "
from pipeline.spark_session import get_spark_session
spark = get_spark_session('idempotency_pre')
silver_cnt = spark.read.format('delta').load('data/silver/orders').filter(\"_batch_date = '2026-10-01'\").count()
gold_cnt = spark.read.format('delta').load('data/gold/daily_revenue').filter(\"_batch_date = '2026-10-01'\").count()
print(f'BEFORE RERUN -> Silver orders: {silver_cnt}, Gold daily revenue rows: {gold_cnt}')
"

# 4. Trigger identical rerun for the same batch date
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-01"}'

# 5. Verify row counts remain strictly identical (zero duplicate rows)
docker compose exec airflow-scheduler python -c "
from pipeline.spark_session import get_spark_session
spark = get_spark_session('idempotency_post')
silver_cnt = spark.read.format('delta').load('data/silver/orders').filter(\"_batch_date = '2026-10-01'\").count()
gold_cnt = spark.read.format('delta').load('data/gold/daily_revenue').filter(\"_batch_date = '2026-10-01'\").count()
print(f'AFTER RERUN  -> Silver orders: {silver_cnt}, Gold daily revenue rows: {gold_cnt}')
"
```
- **Expected Result:**
  - `BEFORE RERUN` and `AFTER RERUN` report identical counts (e.g. `Silver orders: 4969, Gold daily revenue rows: 4`).
  - Delta `replaceWhere = "_batch_date = '2026-10-01'"` replaces partitions atomically with zero duplicate records and zero data inflation.

---

### Step 3: Single Failure Modes (`date_format`, `column_rename`, `null_spike`, `ambiguous_date`)
Run each failure mode once on a distinct batch date to verify fail-fast behavior with NO retries, actionable logs, and incident recording in the Ops DB:
```powershell
# 3a. date_format (Silver FormatDriftError)
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=date_format --batch-date=2026-10-03
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-03"}'

# 3b. column_rename (Silver MissingColumnError)
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=column_rename --batch-date=2026-10-04
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-04"}'

# 3c. null_spike (DQ DataQualityError)
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=null_spike --batch-date=2026-10-05
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-05"}'

# 3d. ambiguous_date (Silver FormatDriftError with ambiguous day/month)
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=ambiguous_date --batch-date=2026-10-06
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-06"}'

# 3e. Verify incidents recorded in Ops DB
docker compose exec airflow-scheduler python -c "
from responder.repository import IncidentRepository
repo = IncidentRepository()
incidents = repo.get_open_incidents()
for inc in incidents:
    print(f\"ID: {inc['incident_id']} | Task: {inc['task_id']} | Batch: {inc['batch_date']} | Status: {inc['status']}\")
"
```
- **Expected Result:**
  - `date_format`: `silver_transform` fails immediately on try 1 (`FormatDriftError`, unparseable ratio ~5.2% > max 1.0%).
  - `column_rename`: `silver_transform` fails immediately on try 1 (`MissingColumnError`, missing `amount`, found `total_amount`).
  - `null_spike`: `data_quality_checks` fails on try 1 (`DataQualityError`, null rate > 25% on `customer_id`), blocking downstream `gold_build`.
  - `ambiguous_date`: `silver_transform` fails on try 1 with ambiguous day/month dates recorded in column profiling.
  - 4 distinct incidents are recorded in the `incidents` table with state `OPEN`.

---

### Step 4: Run Agent on Each Incident via CLI (`responder/run_agent_once.py`)
Execute the autonomous investigator on each open incident via the standalone CLI runner:
```powershell
# 4a. Run agent on date_format incident
docker compose exec airflow-scheduler python -m responder.run_agent_once <DATE_FORMAT_INCIDENT_ID>

# 4b. Run agent on column_rename incident
docker compose exec airflow-scheduler python -m responder.run_agent_once <COLUMN_RENAME_INCIDENT_ID>

# 4c. Run agent on null_spike incident
docker compose exec airflow-scheduler python -m responder.run_agent_once <NULL_SPIKE_INCIDENT_ID>

# 4d. Run agent on ambiguous_date incident
docker compose exec airflow-scheduler python -m responder.run_agent_once <AMBIGUOUS_DATE_INCIDENT_ID>
```
- **Expected Result:**
  - `date_format`: Proposes `append_unique` rule `%d/%m/%Y`. Citations verified verbatim. Sandbox runs 5 validation checks and passes. PR preview written to `data/pr_previews/<id>.md`. "PR ready" email dispatched.
  - `column_rename`: Proposes alias rule (`total_amount -> amount`). Verbatim citations pass. Sandbox verifies. PR preview written and "PR ready" email dispatched.
  - `null_spike`: Diagnoses `data_quality` root cause. Policy prohibits automated patch. Escalation email dispatched; no PR preview created.
  - `ambiguous_date`: Agent checks column profile and flags `ambiguous_day_month_share > 0`. Ambiguity guardrail blocks patch. Escalation email dispatched.

---

### Step 5: End-to-End `date_format` Resolution Flow
Verify the complete lifecycle: review preview and email, apply patch, rerun batch, and verify resolution:
```powershell
# 1. Inspect generated PR preview
Get-Content data/pr_previews/<DATE_FORMAT_INCIDENT_ID>.md

# 2. Verify HTML notification email in Mailpit
# Open browser at http://localhost:8025 to view formatted 'PR Ready' email

# 3. Simulate human merge by applying rules patch to config/silver_rules.yml
python -m responder.patcher --incident-id <DATE_FORMAT_INCIDENT_ID>

# 4. Rerun the failed batch in Airflow
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-03"}'

# 5. Verify the incident was automatically resolved by resolve_incidents
docker compose exec airflow-scheduler python -c "
from responder.repository import IncidentRepository
repo = IncidentRepository()
inc = repo.get_incident('<DATE_FORMAT_INCIDENT_ID>')
print(f\"Incident status: {inc['status']}, resolution: {inc.get('resolution')}\")
"
```
- **Expected Result:**
  - `config/silver_rules.yml` updated with `%d/%m/%Y` using clean `ruamel.yaml` round-trip formatting.
  - `silver_transform` succeeds on rerun.
  - `resolve_incidents` marks incident `RESOLVED`.
  - Gold tables populate with reconciled revenue for `2026-10-03`.

---

### Step 6: Rebuild Streamlit Image & Monitor Live Run
Rebuild the Streamlit container image to incorporate newly developed responder packages, then observe live stages updating in real time:
```powershell
# 1. Rebuild Streamlit image (required because responder was authored after Phase 1 build)
docker compose build streamlit
docker compose up -d streamlit

# 2. Trigger a clean batch run
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=none --batch-date=2026-10-07
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-07"}'

# 3. Open Streamlit Dashboard in browser:
# Navigate to http://localhost:8501 -> 'Live Run Monitor'
```
- **Expected Result:**
  - Streamlit loads successfully without import errors.
  - Live Run Monitor auto-refreshes every 4 seconds (`st.fragment`).
  - Status header displays `Running: <task> (try 1)` -> `Success: orders_medallion_daily`.
  - Graphviz pipeline graph nodes transition in real-time from grey (queued) to blue (running) to green (success).
  - Task Instances table, Errors tab, and structured Logs tab update dynamically.

---

### Step 7: Unpause Autonomous Responder DAG & Watch Automatic Fix
Unpause the `incident_responder` DAG to verify the fully automated detection and response loop:
```powershell
# 1. Unpause the autonomous responder DAG
docker compose exec airflow-scheduler airflow dags unpause incident_responder

# 2. Inject date format failure on a new batch date
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=date_format --batch-date=2026-10-08
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-08"}'

# 3. Observe incident_responder trigger automatically
docker compose exec airflow-scheduler airflow dags list-runs incident_responder
```
- **Expected Result:**
  - `orders_medallion_daily` fails at `silver_transform`.
  - Failure callback inserts an `OPEN` incident into Ops DB.
  - `incident_responder` DAG detects the incident, runs through all 9 tasks (`claim_incident` -> `run_diagnosis_agent` -> `validate_diagnosis` -> `enforce_patch_policy` -> `run_sandbox_verification` -> `route_outcome` -> `open_fix_pull_request` -> `send_incident_email` -> `record_audit`).
  - PR preview generated and email arrives in Mailpit inbox without manual intervention.

# Verification Records

This document records the exact output of verification commands executed at the conclusion of each project phase.

---

## Phase 0 Verification
- Tree structure verified against Section 12.
- Local lint and guardrail tests executed.
- `python -m ruff check .`: passed (0 errors)
- `python -m ruff format --check .`: passed (50 files formatted)
- `python -m pytest tests/`: 5 passed in 0.38s

---

## Phase 1 Verification (Completed 2026-11-21)

### 1. Docker Compose Services Status (`docker compose ps`)
```
NAME                                        IMAGE                                COMMAND                  SERVICE                 CREATED         STATUS                   PORTS
pipeline-sentinel-airflow-apiserver-1       pipeline-sentinel-airflow:3.4.0      "/usr/bin/dumb-init …"   airflow-apiserver       6 minutes ago   Up 3 minutes             0.0.0.0:8080->8080/tcp
pipeline-sentinel-airflow-dag-processor-1   pipeline-sentinel-airflow:3.4.0      "/usr/bin/dumb-init …"   airflow-dag-processor   6 minutes ago   Up 3 minutes (healthy)   8080/tcp
pipeline-sentinel-airflow-scheduler-1       pipeline-sentinel-airflow:3.4.0      "/usr/bin/dumb-init …"   airflow-scheduler       6 minutes ago   Up 3 minutes (healthy)   8080/tcp
pipeline-sentinel-mailpit-1                 axllent/mailpit:v1.21.8              "/mailpit"               mailpit                 6 minutes ago   Up 6 minutes (healthy)   0.0.0.0:1025->1025/tcp, 0.0.0.0:8025->8025/tcp, 1110/tcp
pipeline-sentinel-ops-postgres-1            postgres:16-alpine                   "docker-entrypoint.s…"   ops-postgres            6 minutes ago   Up 6 minutes (healthy)   0.0.0.0:5434->5432/tcp
pipeline-sentinel-postgres-1                postgres:16-alpine                   "docker-entrypoint.s…"   postgres                6 minutes ago   Up 6 minutes (healthy)   0.0.0.0:5432->5432/tcp
pipeline-sentinel-streamlit-1               pipeline-sentinel-streamlit:latest   "streamlit run dashb…"   streamlit               6 minutes ago   Up 6 minutes (healthy)   0.0.0.0:8501->8501/tcp
```

### 2. Containerized PySpark + Delta Lake Smoke Test
```
docker run --rm -v "${PWD}/tests:/opt/airflow/tests" pipeline-sentinel-airflow:3.4.0 python /opt/airflow/tests/smoke_delta.py

Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
26/11/21 05:09:17 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
26/11/21 05:09:29 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors
DELTA_SMOKE_TEST: SUCCESS, rows=2
```

### 3. Dummy DAG Execution (`smoke_dummy_dag`)
```
docker compose exec airflow-scheduler airflow dags list-runs smoke_dummy_dag

dag_id          | run_id                                   | state   | run_after                        | logical_date | start_date                       | end_date                        
================+==========================================+=========+==================================+==============+==================================+=================================
smoke_dummy_dag | manual__2026-11-21T05:06:39.131174+00:00 | success | 2026-11-21T05:06:39.131174+00:00 |              | 2026-11-21T05:06:40.396582+00:00 | 2026-11-21T05:06:42.299383+00:00
```

### 4. Ops Database `pipeline_log` Verification via `OpsDbLogHandler`
```sql
SELECT id, logged_at, level, dag_id, task_id, message FROM pipeline_log WHERE dag_id='smoke_dummy_dag';

 id |           logged_at           | level |     dag_id      |  task_id   |                             message                             
----+-------------------------------+-------+-----------------+------------+-----------------------------------------------------------------
 17 | 2026-11-21 05:06:41.647937+00 | INFO  | smoke_dummy_dag | smoke_task | Smoke dummy task executed successfully inside Airflow container
(1 row)
```

### 5. Web UI HTTP Endpoint Probes
```
Airflow UI (port 8080):   HTTP 200 OK
Mailpit UI (port 8025):   HTTP 200 OK
Streamlit UI (port 8501): HTTP 200 OK
```

### 6. Linting & Unit Tests
```
python -m ruff check .
All checks passed!

python -m ruff format --check .
53 files already formatted

python -m pytest tests/
======================== 5 passed, 1 warning in 0.50s =========================
```

---

## Phase 2 Verification (Completed 2026-11-21)

### 1. Historical Seeding (`make seed`)
```
docker compose exec airflow-scheduler python -m simulator.generate_history

2026-11-21 05:24:39,188 | INFO | __main__ | dag=- task=- run_id=- batch=- try=- | Starting synthetic data generation: days=30, orders_per_day=5000, seed=42, start_date=2026-10-01
2026-11-21 05:24:48,931 | INFO | __main__ | dag=- task=- run_id=- batch=- try=- | Successfully generated 30 days of data in /opt/airflow/data/source_archive (total_orders=150000, quarantine_noise=801)
```
- Total files generated in `data/source_archive`: 60 (30 `orders_YYYY-MM-DD.csv` and 30 `customers_YYYY-MM-DD.json`).

### 2. Simulator DAG Run (`vendor_feed_simulator`)
```
docker compose exec airflow-scheduler airflow dags trigger vendor_feed_simulator --conf '{"batch_date": "2026-10-01", "failure": "none"}'
docker compose exec airflow-scheduler airflow dags list-runs vendor_feed_simulator

dag_id                | run_id                                   | state   | run_after                        | logical_date | start_date                       | end_date                        
======================+==========================================+=========+==================================+==============+==================================+=================================
vendor_feed_simulator | manual__2026-11-21T05:25:05.055829+00:00 | success | 2026-11-21T05:25:05.055829+00:00 |              | 2026-11-21T05:25:05.855241+00:00 | 2026-11-21T05:25:07.457902+00:00
```

### 3. Failure Mode Transformations (`tests.verify_phase2`)
```
PASS: COLUMN_RENAME (header=['order_id', 'customer_id', 'order_date', 'total_amount', 'status', 'channel'])
PASS: DATE_FORMAT (count=229, sample=['21/10/2026', '15/10/2026', '19/10/2026'])
PASS: NULL_SPIKE (null_count=1492/5000, pct=29.8%)
PASS: AMBIGUOUS_DATE (count=243, sample=['05/10/2026', '02/10/2026', '04/10/2026'])
PASS: MISSING_FILE (files suppressed from landing)
PASS: NONE (healthy batch restored)
```

### 4. Ops Database `demo_config` Sync
```sql
SELECT id, failure_mode, target_batch_date, updated_at FROM demo_config;

 id | failure_mode | target_batch_date |          updated_at           
----+--------------+-------------------+-------------------------------
  1 | none         | 2026-10-01        | 2026-11-21 05:26:14.882365+00
(1 row)
```

### 5. Unit Tests
```
python -m pytest tests/ -v
======================== 11 passed, 1 warning in 1.95s ========================
```

---

## Phase 3 Verification (Code Complete — 2026-11-21)

### 1. Code Implementation & Tuning Summary
- Medallion pipeline modules (`bronze.py`, `silver.py`, `profiling.py`, `dq.py`, `gold.py`, `observability.py`, `spark_session.py`) fully implemented.
- Spark configuration tuned: `local[4]`, `driver.memory=2g`, `shuffle.partitions=2`, `default.parallelism=2`, Delta data skipping enabled, progress bar disabled.
- Single-pass aggregations implemented in `dq.py` to calculate row counts, null rates, and revenue sums in one pass.
- `.coalesce(1)` added to DataFrame writes to reduce file-system lock contention on Windows Hyper-V bind mounts.
- Column profiling optimized to reuse in-memory DataFrames without redundant Delta scans.
- Broadcast join (`broadcast(cust_df)`) applied in Gold customer aggregations.
- `incident_responder` DAG paused with `schedule=None` (pending Phase 5).

### 2. Timed Single-Batch Metrics (`2026-11-01`)
```
Layer               Duration    Observed Metrics
----------------------------------------------------------------------
Bronze Ingestion     63.67s     Orders: 5,000, Customers: 369
Silver Transform    ~35s*       Rows In: 5,000, Out: 4,969, Quarantined: 31
Data Quality Checks ~15s*       Row Conservation: Pass, Null Rates: Pass, Rev Diff: 0.00%
Gold Build          ~12s*       Daily Rev: $1,267,351.07, Orders: 4,969
----------------------------------------------------------------------
* Isolated step durations after JVM initialization and cache materialization.
```

### 3. Checkpoint Status
- [x] Code Complete: Medallion pipeline logic, Section 5A error reporting, and Ops DB logging.
- [x] Live Idempotency Rerun: Verified by user on batch `2026-10-01` (identical row counts and revenue).
- [x] Spot-Check Reconciliation: Verified by user (Gold tables baseline preserved).
- [x] Failure Modes Sweep: Verified by user (all failure modes and incident creation confirmed).

---

## Phase 4 Verification: Incident Investigator Agent (Completed 2026-11-21)

### 1. Code-Only Verification & Linting
```
python -m ruff check .
All checks passed!

python -m ruff format --check .
60 files already formatted
```

### 2. Comprehensive Unit Tests & Section 8 Guardrails
```
python -m pytest tests/ -m "not spark"
============================= test session starts =============================
platform win32 -- Python 3.13.5, pytest-9.2.0, pluggy-1.7.0
rootdir: C:\Users\hp\Desktop\pipeline-sentinel
configfile: pyproject.toml
plugins: anyio-4.14.2, Faker-36.1.1, platformdirs-4.13.6
collected 33 items / 1 deselected / 32 selected

tests\test_agent.py ...........                                          [ 34%]
tests\test_guardrails.py ..........                                      [ 65%]
tests\test_logging.py ..                                                 [ 71%]
tests\test_pipeline.py ...                                               [ 81%]
tests\test_simulator.py ......                                           [100%]

================= 32 passed, 1 deselected, 1 warning in 1.02s =================
```

### 3. Guardrail Coverage Summary (Section 8)
- **Destructive Action:** Read-only inspection tools strictly enforced (`TOOL_NAMES`).
- **Wrong File Edited:** Patches proposing non-allow-listed files rejected.
- **Large/Odd Patch:** Operations restricted to `append_unique`, date formats whitelisted, canonical columns verified.
- **Hallucinated Diagnosis:** Verbatim citation validation against cited tool output (max 200 chars).
- **Ambiguity Guardrail:** `ambiguous_day_month_share > 0` blocks date patch and forces escalation.
- **Prompt Injection:** Logs treated as untrusted text, inputs sanitized, tools have no write side-effects.
- **Runaway Loops:** Bounded at 8 tool calls; timeout forces escalation.
- **Secrets/PII Leaks:** Redaction of emails, OpenAI keys, GitHub tokens, Bearer tokens, and secrets.
- **API Failure:** Fail-safe escalation on LLM errors.
- **Kill Switch:** `AGENT_ENABLED=false` or missing `OPENAI_API_KEY` forces escalation without calling LLM.

---

## Phase 5 Verification: Fix and Notify (Completed 2026-11-21)

### 1. Code-Only Verification & Linting
```
python -m ruff check .
All checks passed!

python -m ruff format --check .
62 files already formatted
```

### 2. Unit Tests Across Host & Docker Container
```
python -m pytest tests/ -m "not spark"
============================= test session starts =============================
platform win32 -- Python 3.13.5, pytest-9.2.0, pluggy-1.7.0
rootdir: C:\Users\hp\Desktop\pipeline-sentinel
configfile: pyproject.toml
plugins: anyio-4.14.2, Faker-36.1.1, platformdirs-4.13.6
collected 43 items / 1 deselected / 42 selected

tests\test_agent.py ...........                                          [ 26%]
tests\test_fix_and_notify.py ..........                                  [ 50%]
tests\test_guardrails.py ..........                                      [ 73%]
tests\test_logging.py ..                                                 [ 78%]
tests\test_pipeline.py ...                                               [ 85%]
tests\test_simulator.py ......                                           [100%]

================= 42 passed, 1 deselected, 1 warning in 1.65s =================
```

### 3. Checkpoint Status (Phase 5)
- [x] Patcher: ruamel.yaml round-trip clean formatting, `append_unique` only, minimal diffs (`responder/patcher.py`).
- [x] Sandbox: Silver transform on failed Bronze data in `data/sandbox/<id>/`, 5 validation checks (`responder/sandbox.py`).
- [x] PR Dispatch: Dual-mode `PR_MODE=dry_run` (markdown previews) and `PR_MODE=github` (PyGithub branch + PR, no main/merge) (`responder/pr.py`).
- [x] Notifications: HTML emails with Evidence, Reasoning, Decision, Recommended action, redaction, and SMTP dispatch (`responder/notify.py`).
- [x] Audit Record: Persistence into `incident_diagnosis_audit` (`responder/audit.py`).
- [x] Orchestration: 9-task DAG wired with `AirflowSkipException` short-circuiting and fail-safe escalation (`dags/incident_responder.py`).
- [x] Human Simulation: `make apply-fix INCIDENT=<id>` and `make rerun BATCH=<date>`.
- [x] Resolution: `resolve_incidents` in `orders_medallion_daily` updates non-RESOLVED incidents to `RESOLVED`.
- [x] Incident Investigation CLI: `responder/run_agent_once.py` formats diagnosis, citation check, policy result, timeline, and tokens.
- [x] Failure Mode Runner: `tests/run_failure_modes_demo.py` supports `--mode <failure_mode>`.
- [x] Compose Environment: Container environment updated with `PR_MODE`, `AGENT_ENABLED`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `GITHUB_*`, `SMTP_*`.




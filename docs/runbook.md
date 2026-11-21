# PipelineSentinel Operations & Incident Runbook

This runbook documents operational procedures, demo workflows, on-call incident response steps, and emergency management for PipelineSentinel.

---

## 1. System Operations & Stack Management

### Starting Services
```powershell
# Using Make:
make up

# PowerShell Equivalent:
docker compose up -d
```
Verify that all 7 services are running and healthy:
```powershell
docker compose ps
```
Services:
1. `airflow-apiserver`: Port 8080 (REST API and Web UI)
2. `airflow-scheduler`: LocalExecutor task scheduler
3. `airflow-dag-processor`: DAG file parser and compiler
4. `postgres`: Airflow metadata database (Port 5432)
5. `ops-postgres`: Observability and incident audit database (Port 5434 on host)
6. `mailpit`: SMTP mock sink and web inbox (Port 8025)
7. `streamlit`: 5-page operational monitor and business dashboard (Port 8501)

### Stopping Services
```powershell
# Stop services
docker compose down

# Stop and wipe data volumes (Full Reset)
docker compose down -v
```

---

## 2. Initializing & Seeding Historical Data

Generate 30 days of seeded synthetic historical data (150,000 orders and customer JSON records):
```powershell
# Using Make:
make seed

# PowerShell Equivalent:
docker compose exec airflow-scheduler python -m simulator.generate_history
```
The generator seeds `data/source_archive/` with 30 daily order CSVs and 30 customer JSON files using deterministic seed 42.

---

## 3. Operational Monitoring & Access Points

| Service | Access URL | Credentials / Details |
| :--- | :--- | :--- |
| **Streamlit Dashboard** | [http://localhost:8501](http://localhost:8501) | Live Run Monitor, Revenue KPIs, Pipeline Health, Incident Audits |
| **Airflow Web UI** | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
| **Mailpit Web UI** | [http://localhost:8025](http://localhost:8025) | Local inbox for fix PRs and escalation alerts |
| **Ops PostgreSQL** | `localhost:5434` | DB: `ops`, User: `ops_user`, Pass: `ops_password` |

---

## 4. Failure Mode Demo Procedures

PipelineSentinel supports 8 deterministic operational scenarios:

### Scenario 1: Baseline Healthy Run (`none`)
1. Publish healthy data and trigger pipeline:
   ```powershell
   docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=none --batch-date=2026-10-01
   docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-01"}'
   ```
2. **Verify in Streamlit (Port 8501):**
   - Open **Live Run Monitor**: All 7 stages turn green.
   - Open **Revenue Dashboard**: Gold daily revenue reflects batch totals.

---

### Scenario 2: Transient Network Glitch (`transient_error`)
1. Configure transient glitch on ingestion:
   ```powershell
   docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=none --batch-date=2026-10-02
   docker compose exec airflow-scheduler python -m simulator.publish_batch --record-config=transient_error --batch-date=2026-10-02
   docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-02"}'
   ```
2. **Verify:**
   - In Airflow UI, `bronze_ingest` fails on Try 1 with `RetryablePipelineError`.
   - After a 10s backoff delay, Try 2 automatically succeeds.
   - No incident is created in Ops DB.
   - Streamlit **Pipeline Health** increments the "Resolved by Retry" counter.

---

### Scenario 3: Date Format Drift & Autofix (`date_format`)
1. Inject DD/MM/YYYY date format drift:
   ```powershell
   docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=date_format --batch-date=2026-10-03
   docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-03"}'
   ```
2. **Verify Failure & Autonomous Diagnosis:**
   - `silver_transform` fails immediately with `FormatDriftError`.
   - Failure callback logs an incident in Ops DB.
   - Airflow `incident_responder` claims the incident, investigates logs and column profiles, and proposes `append_unique` value `%d/%m/%Y`.
   - Sandbox validates the patch in `data/sandbox/<incident_id>/`.
   - Markdown PR preview is generated in `data/pr_previews/<incident_id>.md`.
   - HTML notification email appears in Mailpit (`http://localhost:8025`).
3. **Simulate Human Review & Apply Fix:**
   ```powershell
   # Apply patch to production config/silver_rules.yml
   python -m responder.patcher --incident-id <INCIDENT_ID>

   # Rerun the failed batch
   docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-03"}'
   ```
4. **Verify Resolution:**
   - On rerun, `silver_transform` parses all dates cleanly.
   - `resolve_incidents` marks the incident `RESOLVED`.
   - Gold tables are updated.

---

### Scenario 4: Ambiguous Dates (`ambiguous_date`)
1. Inject ambiguous day/month dates:
   ```powershell
   docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=ambiguous_date --batch-date=2026-10-04
   docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-04"}'
   ```
2. **Verify:**
   - `silver_transform` fails.
   - The agent detects `ambiguous_day_month_share > 0`.
   - The ambiguity guardrail strictly blocks patch proposal.
   - Mailpit receives an `[ESCALATION]` email requiring human decision.

---

### Scenario 5: Data Quality Null Spike (`null_spike`)
1. Inject null spike:
   ```powershell
   docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=null_spike --batch-date=2026-10-05
   docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{"batch_date": "2026-10-05"}'
   ```
2. **Verify:**
   - `data_quality_checks` detects null rate > 25%, failing the task.
   - Gold tasks are blocked, protecting downstream tables.
   - Agent diagnoses `data_quality` root cause and dispatches an escalation email.

---

## 5. Standalone CLI Investigation

To run the autonomous diagnosis agent directly from the command line against any recorded incident:
```powershell
python -m responder.run_agent_once <INCIDENT_ID>
```
Displays:
1. Diagnosis summary, root cause category, and confidence score.
2. Evidence citations with verbatim citation validation check (`PASSED` / `FAILED`).
3. Proposed rules patch and policy compliance result.
4. Step-by-step agent tool-call timeline.
5. Model used and token economics.

---

## 6. Emergency Procedures

### 1. Activating the AI Kill Switch
If the AI agent requires immediate deactivation:
1. Set `AGENT_ENABLED=false` in `.env`.
2. All incidents will bypass LLM diagnosis and route directly to human escalation emails.

### 2. Clearing Stuck Airflow Runs
```powershell
docker compose exec airflow-scheduler python -c "
from airflow.models import DagRun
from airflow.utils.session import create_session

with create_session() as session:
    stuck = session.query(DagRun).filter(DagRun.state == 'running').all()
    for r in stuck:
        r.set_state('failed')
    session.commit()
    print(f'Cleared {len(stuck)} stuck runs.')
"
```

### 3. Resetting Production Rules File
To restore the baseline rules file to original configuration:
```powershell
git checkout config/silver_rules.yml
```

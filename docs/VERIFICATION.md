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

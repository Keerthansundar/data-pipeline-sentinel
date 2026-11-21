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

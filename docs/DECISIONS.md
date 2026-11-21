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

# Progress & Handoff Tracking: PipelineSentinel

## Current Phase: Phase 3 (Pipeline) — CODE COMPLETE (UNVERIFIED CHECKPOINT ITEMS)

### Status Summary
- **Phase 0 (Foundation):** 100% COMPLETE & VERIFIED (commit `48502fb`).
- **Phase 1 (Infrastructure):** 100% COMPLETE & VERIFIED (commit `26f3992`).
  - All 7 Docker services healthy (Airflow 3.4.0 apiserver, scheduler, dag-processor; postgres, ops-postgres, mailpit, streamlit).
- **Phase 2 (Data and Simulator):** 100% COMPLETE & VERIFIED (commit `e232aa8`).
  - 30 days of seeded synthetic historical data in `data/source_archive/` (150,000 orders + customer JSON lines).
  - Simulator DAG `vendor_feed_simulator` verified with failure modes.
- **Phase 3 (Pipeline):** CODE COMPLETE.
  - Idempotency, spot-check reconciliation, and failure modes still unverified.
  - Medallion pipeline modules fully implemented and optimized:
    - `pipeline/spark_session.py`: Shared singleton SparkSession configured with `local[4]`, driver memory 2g, shuffle partitions 2, parallelism 2, Delta data skipping, and console progress disabled.
    - `pipeline/bronze.py`: Ingests CSV & JSON with single-file coalescing, partitioned by `_batch_date`, records schema snapshot and pipeline run.
    - `pipeline/silver.py`: Rules-driven transformation (`config/silver_rules.yml`), vectorized date parsing and quarantine splitting, PII redaction on customer emails, fast column profiling.
    - `pipeline/profiling.py`: Single-pass column profiling, ambiguous date calculation (`ambiguous_day_month_share`), records `column_profile`. Reuses in-memory DataFrames.
    - `pipeline/dq.py`: Native single-pass Spark aggregations for row conservation, null limits, and revenue reconciliation.
    - `pipeline/gold.py`: Cached orders aggregation for `daily_revenue`, `revenue_by_channel`, and broadcast-joined `customer_summary`.
    - `dags/orders_medallion_daily.py`: Daily Airflow DAG chaining `wait_for_vendor_files -> bronze_ingest -> silver_transform -> data_quality_checks -> gold_build -> freshness_check -> resolve_incidents`.
    - `dags/incident_responder.py`: Paused with `schedule=None` (MUST be restored to `schedule="* * * * *"` and unpaused in Phase 5).
  - Decisions logged in `docs/DECISIONS.md` (DECISION-007 and DECISION-008).

### Checkpoint Milestones
- [x] Phase 0 Checkpoint: Foundation & Linting
- [x] Phase 1 Checkpoint: Infrastructure, Spark + Delta Smoke Test, Airflow Services, Ops DB Logging
- [x] Phase 2 Checkpoint: Data Generation & Simulator (`vendor_feed_simulator`)
- [ ] Phase 3 Checkpoint: Pipeline (`orders_medallion_daily`: Bronze, Silver, DQ, Gold) — *Code complete; live runs unverified*
- [ ] Phase 4 Checkpoint: Agent (Text-only diagnosis loop & guardrail validation)
- [ ] Phase 5 Checkpoint: Fix and Notify (`incident_responder` DAG, PR preview, Email)
- [ ] Phase 6 Checkpoint: Streamlit Dashboard (Live Run Monitor, Revenue, Health, Audit, Demo Control)
- [ ] Phase 7 Checkpoint: Documentation & Full Demo Script Acceptance

### Known Issues & Performance Findings
- **Windows Hyper-V Docker Bind Mount Latency:** Docker bind mounts on Windows NTFS experience significant disk I/O lock latency when Spark writes multiple partition files and CRC files. Coalescing writes to 1 partition (`.coalesce(1)`) and setting `spark.sql.shuffle.partitions=2` significantly reduces file-handle thrashing.
- **Java 21 Arrow Compatibility:** OpenJDK 21 strictly encapsulates internal JDK modules (`java.nio.DirectByteBuffer`), preventing Apache Arrow from using unsafe off-heap buffers without explicit `--add-opens` JVM flags. Standard PySpark serialization is used instead.
- **Incident Responder DAG:** Set to `schedule=None` and paused in Airflow DB to prevent background runs during Phase 3 and Phase 4.

### Exact Resume Commands
```bash
# 1. Run idempotency check
docker compose exec airflow-scheduler python -m tests.verify_idempotency

# 2. Run failure modes demo
docker compose exec airflow-scheduler python -m tests.run_failure_modes_demo

# 3. Run unit tests
python -m pytest tests/ -m "not spark"

# 4. Check lint and format
python -m ruff check .
python -m ruff format --check .
```

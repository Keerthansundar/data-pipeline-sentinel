# Logging Guidelines (Section 5A Standard)

1. **Shared Logger Only:** Always use `from pipeline.logging_config import get_logger`. Never use bare `print()`.
2. **Context Enrichment:** Loggers must automatically record:
   - `dag_id`
   - `task_id`
   - `run_id`
   - `batch_date`
   - `try_number`
3. **Console Format:**
   `%(asctime)s | %(levelname)s | %(name)s | dag=... task=... batch=... try=... | %(message)s`
4. **OpsDbLogHandler:**
   - Persists INFO and above to the `pipeline_log` table in the Ops DB.
   - Non-blocking and fail-safe: catches all exceptions, falls back to console, truncates message to 2,000 characters, runs `redact.py` prior to persistence.
   - Must never cause a pipeline task or agent run to crash.
5. **Log Levels:**
   - `DEBUG`: Fine-grained operational detail (console only, not persisted to DB).
   - `INFO`: Milestones, input/output row counts, applied transformations, decisions.
   - `WARNING`: Handled anomalies (quarantined rows, close thresholds).
   - `ERROR`: Task failures (use `logger.exception` to capture full stack trace).
6. **Task Lifecycle Logging:**
   - Start: task, batch date, try number, input paths, rules hash, thresholds.
   - Inputs: files found, sizes, row counts, columns seen.
   - Transforms: rows in/out, aliases applied, date formats tested and parse success rates, quarantined rows by reason.
   - Decisions: explicit numeric thresholds (e.g., `unparseable date ratio 5.2% exceeds max 1.0%`).
   - Schema: expected vs actual columns, missing columns, available columns.
   - DQ checks: check name, observed value, threshold, status (PASS/FAIL).
   - External calls: duration and counts (never log tokens or raw prompts).
   - End: status, duration, total output rows.
7. **Redaction & PII Safety:**
   - Never log per-row records, secrets, passwords, or PII (customer names, emails).
   - Sample bad values are restricted to non-PII columns (`order_date`, `amount`, `status`, `channel`), maximum 5 samples per log message.

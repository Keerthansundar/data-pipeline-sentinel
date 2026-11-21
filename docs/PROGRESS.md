# Progress & Handoff Tracking: PipelineSentinel

## Current Phase: Phase 5 (Fix Generation & Notifications) — COMPLETE (CODE-ONLY MODE)

### Status Summary
- **Phase 0 (Foundation):** 100% COMPLETE & VERIFIED (commit `48502fb`).
- **Phase 1 (Infrastructure):** 100% COMPLETE & VERIFIED (commit `26f3992`).
  - All 7 Docker services healthy (Airflow 3.4.0 apiserver, scheduler, dag-processor; postgres, ops-postgres, mailpit, streamlit).
- **Phase 2 (Data and Simulator):** 100% COMPLETE & VERIFIED (commit `e232aa8`).
  - 30 days of seeded synthetic historical data in `data/source_archive/` (150,000 orders + customer JSON lines).
  - Simulator DAG `vendor_feed_simulator` verified with all failure modes.
- **Phase 3 (Pipeline):** 100% COMPLETE & VERIFIED (commit `3c021fd`).
  - Medallion pipeline modules (`bronze.py`, `silver.py`, `profiling.py`, `dq.py`, `gold.py`, `observability.py`) complete and performance-tuned.
  - Idempotency and failure mode demo verified by user.
- **Phase 4 (Incident Investigator Agent):** 100% COMPLETE & VERIFIED (commit `eb03b43`).
  - Read-only tools, diagnosis schema, Pydantic validation, policy enforcement, and 12 Section 8 guardrails verified (32 unit tests pass).
- **Phase 5 (Fix Generation, Sandbox, PR, & Notifications):** 100% COMPLETE (CODE-ONLY MODE).
  - **Patcher (`responder/patcher.py`):** Uses `ruamel.yaml` to apply `append_unique` patches to copies of `config/silver_rules.yml`, preserving comments and minimal unified diffs. Includes `apply_fix_for_incident` CLI handler for `make apply-fix INCIDENT=<id>`.
  - **Isolated Sandbox (`responder/sandbox.py`):** Re-runs Silver transformation on failed batch Bronze data using the patched rules copy, writing outputs strictly to `data/sandbox/<incident_id>/`. Enforces 5 deterministic checks: no exception, unparseable ratio within limit, row count conservation (`rows_out + rows_quarantined == rows_in`), revenue reconciliation (`abs(diff) <= 0.001`), and schema contract adherence.
  - **PR Generator (`responder/pr.py`):** Dual-mode PR dispatch. When `PR_MODE=dry_run` (default), writes markdown audit preview to `data/pr_previews/<incident_id>.md` and patch diff to `<incident_id>.diff`. When `PR_MODE=github`, creates branch `autofix/<incident_id>-<slug>`, commits updated rules, and opens PR via PyGithub. Never merges and never touches `main`.
  - **Notifications (`responder/notify.py`):** Structured HTML email dispatch to Mailpit SMTP sink with sections Evidence, Reasoning, Decision, Recommended action, impact, and PR link/preview path. Redacts all secrets and PII via `responder/redact.py`.
  - **Audit Persistence (`responder/audit.py`):** Persists full diagnosis, validation results, policy results, sandbox metrics, final action, PR URL, email status, model, and token metrics into `incident_diagnosis_audit`.
  - **DAG Orchestration (`dags/incident_responder.py`):** Wires all 9 tasks (`claim_incident >> run_diagnosis_agent >> validate_diagnosis >> enforce_patch_policy >> run_sandbox_verification >> route_outcome >> open_fix_pull_request >> send_incident_email >> record_audit`). Configured with `schedule="* * * * *"`, `max_active_runs=1`, `catchup=False`, and `is_paused_upon_creation=True`. Short-circuits with `AirflowSkipException` when no `OPEN` incidents exist. Fail-safe fallback routes any error/exception to escalation email.
  - **Incident Resolution (`dags/orders_medallion_daily.py`):** Updated `_run_resolve_incidents_task` to mark all non-RESOLVED incidents for the batch as `RESOLVED` after a successful rerun.
  - **CLI Investigation Tool (`responder/run_agent_once.py`):** CLI script printing full investigation report: diagnosis, citation validation check, patch & policy result, tool call timeline, and token metrics with redaction.
  - **Failure Modes Demo Runner (`tests/run_failure_modes_demo.py`):** Enhanced with `--mode <failure_mode>` CLI argument to run single failure modes and restore clean state.
  - **Container Configuration (`docker-compose.yml`):** Added `PR_MODE`, `AGENT_ENABLED`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `GITHUB_*`, and `SMTP_*` to `x-airflow-common.environment`.
  - **Unit Test Suite (`tests/test_fix_and_notify.py`):** Added 10 mocked unit tests covering patcher round-trip, column aliases, invalid operations, sandbox passing, sandbox failing (unparseable ratio and revenue mismatch), PR dry run preview output, email HTML rendering, SMTP dispatch, fail-safe routing, and GitHub PR mocking (42 unit tests passing).

### Checkpoint Milestones
- [x] Phase 0 Checkpoint: Foundation & Linting
- [x] Phase 1 Checkpoint: Infrastructure, Spark + Delta Smoke Test, Airflow Services, Ops DB Logging
- [x] Phase 2 Checkpoint: Data Generation & Simulator (`vendor_feed_simulator`)
- [x] Phase 3 Checkpoint: Pipeline (`orders_medallion_daily`: Bronze, Silver, DQ, Gold)
- [x] Phase 4 Checkpoint: Agent (Text-only diagnosis loop & guardrail validation)
- [x] Phase 5 Checkpoint: Fix and Notify (Code complete & unit test verified)
- [ ] Phase 6 Checkpoint: Streamlit Dashboard (Live Run Monitor, Revenue, Health, Audit, Demo Control)
- [ ] Phase 7 Checkpoint: Documentation & Full Demo Script Acceptance

---

### Verification Debt (Integration / Live Checks Pending)
The following live execution tests were intentionally deferred per quota rules and CODE-ONLY mode constraints, and should be verified during live integration testing:
1. **Live Airflow `incident_responder` DAG Execution:** Triggering `incident_responder` in Airflow while unpaused with a real OPEN incident in Postgres and observing task transitions across the 9 stages.
2. **Live Mailpit Email Delivery:** Verifying incoming HTML notification emails rendered in the Mailpit web UI (`http://localhost:8025`).
3. **Live Dry-Run Preview Generation:** Confirming actual generation of `.md` and `.diff` files in `data/pr_previews/` from an end-to-end Airflow execution.
4. **Live Human-in-the-Loop Simulation:** Running `make apply-fix INCIDENT=<id>` and `make rerun BATCH=<date>` to confirm `orders_medallion_daily` updates Gold and transitions the incident status to `RESOLVED` in the database.
5. **Live GitHub PR Mode:** Testing PR creation with a live GitHub repository and personal access token (`PR_MODE=github`).

---

### Exact Resume Commands (PowerShell)

```powershell
# 1. Run all unit tests (42 passing unit tests across agent, guardrails, pipeline, fix & notify)
python -m pytest tests/ -m "not spark"

# 2. Check formatting and linting
python -m ruff check .
python -m ruff format --check .

# 3. Run agent CLI on a target incident (no live DAG needed)
python -m responder.run_agent_once <incident_id>

# 4. Apply fix for an incident (PowerShell equivalent to 'make apply-fix INCIDENT=<id>')
python -m responder.patcher --incident-id <incident_id>

# 5. Rerun pipeline for a batch (PowerShell equivalent to 'make rerun BATCH=<date>')
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{\"batch_date\": \"<date>\"}'

# 6. Run single failure mode demo (PowerShell equivalent)
docker compose exec airflow-scheduler python -m tests.run_failure_modes_demo --mode date_format --batch-date 2026-10-01
```

### Next Action
Begin **Phase 6: Streamlit Dashboard** per `docs/PROJECT_BRIEF.md` Section 9.
Build the multi-page dashboard application (`dashboard/app.py`, `dashboard/lib/`, `dashboard/pages/`) covering:
- Page 1: Live Run Monitor (Task Instances table, DAG run selector, colored pipeline graph, task error excerpt, logs tab)
- Page 2: Revenue Dashboard (KPIs, daily and channel breakdowns, Gold freshness)
- Page 3: Pipeline Health (run history, DQ results, quarantine counts, resolution rate)
- Page 4: Incident Audit (incident filters, diagnosis details, evidence citations, sandbox results, PR preview, tool-call timeline, token costs)
- Page 5: Demo Control (failure mode injector and batch selector)

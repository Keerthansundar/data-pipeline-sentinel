# Progress & Handoff Tracking: PipelineSentinel

## Current Phase: Phase 4 (Incident Responder Agent) — COMPLETE & VERIFIED

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
- **Phase 4 (Incident Investigator Agent):** 100% COMPLETE & VERIFIED (CODE-ONLY MODE).
  - Implemented `responder/schemas.py`: Exact `TOOL_NAMES`, Pydantic diagnosis schemas, and OpenAI function calling definitions.
  - Implemented `responder/redact.py`: Sanitizes emails, OpenAI API keys, GitHub tokens, Bearer strings, and credentials.
  - Implemented `responder/repository.py`: Postgres Ops repository for `incidents`, `pipeline_log`, `column_profile`, `schema_snapshot`, `dq_results`, `incident_tool_calls`, and `incident_diagnosis_audit`.
  - Implemented `responder/tools.py`: The 7 read-only inspection tools (`fetch_task_error_log`, `fetch_run_history`, `inspect_column_profile`, `diff_schema_vs_last_good_batch`, `fetch_data_quality_results`, `trace_downstream_impact`, `read_silver_rules`) returning "no data available" notices rather than raising.
  - Implemented `responder/validate.py`: Schema validation and strict evidence-citation verification (verbatim check against cited tool output, max 200 chars).
  - Implemented `responder/policy.py`: Allow-listed file enforcement (`config/silver_rules.yml`), `append_unique` grammar, whitelisted date formats, identifier validation, and ambiguity ratio blocker.
  - Implemented `responder/llm_client.py`: Thin OpenAI wrapper reading `OPENAI_MODEL` from environment.
  - Implemented `responder/agent.py`: Tool-calling loop bounded by 8 calls, prompt injection resistance, token tracking, kill switch (`AGENT_ENABLED=false`), and audit logging.
  - Unit tests in `tests/test_agent.py` and `tests/test_guardrails.py` covering all 12 guardrails in Section 8 (32 unit tests pass).
  - `incident_responder` DAG paused with `schedule=None` (pending Phase 5).

### Checkpoint Milestones
- [x] Phase 0 Checkpoint: Foundation & Linting
- [x] Phase 1 Checkpoint: Infrastructure, Spark + Delta Smoke Test, Airflow Services, Ops DB Logging
- [x] Phase 2 Checkpoint: Data Generation & Simulator (`vendor_feed_simulator`)
- [x] Phase 3 Checkpoint: Pipeline (`orders_medallion_daily`: Bronze, Silver, DQ, Gold)
- [x] Phase 4 Checkpoint: Agent (Text-only diagnosis loop & guardrail validation)
- [ ] Phase 5 Checkpoint: Fix and Notify (`incident_responder` DAG, sandbox verification, PR preview/creation, Email)
- [ ] Phase 6 Checkpoint: Streamlit Dashboard (Live Run Monitor, Revenue, Health, Audit, Demo Control)
- [ ] Phase 7 Checkpoint: Documentation & Full Demo Script Acceptance

### Known Issues & Design Notes
- **Incident Responder DAG Schedule:** Remains set to `schedule=None` and paused in Airflow DB. In Phase 5, restore to `schedule="* * * * *"` and unpause when full automated fix and notification flow is connected.
- **Model Configuration:** Configured to read `OPENAI_MODEL` from environment (e.g. `gpt-4o-mini`). No hardcoded model string.

### Exact Resume Commands
```bash
# 1. Run unit tests (including all Section 8 guardrails & agent loop)
python -m pytest tests/ -m "not spark"

# 2. Check lint and format
python -m ruff check .
python -m ruff format --check .

# 3. Test agent against a live incident inside Airflow container
docker compose exec airflow-scheduler python -c "
import os
from responder.agent import diagnose_incident
from responder.repository import OpsRepository

repo = OpsRepository()
incidents = repo.get_open_incidents()
target = next((i['id'] for i in incidents if 'FormatDriftError' in i.get('error_type', '')), incidents[0]['id'] if incidents else None)
if target:
    print(f'Diagnosing incident: {target}')
    diag = diagnose_incident(target)
    print('Action:', diag.proposed_action)
    print('Category:', diag.root_cause_category)
    print('Confidence:', diag.confidence)
    print('Patch:', diag.patch)
else:
    print('No open incidents found in Ops DB.')
"
```

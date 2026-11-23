# Progress & Handoff Tracking: PipelineSentinel

## Project Status: MVP verified for 5 of 7 injected failure modes

`auth_error` and `missing_file` have not been run yet. Everything marked **Verified** below is backed by real command output, Mailpit emails, or Airflow/Streamlit screenshots captured during live runs. Anything not re-checked live is marked **Not re-verified**.

---

## Verified Results (live runs)

| Failure mode | Batch | Result | Evidence |
|---|---|---|---|
| `date_format` | 2026-10-03 | Healed. Silver failed on try 1 (no retries), agent diagnosed `format_drift`, sandbox passed, one-line patch (`%d/%m/%Y`) applied by a human, rerun all 7 tasks green, incident `RESOLVED` | Audit rows, PR preview, `git diff` (one added line), rerun |
| `date_format` (fully hands-off) | 2026-10-08 | Incident, diagnosis, sandbox-tested patch, PR preview and "PR ready" email with no manual step. Human apply and rerun then closed the incident (`RESOLVED` at the end of the rerun) | Audit row, `.md` and `.diff` previews, Mailpit email |
| `ambiguous_date` | 2026-10-04 | Blocked by code policy (source ambiguity 1.0 > 0.0). `final_action = escalate`, no preview written | Audit rows 12 and 13, `compute_source_ambiguity('2026-10-04') = 1.0` |
| `column_rename` | 2026-10-06, 10-09, 10-10 | Escalated with cited evidence (`schema_drift`, confidence 0.90). The model proposed no patch in 3 runs, even after prompt rule 5b was added | Audit rows (`patch: null`), Mailpit escalation emails |
| `null_spike` | 2026-10-12 | Silver passed, `data_quality_checks` failed on try 1 (no retries), escalated as `data_quality`, no patch. 7 tool calls, no repeats | Live Run Monitor, audit row, Mailpit email |
| `transient_error` | 2026-10-07 | `bronze_ingest` failed on try 1, succeeded on try 2, run `success`, no incident created | Task Instances table, attempt-2 log, 0 incident rows |
| `auth_error` | n/a | **Not run** | n/a |
| `missing_file` | n/a | **Not run** | n/a |

Healthy-batch checks verified earlier: batch 2026-10-02 reconciled (raw = Silver + quarantine = Gold, $1,262,280.45, 4,974 orders); idempotency test on 2026-10-01 passed (identical counts).

### Measured routing time

Time from incident creation to final routing decision, includes the responder DAG's 1-minute polling interval:

| Incident | Seconds | Outcome |
|---|---|---|
| 10-06 column_rename | 29.7 | escalate |
| 10-09 column_rename | 51.2 | escalate |
| 10-12 null_spike | 63.1 | escalate |
| 10-10 column_rename | 66.1 | escalate |
| 10-08 date_format | 73.5 | pr_opened |
| 10-11 null_spike | 80.0 | escalate (agent hit the tool-call limit) |
| 10-05 UnboundLocalError | 83.4 | escalate |

- n = 7, median 66 s, mean 64 s, range 30 to 83 s.
- Only 5 of the 7 are real diagnoses (10-05 was a code bug, 10-11 was an agent timeout).
- Agent token usage, five clean diagnoses (10-06, 10-08, 10-09, 10-10, 10-12): about 9,000 prompt tokens and 500 completion tokens on average. Cost depends on the model price, so compute it from `OPENAI_MODEL`.
- The 10-03 and 10-04 audit rows are leftovers from development reruns and are not valid for timing.

---

## Claims this project supports

- Failure, incident, LLM diagnosis, code-validated evidence, sandbox-tested one-line patch, human-approved rerun, incident `RESOLVED` (verified twice, date format drift only).
- Guardrails: patch policy (one allow-listed edit), source-based ambiguity check, evidence-citation validation, sandbox row and revenue conservation.
- Deterministic errors fail fast (no retries). Transient errors retry and recover.
- Anything the agent cannot safely fix is escalated with cited evidence.

## Claims this project does NOT support

- Autonomous or unattended fixing. A human always applies the patch and reruns.
- Healing schema drift. Column renames escalate by design (see DECISION-015).
- Verification of `auth_error` and `missing_file`.
- DeepEval results (evals are postponed).

---

## Phase Status

- **Phase 0 (Foundation):** Complete (commit `48502fb`).
- **Phase 1 (Infrastructure):** Complete (commit `26f3992`). 7 Docker services, PySpark 3.5.4 and Delta Lake 3.3.0 smoke test verified.
- **Phase 2 (Data and Simulator):** Complete (commit `e232aa8`). Synthetic source data for 2026-10-01 to 2026-10-30 (5,000 orders per day). Other dates fail with `FileNotFoundError`.
- **Phase 3 (Pipeline):** Complete (commit `3c021fd`) and hardened after live runs (profiling fix, base-class fail-fast, see DECISION-016).
- **Phase 4 (Incident Investigator Agent):** Complete (commit `eb03b43`) and live-verified. Prompt rule 8 added after a tool-call loop (see DECISION-017).
- **Phase 5 (Fix generation, sandbox, PR preview, notifications):** Complete (commit `d50ec5f`) and live-verified for `date_format`. Sandbox alias bug fixed but the alias path has not run end to end (see DECISION-015).
- **Phase 6 (Streamlit dashboard):** Built. A light theme is forced via a mounted `.streamlit/config.toml` (DECISION-019) and the container is healthy. **Not re-verified:** visual readability of Live Run Monitor, whether the three Revenue Dashboard charts render, and the red banner shown on a successful run.
- **Phase 7 (Documentation):** README, `docs/architecture.md`, `docs/runbook.md` and `.github/workflows/ci.yml` exist. **Not re-verified:** they must be checked against the verified results above (the old runbook expected column_rename to produce a PR, which does not happen).

---

## Known Issues (accepted for now)

- **Dashboard:** red banner on Live Run Monitor still showed on the successful 10-07 run (may be a stale message from try 1). Revenue Dashboard charts (Revenue by Channel donut, Run Duration by Batch, Latest Batch Row Flow) were blank in an earlier check and have not been re-checked. Date selector was hard to see.
- **Stray batch 2026-11-01** exists in Gold (copy of 2026-10-01). `tests/cleanup_batch.py` exists but must not be run until its dry-run option is checked.
- **Airflow logs:** INFO lines are labelled ERROR (logger writes to stderr). `revenue_reconciliation` check logs twice. About 8 minutes per 5,000-row batch (Spark startup per task).
- **`orders_medallion_daily` daily schedule** fails every night (no files for today's date). Keep it paused when idle. A manual trigger needs the DAG unpaused or the run stays queued.
- **`incident_responder` processes every OPEN incident** (each costs an OpenAI call). Close old ones first: `UPDATE incidents SET status='RESOLVED' WHERE status='OPEN';`
- **Escalation email:** the Recommended Action section is empty when the model leaves `recommended_next_steps` blank (no fallback text yet).
- **Agent loop limit** is checked per model turn, so 9 tool calls can be logged against a limit of 8.
- **Pytest:** tests marked `spark` need pyspark and run inside the container: `docker compose exec airflow-scheduler python -m pytest -q tests/test_pipeline.py -m spark` (about 8 minutes). On the host use `pytest -q tests/ -m "not spark"`. The `spark` marker is not registered, which causes a warning.
- **Dashboard restarts:** Streamlit does not auto-reload on Windows. Run `docker compose restart streamlit`, then Ctrl+F5. A change to compose volumes needs `docker compose up -d streamlit`.
- **Test files:** `INC-TEST-DRYRUN-001` files in `data/pr_previews` come from a test, not a real incident.

---

## Verified Runbook (PowerShell, from the project folder)

Run one pipeline command at a time. Do not restart Docker while a run is active.

### 0. Prepare

```powershell
docker compose ps
docker compose exec airflow-scheduler python -c "import os; print(bool(os.getenv('OPENAI_API_KEY')), os.getenv('OPENAI_MODEL'), os.getenv('PR_MODE'))"
git status
docker compose exec ops-postgres psql -U ops_user -d ops -c "SELECT id, status FROM incidents WHERE status='OPEN';"
```

Expect healthy services, `True`, a model name and `dry_run`. `config/silver_rules.yml` must not be modified. Close any OPEN incidents before the run (see Known Issues).

### 1. Unpause both DAGs

```powershell
docker compose exec airflow-scheduler airflow dags unpause orders_medallion_daily
docker compose exec airflow-scheduler airflow dags unpause incident_responder
```

### 2. Inject a failure and trigger the run

```powershell
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=date_format --batch-date=2026-10-08
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{\"batch_date\": \"2026-10-08\"}'
```

Use a date that has no incident yet. Other modes: `none`, `transient_error`, `missing_file`, `column_rename`, `null_spike`, `ambiguous_date`, `auth_error`.

### 3. Expected behavior by mode

| Mode | Expected (verified unless noted) |
|---|---|
| `date_format` | Silver fails on try 1, diagnosis `format_drift`, sandbox passes, PR preview and "PR ready" email |
| `ambiguous_date` | Silver fails on try 1, policy blocks the patch, escalation email, no preview |
| `column_rename` | Silver fails on try 1 (`MissingColumnError`), model escalates with evidence, no preview |
| `null_spike` | Silver passes, `data_quality_checks` fails on try 1 (`max_null_rate:amount`), escalation as `data_quality` |
| `transient_error` | `bronze_ingest` fails on try 1, succeeds on try 2, no incident |
| `auth_error` | Not yet verified. Expected: fail fast, no retries |
| `missing_file` | Not yet verified. Expected: sensor times out after about 2 minutes |

### 4. Read the result

```powershell
docker compose exec ops-postgres psql -U ops_user -d ops -c "SELECT id, status, task_id, batch_date FROM incidents ORDER BY created_at DESC LIMIT 5;"
docker compose exec ops-postgres psql -U ops_user -d ops -x -c "SELECT incident_id, final_action, diagnosis->>'root_cause_category' AS category, diagnosis->'patch' AS patch, diagnosis->>'escalation_reason' AS reason, sandbox_results FROM incident_diagnosis_audit ORDER BY created_at DESC LIMIT 1;"
dir data\pr_previews
```

Then open Mailpit at http://localhost:8025. Each incident produces one `[AUTOFIX]` or `[ESCALATION]` email.

### 5. Human approval and recovery (date_format only)

```powershell
docker compose exec airflow-scheduler python -m responder.patcher --incident-id <INCIDENT_ID>
git diff config/silver_rules.yml
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{\"batch_date\": \"2026-10-08\"}'
```

The diff must show one added line. The rerun takes about 6 to 8 minutes and all 7 tasks should turn green. Do not republish the file. Then check the incident is `RESOLVED`.

### 6. Reset

```powershell
git restore config/silver_rules.yml
docker compose exec airflow-scheduler airflow dags pause incident_responder
docker compose exec airflow-scheduler airflow dags pause orders_medallion_daily
```

---

## Remaining Work

1. Re-check the Streamlit pages after the theme change (Live Run Monitor, Revenue Dashboard, Incident Audit). Fix blank charts and the stale red banner if they persist.
2. Run `auth_error` and `missing_file`, or keep the claim at 5 of 7.
3. Verify README, `docs/architecture.md` and `docs/runbook.md` against this file.
4. Clean up the stray 2026-11-01 batch (dry-run first).
5. Record a 2 to 3 minute demo, take README screenshots, check `.env` is untracked, push to GitHub.
6. Add DeepEval evals (postponed). Write resume numbers only from measured results.

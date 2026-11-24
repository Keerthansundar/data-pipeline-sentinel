# Progress & Handoff Tracking: PipelineSentinel
 
## Project Status: 2 healed failure classes, 5 of 7 injected failure modes exercised live
 
Healed (a human merges and reruns): **date format drift** and **single-column rename**. Everything else escalates by design. `auth_error` and `missing_file` have not been run on purpose. Everything marked **Verified** is backed by real command output, Mailpit emails, GitHub pages or Airflow/Streamlit screenshots from live runs. Anything not re-checked is marked **Not re-verified**.
 
Last updated: 2026-11-24.
 
---
 
## Verified Results (live runs)
 
| Failure mode | Batch | Result | Evidence |
|---|---|---|---|
| `date_format` | 2026-10-03 | Healed (local preview). Silver failed on try 1 (no retries), agent diagnosed `format_drift`, sandbox passed, one-line patch `%d/%m/%Y` applied by a human, rerun all 7 tasks green, incident `RESOLVED` | Audit rows, PR preview, `git diff` (one added line), rerun |
| `date_format` | 2026-10-08 | Healed (local preview). Incident, diagnosis, sandbox-tested patch, preview and email with no manual step. Human apply and rerun closed the incident | Audit row, `.md` and `.diff` previews, Mailpit email |
| `date_format` (real GitHub PR) | 2026-10-13 | **Healed through GitHub.** Silver failed on try 1, agent diagnosed `format_drift` (0.90), sandbox PASSED (5000 in = 4982 out + 18 quarantined, revenue reconciled), **PR #2** opened (+1 line), email with the PR link, human merged on GitHub, `git pull`, rerun 7/7 green on try 1, incident `RESOLVED` | Audit row 31 (`pr_opened`, `/pull/2`), PR page, Mailpit, Live Run Monitor, incidents query |
| `column_rename` (LLM path) | 2026-10-16 | **Healed, one live run.** Code fallback switched off (`ALIAS_FALLBACK=off`). The LLM proposed `orders.column_aliases.amount = total_amount` itself, policy passed, sandbox PASSED (5000 = 4973 + 27, revenue reconciled), **PR #4** opened (+1 line), human merged, `git pull`. The first rerun failed in `data_quality_checks` (see "Bugs found by the live rename run"). After two DQ fixes the rerun was 7/7 green on try 1 and both 10-16 incidents are `RESOLVED` | Audit rows 41/42, PR #4, `dq_results` rows 360-365, Live Run Monitor, incidents query |
| `column_rename` (code fallback) | 2026-10-15 | PR #3 opened by the **deterministic code fallback** (sandbox PASSED, 5000 = 4975 + 25). **Closed without merging** by the author, because the goal was an LLM-proposed patch. Not a healed run | Audit row, PR #3, email says "derived by a deterministic code fallback" |
| `ambiguous_date` | 2026-10-04 | Blocked by code policy (source ambiguity 1.0 > 0.0). `final_action = escalate`, no preview. Offline re-check with the new prompt: the model *did* propose `%d/%m/%Y`, and policy blocked it | Audit rows 12/13, `run_agent_once` log line |
| `null_spike` | 2026-10-12 | Silver passed, `data_quality_checks` failed on try 1, escalated as `data_quality`, no patch, 7 tool calls | Live Run Monitor, audit row, Mailpit |
| `null_spike` (escalate path, GitHub mode on) | 2026-10-14 | Silver passed, `data_quality_checks` failed on try 1, audit `final_action = escalate`, empty `pr_url_or_preview`, `email_status = SENT`, email shows "Sandbox: not run (no patch proposed)" and no PR line. **Not shown:** a look at the GitHub Pull requests tab for 10-14 | Audit row 33, Mailpit, Live Run Monitor |
| `transient_error` | 2026-10-07 | `bronze_ingest` failed on try 1, succeeded on try 2, run `success`, no incident | Task Instances table, attempt-2 log, 0 incident rows |
| `auth_error` | n/a | **Not run** | n/a |
| `missing_file` | n/a | **Not run** (see stray incident under Known Issues) | n/a |
 
Healthy-batch checks verified earlier: batch 2026-10-02 reconciled (raw = Silver + quarantine = Gold, $1,262,280.45, 4,974 orders); idempotency test on 2026-10-01 passed.
 
### GitHub setup (verified)
 
- Repo `Keerthansundar/data-pipeline-sentinel`, now **private** (it was public for a short time while testing; see Known Issues).
- Container check: `PR_MODE=github`, token present, `GITHUB_REPO=Keerthansundar/data-pipeline-sentinel`, `GITHUB_BASE_BRANCH=main`, PyGithub imports.
- Smoke test (`tests/manual_github_smoke.py`) opened PR #1 with exactly one added line; closed without merging, branch deleted. Its PR body showed a FAILED sandbox with N/A numbers because the script passed dummy sandbox data. Real runs show real numbers (PRs #2 and #4).
- Pre-push secret scan: no commit contains the real OpenAI key or GitHub token (checked with `git log -S` on the real values, lengths 164 and 93). The pattern hits were a README placeholder and redaction-test fixtures (`sk-your...`, `sk-1234...`, `sk-proj-1234567890abcdef...`).
### Offline agent checks (`python -m responder.run_agent_once`, new prompt, gpt-4o-mini)
 
These skip the DAG and the code fallback. They are replays of stored incidents, not fresh batches.
 
| Incident | Expected | Result |
|---|---|---|
| 10-15 `column_rename` | patch | `rules_patch`, `column_aliases.amount = total_amount`, citations PASSED, policy True |
| 10-09 `column_rename` (escalated 3 times under the old prompt) | patch | same patch, 8 steps (exactly the loop limit) |
| 10-13 `date_format` | patch | `rules_patch`, `%d/%m/%Y` |
| 10-14 `null_spike` | escalate | `escalate`, `data_quality` |
| 10-04 `ambiguous_date` | no patch applied | model proposed a patch, **policy blocked it** (source ambiguity 1.0), final `escalate` |
 
n = 2 for renames. Do not quote a rate.
 
### Bugs found by the live rename run (fixed)
 
The first 10-16 rerun failed in `data_quality_checks` with `max_null_rate:amount` observed 1.0. The Silver sandbox had passed, because it only runs Silver.
 
1. `pipeline/dq.py` read only the Bronze column named `amount`. In the renamed batch Bronze has `amount` (all NULL) and `total_amount` (real values), so the null rate was 1.0 and revenue summed to 0.00 (`raw=0.00`), which made the revenue check blind. Fix: `pipeline/column_resolve.py::candidate_columns` plus `coalesce` over the canonical and alias columns.
2. The first fix still failed because `dq.py` took `column_aliases` from `config/dq_rules.yml`, which has none. Aliases live in `config/silver_rules.yml`. Fix: DQ now loads aliases from `silver_rules.yml`.
3. While in `dq.py`: the revenue reconciliation block was duplicated (two identical rows per run). Removed.
After the fixes: `max_null_rate:amount = 0.0000`, `revenue_reconciliation raw=1270856.67 silver=1266900.04 q=3956.63 rel_diff=0.000000`, 7/7 green on try 1.
 
### Measured routing time (old prompt, before the alias work)
 
Time from incident creation to final routing decision, includes the responder's 1-minute polling interval:
 
| Incident | Seconds | Outcome |
|---|---|---|
| 10-06 column_rename | 29.7 | escalate |
| 10-09 column_rename | 51.2 | escalate |
| 10-12 null_spike | 63.1 | escalate |
| 10-10 column_rename | 66.1 | escalate |
| 10-08 date_format | 73.5 | pr_opened |
| 10-11 null_spike | 80.0 | escalate (agent hit the tool-call limit) |
| 10-05 UnboundLocalError | 83.4 | escalate |
 
- n = 7, median 66 s, mean 64 s, range 30 to 83 s. Only 5 are real diagnoses.
- **Not re-measured** with the longer prompt. The new prompt uses more tokens (offline replays: about 15.6k, 24.8k and 21.2k prompt tokens for patch runs, 5.7k for the null_spike escalation, versus about 9,000 before). Re-measure before quoting any cost or time figure.
- **Where tokens live:** the agent writes an audit row with real token counts and `email_status = PENDING`. The DAG's `record_audit` writes a second row (`SENT`, `final_action` set) with 0 tokens. Take token numbers from the PENDING row and do not double count incidents when computing metrics from the audit table.
---
 
## Claims this project supports
 
- Failure, incident, LLM diagnosis, evidence-citation validation, patch policy, sandbox-tested one-line patch, **real GitHub PR**, human merge, `git pull`, rerun, incident `RESOLVED`, shown for **date format drift** (10-13 via GitHub; 10-03 and 10-08 via local preview) and **single-column rename** (10-16, one live run, LLM-proposed with the code fallback off).
- Guardrails: patch policy (one allow-listed `append_unique` edit), source-based ambiguity check (it blocked an LLM-proposed patch on 10-04), evidence-citation validation, sandbox row and revenue conservation.
- Deterministic errors fail fast. Transient errors retry and recover.
- Anything else (null spikes, ambiguous dates, multi-column changes) escalates with cited evidence and no PR.
- The live rename run exposed a downstream DQ bug that the Silver-only sandbox could not see. It was found, fixed and rerun green.
## Claims this project does NOT support
 
- Autonomous or unattended fixing. A human always merges and triggers the rerun.
- Healing schema drift in general. Only a single-column rename onto `amount` has run live.
- A rate for LLM alias proposals (one live run plus two offline replays).
- Verification of `auth_error` and `missing_file`.
- DeepEval results (evals are postponed).
- Token or cost figures for the current prompt (not re-measured).
---
 
## Phase Status
 
- **Phase 0 (Foundation):** Complete (commit `48502fb`).
- **Phase 1 (Infrastructure):** Complete (commit `26f3992`).
- **Phase 2 (Data and Simulator):** Complete (commit `e232aa8`). Source data for 2026-10-01 to 2026-10-30 only.
- **Phase 3 (Pipeline):** Complete and hardened (profiling fix, base-class fail-fast, DQ alias resolution; DECISION-016, DECISION-023).
- **Phase 4 (Incident Investigator Agent):** Complete and live-verified. Prompt rewritten for renames (DECISION-021).
- **Phase 5 (Fix generation, sandbox, PR, notifications):** Complete. **GitHub PR mode live-verified** (PRs #2 and #4). Sandbox still only runs Silver (DECISION-024).
- **Phase 6 (Streamlit dashboard):** Built. **Not re-verified** after the theme change: readability, the Revenue Dashboard charts, and the red banner on a successful run. Left alone on purpose.
- **Phase 7 (Documentation):** README, `docs/architecture.md`, `docs/runbook.md` and `.github/workflows/ci.yml` exist. **Not re-verified:** they must be checked against this file (the old runbook expected `column_rename` to escalate).
Recent commits (local; confirm with `git --no-pager log --oneline` and `git status` that they are pushed): `675a636` alias fallback, `ddd0957` prompt and schema, `2fb75fb` `ALIAS_FALLBACK` switch, `cbd4284` DQ alias resolution and duplicate removal, plus a later commit for "DQ reads column aliases from silver_rules.yml" (confirm it exists).
 
---
 
## Known Issues (accepted for now)
 
- **CI is red.** `CI / lint-and-test` fails on every PR and on the first push (about 40 s, probably install or lint). Parked. Fix before the demo, since each auto-fix PR shows a failing check. Cause not investigated.
- **Public repo exposure.** The repo was public for a short time. The author email `keerthasundar@gmail.com` is in the commit history. Decide on the GitHub noreply address (and whether to rewrite history) before making the repo public again.
- **Sandbox only runs Silver.** It passed the rename fix that then failed in `data_quality_checks`. Extending the sandbox to run the DQ null-rate check is open work (DECISION-024).
- **Alias edge case:** Silver applies an alias only when the canonical column has no non-null values; DQ coalesces across candidates row by row. A batch with data split across `amount` and `total_amount` would be handled differently. Not tested.
- **Escalation and autofix emails:** the Recommended Action section is empty when the model leaves `recommended_next_steps` blank (the LLM-proposed rename path does). A policy override also leaves the model's text, for example "Implement the proposed patch" on an escalated ambiguous batch (10-04).
- **`run_agent_once` section [3]** recomputes ambiguity from the column profile (0.0000) while `diagnose_incident` uses the source-based value (1.0). For ambiguous batches trust the agent log line and the final action.
- **Daily schedule creates stray incidents.** With `orders_medallion_daily` unpaused, the schedule fails for today's date and `INC-2026-11-24-wait_for_vendor_files-163b08ba` was escalated with "Exceeded max_tool_calls (8) without diagnosis". Keep both DAGs paused when idle. Not investigated; relevant to the `missing_file` run.
- **Two audit rows per incident** (agent row `PENDING`, DAG row `SENT`). Dashboard counts and any per-incident metrics must account for it.
- **Dashboard:** donut and other charts blank in an earlier check, stale red banner on the successful 10-07 run, flat Daily Revenue bars, Pipeline Health unchecked. Left alone.
- **Stray batch 2026-11-01** in Gold (copy of 10-01) inflates dashboard totals. `tests/cleanup_batch.py` must not be run until its dry-run option has been read.
- **Airflow logs** label INFO as ERROR (logger writes to stderr). About 8 minutes per 5,000-row batch.
- **Pytest:** the `spark` marker is unregistered (warning). Spark tests run in the container (about 8 minutes). The last Spark suite run (1 passed, 8:55) happened **before** the final `dq.py` change that reads aliases from `silver_rules.yml`; rerun it. `test_fail_safe_escalation_routing` re-implements routing logic instead of calling `_route_outcome`.
- **Agent loop limit** is checked per model turn, so more than 8 tool calls can be logged.
- **Test files:** `tests/manual_github_smoke.py` and `tests/manual_inspect_bronze.py` are manual helpers, not part of the suite.
---
 
## Configuration switches
 
| Variable | Value | Effect |
|---|---|---|
| `PR_MODE` | `dry_run` / `github` | Preview files versus a real PR. Currently `github` in `.env`. |
| `ALIAS_FALLBACK` | `on` (default) / `off` | When `on`, if the model diagnoses `schema_drift` but proposes no patch, plain code derives the alias (one missing `amount`, exactly one new unknown column). Currently `off` in `.env`, which is how the 10-16 run proved the LLM did it. |
| `AGENT_ENABLED` | `true` / `false` | Kill switch for the LLM agent. |
| `OPENAI_MODEL` | default `gpt-4o-mini` | gpt-4o was never needed for renames. |
 
---
 
## Verified Runbook (PowerShell, from the project folder)
 
Run one pipeline command at a time. Do not restart Docker while a run is active. Keep API keys and tokens out of chat and git.
 
### 0. Prepare
 
```powershell
docker compose ps
docker compose exec airflow-scheduler python -c "import os; print(bool(os.getenv('OPENAI_API_KEY')), os.getenv('OPENAI_MODEL'), os.getenv('PR_MODE'), bool(os.getenv('GITHUB_TOKEN')), os.getenv('GITHUB_REPO'), os.getenv('ALIAS_FALLBACK'))"
git status
docker compose exec ops-postgres psql -U ops_user -d ops -c "SELECT id, status FROM incidents WHERE status='OPEN';"
```
 
Expect healthy services, `True`, a model name, the PR mode, `True`, the repo name and the fallback setting. `config/silver_rules.yml` must not show as modified. Close OPEN incidents first:
 
```powershell
docker compose exec ops-postgres psql -U ops_user -d ops -c "UPDATE incidents SET status='RESOLVED' WHERE status='OPEN';"
```
 
### 1. Unpause both DAGs
 
```powershell
docker compose exec airflow-scheduler airflow dags unpause orders_medallion_daily
docker compose exec airflow-scheduler airflow dags unpause incident_responder
```
 
### 2. Inject a failure and trigger the run
 
Use a date with no incident yet. Dates already used: 2026-10-03 to 2026-10-16.
 
```powershell
docker compose exec airflow-scheduler python -m simulator.publish_batch --failure=date_format --batch-date=2026-10-17
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{\"batch_date\": \"2026-10-17\"}'
```
 
Modes: `none`, `transient_error`, `missing_file`, `date_format`, `column_rename`, `null_spike`, `ambiguous_date`, `auth_error`.
 
### 3. Expected behavior by mode
 
| Mode | Expected |
|---|---|
| `date_format` | Silver fails on try 1, `format_drift`, sandbox passes, PR opened (GitHub mode) or preview (dry run), email |
| `column_rename` | Silver fails on try 1 (`MissingColumnError`), `schema_drift`, LLM proposes `column_aliases.amount`, sandbox passes, PR opened. If the model declines and `ALIAS_FALLBACK=on`, the code fallback proposes it |
| `ambiguous_date` | Silver fails on try 1, policy blocks the patch, escalation email, no PR |
| `null_spike` | Silver passes, `data_quality_checks` fails on try 1, escalation as `data_quality`, no PR |
| `transient_error` | `bronze_ingest` fails on try 1, succeeds on try 2, no incident |
| `auth_error` | Not yet verified. Expected: fail fast, no retries |
| `missing_file` | Not yet verified. Expected: sensor times out after about 2 minutes |
 
### 4. Read the result
 
The audit table has two rows per incident. Sort by `id` and read the `SENT` row.
 
```powershell
docker compose exec ops-postgres psql -U ops_user -d ops -P pager=off -c "SELECT id, status, task_id, batch_date FROM incidents ORDER BY created_at DESC LIMIT 5;"
docker compose exec ops-postgres psql -U ops_user -d ops -P pager=off -x -c "SELECT id, incident_id, final_action, pr_url_or_preview, email_status, diagnosis->>'proposed_action' AS action, diagnosis->'patch' AS patch FROM incident_diagnosis_audit ORDER BY id DESC LIMIT 2;"
```
 
In GitHub mode `pr_url_or_preview` must be `https://github.com/Keerthansundar/data-pipeline-sentinel/pull/N`. A path under `/opt/airflow/data/pr_previews` means it fell back to a local preview; read the `open_fix_pull_request` task log. Then open Mailpit (http://localhost:8025): one `[AUTOFIX]` or `[ESCALATION]` email per incident.
 
### 5. Human approval and recovery (date_format and column_rename)
 
GitHub mode (do **not** use `responder.patcher` here):
 
1. On the PR, open Files changed and confirm exactly one added line.
2. Merge the PR on GitHub.
3. Pull and check. If git refuses because `config/silver_rules.yml` has local edits (whitespace only, for example), inspect with `git --no-pager diff config/silver_rules.yml`, then `git checkout -- config/silver_rules.yml`. If branches diverged, use `git pull --no-rebase`.
```powershell
git pull
findstr "<new value>" config\silver_rules.yml
docker compose exec ops-postgres psql -U ops_user -d ops -P pager=off -c "SELECT id, status FROM incidents WHERE id LIKE 'INC-<date>%';"
docker compose exec airflow-scheduler airflow dags trigger orders_medallion_daily --conf '{\"batch_date\": \"<date>\"}'
```
 
Expect `PR_OPENED` before the rerun, then 7/7 green on try 1 (about 8 minutes) and `RESOLVED` afterwards. Delete the merged remote branch on GitHub.
 
Dry-run mode only: `docker compose exec airflow-scheduler python -m responder.patcher --incident-id <INCIDENT_ID>`, then `git diff config/silver_rules.yml` (one added line), then rerun.
 
### 6. Reset
 
```powershell
docker compose exec airflow-scheduler airflow dags pause incident_responder
docker compose exec airflow-scheduler airflow dags pause orders_medallion_daily
docker compose exec ops-postgres psql -U ops_user -d ops -c "UPDATE incidents SET status='RESOLVED' WHERE status='OPEN';"
```
 
For an unmerged or closed PR: close it on GitHub without merging and delete its branch, or the next run for a similar incident may reuse it.
 
### Offline agent check (no pipeline run, one OpenAI call)
 
```powershell
docker compose exec airflow-scheduler python -m responder.run_agent_once <INCIDENT_ID>
```
 
---
 
## Remaining Work
 
1. Update README, `docs/architecture.md` and `docs/runbook.md` to match this file.
2. Repeat `column_rename` on 2026-10-17 and 10-18 (new dates, one at a time) to get more than one live data point.
3. Extend the sandbox to run the DQ null-rate check on its output.
4. Rerun the Spark suite after the final `dq.py` change.
5. Run `auth_error` and `missing_file`, or keep the claim at 5 of 7 modes.
6. Fix CI (red on every run).
7. Decide on the author email and repo visibility before making the repo public.
8. Re-check the Streamlit pages; clean up the stray 2026-11-01 batch (dry run first).
9. Record a 2 to 3 minute demo, take README screenshots, do a fresh-clone test following the README.
10. DeepEval evals (postponed). Write resume numbers only from measured results.
 
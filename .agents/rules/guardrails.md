# Guardrails and Safety Principles

1. **Read-Only Tools:** The agent's tools (`fetch_task_error_log`, `fetch_run_history`, `inspect_column_profile`, `diff_schema_vs_last_good_batch`, `fetch_data_quality_results`, `trace_downstream_impact`, `read_silver_rules`, `submit_incident_diagnosis`) are strictly read-only.
2. **Strict File Whitelist:** The only file subject to agent-suggested modification is `config/silver_rules.yml`. Proposing modifications to any other file is immediately rejected by policy.
3. **Strict Patch Grammar:**
   - Operation: `append_unique` only.
   - Allowed key paths: `orders.date_formats.order_date` or `orders.column_aliases.<canonical_column>`.
   - Date formats must be in whitelist: `%d/%m/%Y`, `%m/%d/%Y`, `%d-%m-%Y`, `%Y/%m/%d`, `%d.%m.%Y`.
   - Column alias values must be present in the failed batch's schema snapshot and not previously mapped.
   - Maximum of 1 patch per incident.
4. **Evidence Citation Check:** Every quote provided in the diagnosis `evidence` field must match verbatim (case-sensitive substring) within the corresponding cited tool call's output.
5. **Ambiguity Gate:** If `ambiguous_day_month_share` exceeds the allowed threshold (default 0.0), any date-format patch is blocked and the incident is escalated.
6. **Prompt Injection Defense:** System prompt explicitly declares all tool outputs and logs as untrusted text. Inputs are quoted and sanitized.
7. **Secrets and PII Redaction:** `responder/redact.py` must scrub email addresses, API keys (`sk-...`), GitHub tokens, and bearer tokens before logs, tool outputs, or emails are emitted or sent to the LLM.
8. **Kill Switch:** When `AGENT_ENABLED=false`, the incident responder skips calling the LLM and escalates directly via email.
9. **Human Merge Gate:** The agent never pushes to `main` and never merges. PR creation requires human review.

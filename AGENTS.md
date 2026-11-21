# PipelineSentinel Agent Instructions

These instructions are the absolute rules for developing, maintaining, and running PipelineSentinel.

## Hard Rules
1. **The LLM has no write access:** All agent tools are strictly read-only. Side effects (opening PRs, sending alert emails, writing audit records) exist only in deterministic code that the model cannot invoke.
2. **Code decides routing, not the LLM:** The LLM proposes diagnoses and patches; deterministic Python validation, policy checks, and sandbox execution decide whether a PR is opened or the issue is escalated.
3. **Fail-safe default:** Missing OpenAI API keys, timeouts, schema validation failures, policy violations, or sandbox failures must always trigger an escalation email. The data pipeline must never depend on the agent to function.
4. **Humans merge PRs:** The agent only creates branches named `autofix/<incident_id>-<slug>` and opens a pull request (or writes a preview in `data/pr_previews/` when `PR_MODE=dry_run`). It must never touch `main` and never merge.
5. **Single allow-listed target for edits:** The agent can only propose structured edits (`append_unique`) to `config/silver_rules.yml`. No other file may ever be modified by the agent.
6. **No invented API shapes:** Use exact pinned versions for Airflow, PySpark, Delta Lake, OpenAI SDK, Streamlit, and PyGithub. Check official documentation for the pinned versions.
7. **Typed Python and clean style:** All Python modules must have type annotations, docstrings on public functions, and pass `ruff` check and format.
8. **Comprehensive tests with mocks:** Every module must have unit tests. Mock external services (OpenAI, GitHub, SMTP) in unit tests. Spark tests must be marked with `@pytest.mark.spark`.
9. **Meaningful logging:** Follow Section 5A of `PROJECT_BRIEF.md`. Use `pipeline.logging_config.get_logger` only. Never use bare `print()`. Error messages must contain actionable context (expected vs actual, row counts, up to 5 sample non-PII values).

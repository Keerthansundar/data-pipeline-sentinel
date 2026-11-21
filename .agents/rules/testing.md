# Testing Guidelines

- **Framework:** `pytest` with test files located in `tests/` prefixed with `test_`.
- **Zero Real External Calls:**
  - Mock `openai` SDK calls completely (`responder.llm_client`).
  - Mock `PyGithub` and GitHub API calls (`responder.pr`).
  - Use Mailpit or mock `smtplib` for email testing (`responder.notify`).
  - Never initiate live network calls in unit tests.
- **Guardrails Coverage:** Every guardrail listed in Section 8 of `PROJECT_BRIEF.md` must have an explicit unit test in `tests/test_guardrails.py`.
- **Spark & Delta Tests:** Tests requiring a live `SparkSession` must be marked with `@pytest.mark.spark` and use a shared fixture (`local[2]`, memory optimized).
- **Idempotency Testing:** Pipeline tests must verify that running the same batch date twice yields identical row counts and table state.
- **Fast Execution:** Non-spark unit tests should complete in under 5 seconds in aggregate.

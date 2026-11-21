# PipelineSentinel

PipelineSentinel is a self-healing medallion data pipeline with a guardrailed AI incident responder.

## Pitch
A daily Airflow pipeline (Bronze, Silver, Gold on Delta Lake) absorbs transient failures with retries and timeouts. When that is not enough, an OpenAI tool-calling agent diagnoses the failure from logs and data profiles, then either opens a sandbox-verified fix pull request on a new branch or emails the engineer a full audit. The agent can never merge, never run pipeline actions, and can only propose one structured edit to one allow-listed rules file. A Streamlit app shows the backend stages live (Airflow "Task Instances" style), dashboards, and incident audits.

## Documentation
- [PROJECT_BRIEF.md](docs/PROJECT_BRIEF.md) - Single source of truth.
- [DECISIONS.md](docs/DECISIONS.md) - Architecture decision log.
- [architecture.md](docs/architecture.md) - Detailed system architecture.
- [runbook.md](docs/runbook.md) - Operational runbook.
- [VERIFICATION.md](docs/VERIFICATION.md) - Verification logs and outputs.

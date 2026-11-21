# PipelineSentinel Architecture

## System Overview
PipelineSentinel is designed to absorb transient failures autonomously through orchestrator backoff and retries, while utilizing an OpenAI tool-calling agent to diagnose deterministic format/schema drift failures. The agent proposes strict, sandbox-verified rules patches without possessing write access to any production environments.

```mermaid
flowchart TD
    VendorFeed["Vendor Feed Simulator (DAG)"] -->|Publishes CSV & JSON| Landing["data/landing/"]
    Landing --> MedallionDAG["orders_medallion_daily (DAG)"]
    
    subgraph MedallionDAG [Medallion Pipeline]
        wait["wait_for_vendor_files"] --> bronze["bronze_ingest"]
        bronze --> silver["silver_transform"]
        silver --> dq["data_quality_checks"]
        dq --> gold["gold_build"]
        gold --> freshness["freshness_check"]
        freshness --> resolve["resolve_incidents"]
        silver -.->|Quarantine bad rows| Quarantine["data/quarantine/"]
    end

    MedallionDAG -->|Observability & Failure Callback| OpsDB[("Ops Postgres DB")]
    OpsDB -->|OPEN Incidents| ResponderDAG["incident_responder (DAG)"]

    subgraph ResponderDAG [Incident Responder & Agent]
        claim["claim_incident"] --> agent["run_diagnosis_agent (Read-only tools)"]
        agent --> validate["validate_diagnosis (Evidence citation check)"]
        validate --> policy["enforce_patch_policy (Whitelist & Ambiguity checks)"]
        policy --> sandbox["run_sandbox_verification (data/sandbox/)"]
        sandbox --> route["route_outcome"]
        route -->|Pass| pr["open_fix_pull_request (Branch / Preview)"]
        route -->|Fail/Escalate| email["send_incident_email (Mailpit)"]
        pr --> audit["record_audit"]
        email --> audit
    end

    OpsDB --> Dashboard["Streamlit Dashboard (8501)"]
    MedallionDAG --> Mailpit["Mailpit (8025)"]
```

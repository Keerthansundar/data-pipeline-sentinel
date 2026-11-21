# PipelineSentinel Runbook

## Operating Procedures

### 1. Starting the Stack
```bash
make up
```
Checks:
- Airflow web UI is accessible at `http://localhost:8080` (user: `airflow`, pass: `airflow`).
- Streamlit app is accessible at `http://localhost:8501`.
- Mailpit is accessible at `http://localhost:8025`.

### 2. Generating Historical Data
```bash
make seed
```
Creates 30 days of seeded synthetic orders and customers in `data/source_archive/`.

### 3. Running Demos
```bash
# Healthy run
make demo FAILURE=none BATCH=2026-10-01

# Date format drift failure
make demo FAILURE=date_format BATCH=2026-10-02

# Column rename failure
make demo FAILURE=column_rename BATCH=2026-10-03
```

### 4. Applying Fixes and Rerunning
```bash
make apply-fix INCIDENT=<incident_id>
make rerun BATCH=<batch_date>
```

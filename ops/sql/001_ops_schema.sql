-- ============================================================================
-- Ops Database Schema (PipelineSentinel Observability & Incident Audit)
-- ============================================================================

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    batch_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL,
    rows_in BIGINT DEFAULT 0,
    rows_out BIGINT DEFAULT 0,
    rows_quarantined BIGINT DEFAULT 0,
    error_type VARCHAR(255),
    error_message TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS pipeline_log (
    id SERIAL PRIMARY KEY,
    logged_at TIMESTAMP WITH TIME ZONE NOT NULL,
    level VARCHAR(20) NOT NULL,
    logger VARCHAR(255) NOT NULL,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    run_id VARCHAR(255),
    batch_date VARCHAR(50),
    try_number VARCHAR(20),
    message TEXT NOT NULL,
    context JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_pipeline_log_search
ON pipeline_log (dag_id, task_id, batch_date, logged_at);

CREATE TABLE IF NOT EXISTS column_profile (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    total_rows BIGINT NOT NULL,
    null_count BIGINT NOT NULL,
    null_rate DOUBLE PRECISION NOT NULL,
    distinct_count BIGINT,
    patterns JSONB,
    ambiguous_day_month_share DOUBLE PRECISION DEFAULT 0.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS schema_snapshot (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    columns JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dq_results (
    id SERIAL PRIMARY KEY,
    batch_date DATE NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    observed_value TEXT NOT NULL,
    threshold_value TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    details TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS demo_config (
    id SERIAL PRIMARY KEY,
    failure_mode VARCHAR(50) NOT NULL DEFAULT 'none',
    target_batch_date DATE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO demo_config (id, failure_mode, target_batch_date)
VALUES (1, 'none', CURRENT_DATE)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS incidents (
    id VARCHAR(100) PRIMARY KEY,
    status VARCHAR(50) NOT NULL, -- OPEN, IN_PROGRESS, PR_OPENED, ESCALATED, RESOLVED
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    batch_date DATE NOT NULL,
    error_type VARCHAR(255),
    error_message TEXT,
    error_trace TEXT,
    error_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_incident_dedup UNIQUE (task_id, batch_date, error_hash)
);

CREATE TABLE IF NOT EXISTS incident_tool_calls (
    id SERIAL PRIMARY KEY,
    incident_id VARCHAR(100) NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    step INT NOT NULL,
    tool_name VARCHAR(100) NOT NULL,
    arguments JSONB,
    output_truncated TEXT,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    duration_ms INT
);

CREATE TABLE IF NOT EXISTS incident_diagnosis_audit (
    id SERIAL PRIMARY KEY,
    incident_id VARCHAR(100) NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    diagnosis JSONB NOT NULL,
    validation_results JSONB NOT NULL,
    policy_results JSONB NOT NULL,
    sandbox_results JSONB NOT NULL,
    final_action VARCHAR(50) NOT NULL,
    pr_url_or_preview TEXT,
    email_status VARCHAR(50),
    model VARCHAR(100),
    prompt_tokens INT DEFAULT 0,
    completion_tokens INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

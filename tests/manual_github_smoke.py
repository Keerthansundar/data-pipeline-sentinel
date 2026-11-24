"""Manual smoke test: opens a real PR on GITHUB_REPO. Not collected by pytest."""

from responder.pr import _open_github_pr

audit = {
    "diagnosis": {
        "summary": "Manual GitHub smoke test (not a real incident)",
        "root_cause_category": "format_drift",
        "confidence": 0.9,
        "proposed_action": "rules_patch",
        "patch": {
            "file": "config/silver_rules.yml",
            "operation": "append_unique",
            "key_path": "orders.date_formats.order_date",
            "value": "%d/%m/%Y",
        },
        "evidence": [],
    },
    "sandbox_results": {},
}

print(_open_github_pr("INC-GH-SMOKE-001", audit))

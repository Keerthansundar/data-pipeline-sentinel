from responder.alias_fallback import apply_alias_fallback, propose_alias_patch

REQ = ["order_id", "customer_id", "order_date", "amount", "status", "channel"]
ERR = ("Required column 'amount' missing from input dataset. Available columns: "
       "['order_id', 'customer_id', 'order_date', 'status', 'channel', 'total_amount']")


def test_single_rename_gives_patch():
    p = propose_alias_patch(ERR, REQ, {"amount": ["amount"]})
    assert p == {"file": "config/silver_rules.yml", "operation": "append_unique",
                 "key_path": "orders.column_aliases.amount", "value": "total_amount"}


def test_two_extra_columns_escalates():
    err = ERR.replace("'total_amount']", "'total_amount', 'amt2']")
    assert propose_alias_patch(err, REQ, {}) is None


def test_non_amount_column_escalates():
    err = ERR.replace("'amount' missing", "'status' missing")
    assert propose_alias_patch(err, REQ, {}) is None


def test_existing_alias_does_not_loop():
    assert propose_alias_patch(ERR, REQ, {"amount": ["amount", "total_amount"]}) is None


def test_garbage_input():
    assert propose_alias_patch("boom", REQ, {}) is None
    assert propose_alias_patch(None, REQ, {}) is None



RULES = {"orders": {"required_columns": REQ, "column_aliases": {"amount": ["amount"]}}}


def test_fallback_applies_on_schema_drift_without_patch():
    d = {"root_cause_category": "schema_drift", "proposed_action": "escalate", "patch": None}
    out, applied = apply_alias_fallback(d, ERR, RULES)
    assert applied and out["proposed_action"] == "rules_patch"
    assert out["patch"]["value"] == "total_amount"


def test_fallback_ignores_other_categories():
    d = {"root_cause_category": "data_quality", "proposed_action": "escalate", "patch": None}
    assert apply_alias_fallback(d, ERR, RULES) == (d, False)


def test_fallback_keeps_model_patch():
    d = {"root_cause_category": "schema_drift", "proposed_action": "rules_patch",
         "patch": {"value": "x"}}
    assert apply_alias_fallback(d, ERR, RULES) == (d, False)

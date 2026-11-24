from pipeline.column_resolve import candidate_columns

ALIASES = {"amount": ["amount", "total_amount"]}


def test_both_present_keeps_order():
    cols = ["order_id", "amount", "total_amount"]
    assert candidate_columns("amount", cols, ALIASES) == ["amount", "total_amount"]


def test_only_alias_present():
    assert candidate_columns("amount", ["order_id", "total_amount"], ALIASES) == ["total_amount"]


def test_none_present():
    assert candidate_columns("amount", ["order_id"], ALIASES) == []


def test_no_aliases():
    assert candidate_columns("amount", ["order_id", "amount"], None) == ["amount"]

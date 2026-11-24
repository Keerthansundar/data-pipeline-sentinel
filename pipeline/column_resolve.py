"""Find which raw columns can supply a canonical column."""


def candidate_columns(canonical, available_columns, aliases):
    """Return [canonical] + aliases that exist in available_columns, in order, no duplicates."""
    wanted = [canonical, *(aliases or {}).get(canonical, [])]
    out = []
    for name in wanted:
        if name in available_columns and name not in out:
            out.append(name)
    return out

"""Offline eval: replay stored incidents through the diagnosis agent and score the decisions.

Run inside the container:
    python -m tests.evals.run_eval --list
    python -m tests.evals.run_eval --repeats 1
    python -m tests.evals.run_eval --repeats 3

Replays do not write audit rows or change incident status (audit writes are captured in memory).
Each run costs one OpenAI call (several tool calls inside it).
"""

import argparse
import csv
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

import yaml

from pipeline.config import get_project_root
from responder.agent import diagnose_incident
from responder.repository import OpsRepository

# Fill from the OpenAI pricing page (USD per 1M tokens). Leave None to skip the cost figure.
PRICE_IN_PER_M = None
PRICE_OUT_PER_M = None

GOLDEN = Path(__file__).with_name("golden.yml")


class CapturingRepo(OpsRepository):
    """Reads like the real repo, but keeps audit and tool-call writes in memory."""

    def __init__(self):
        super().__init__()
        self.audits = []
        self.tool_calls = 0

    def record_audit(self, **kwargs):
        self.audits.append(kwargs)

    def record_tool_call(self, **kwargs):
        self.tool_calls += 1


def run_once(case, run_no):
    repo = CapturingRepo()
    row = {
        "incident_id": case["incident_id"],
        "kind": case["kind"],
        "run": run_no,
        "expected_action": case["expected_action"],
        "error": "",
    }
    t0 = time.perf_counter()
    try:
        diag = diagnose_incident(case["incident_id"], repo=repo)
    except Exception as exc:  # keep the batch going
        row["error"] = str(exc)[:200]
        return row
    row["seconds"] = round(time.perf_counter() - t0, 1)

    audit = repo.audits[-1] if repo.audits else {}
    val = audit.get("validation_results") or {}
    pol = audit.get("policy_results") or {}
    final_action = diag.proposed_action.value
    patch = diag.patch

    correct = final_action == case["expected_action"]
    patch_match = ""
    if case["expected_action"] == "rules_patch" and final_action == "rules_patch":
        patch_match = bool(
            patch
            and patch.key_path == case.get("expected_key_path")
            and patch.value == case.get("expected_value")
        )
        correct = correct and patch_match

    model_patch = patch is not None
    row.update(
        {
            "final_action": final_action,
            "category": diag.root_cause_category.value,
            "confidence": diag.confidence,
            "correct": correct,
            "patch_match": patch_match,
            "unsafe_patch": case["expected_action"] == "escalate" and final_action == "rules_patch",
            "model_proposed_patch": model_patch,
            "caught_by_policy": model_patch
            and final_action == "escalate"
            and val.get("passed") is True
            and pol.get("passed") is False,
            "citation_ok": bool(val.get("passed")),
            "tool_calls": repo.tool_calls,
            "prompt_tokens": audit.get("prompt_tokens") or 0,
            "completion_tokens": audit.get("completion_tokens") or 0,
            "patch_value": patch.value if patch else "",
            "escalation_reason": (diag.escalation_reason or "")[:160],
        }
    )
    if PRICE_IN_PER_M is not None and PRICE_OUT_PER_M is not None:
        row["cost_usd"] = round(
            row["prompt_tokens"] * PRICE_IN_PER_M / 1e6
            + row["completion_tokens"] * PRICE_OUT_PER_M / 1e6,
            5,
        )
    return row


def pct(num, den):
    return f"{100.0 * num / den:.1f}%" if den else "n/a"


def summarize(rows):
    ok = [r for r in rows if not r["error"]]
    errors = len(rows) - len(ok)
    print("\n=== SUMMARY ===")
    print(f"runs: {len(rows)} (errors: {errors})")
    if not ok:
        return
    n = len(ok)
    correct = sum(1 for r in ok if r["correct"])
    print(f"correct heal-vs-escalate (and right patch): {correct}/{n} = {pct(correct, n)}")

    unsafe = [r for r in ok if r["unsafe_patch"]]
    print(f"unsafe patches (expected escalate, patch survived validation + policy): {len(unsafe)}")
    esc_cases = [r for r in ok if r["expected_action"] == "escalate"]
    proposed = [r for r in esc_cases if r["model_proposed_patch"]]
    caught = [r for r in proposed if r["caught_by_policy"]]
    print(
        f"escalate-labelled runs where the MODEL proposed a patch: {len(proposed)}"
        f" (caught by policy: {len(caught)})"
    )
    cit = sum(1 for r in ok if r["citation_ok"])
    print(f"citation check passed: {cit}/{n} = {pct(cit, n)}")
    print(f"median seconds per run: {statistics.median(r['seconds'] for r in ok):.1f}")
    print(f"median prompt tokens: {statistics.median(r['prompt_tokens'] for r in ok):.0f}")
    print(f"mean completion tokens: {statistics.mean(r['completion_tokens'] for r in ok):.0f}")
    if "cost_usd" in ok[0]:
        print(f"mean cost per run: ${statistics.mean(r['cost_usd'] for r in ok):.4f}")

    print("\nper incident (action chosen per repeat):")
    seen = []
    for r in ok:
        if r["incident_id"] not in seen:
            seen.append(r["incident_id"])
    for inc in seen:
        runs = [r for r in ok if r["incident_id"] == inc]
        acts = ",".join("P" if r["final_action"] == "rules_patch" else "E" for r in runs)
        good = sum(1 for r in runs if r["correct"])
        print(f"  {inc}  expected={runs[0]['expected_action']:<11} got=[{acts}]  correct {good}/{len(runs)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--only", default="", help="substring filter on incident_id")
    ap.add_argument("--list", action="store_true", help="print cases and exit (no API calls)")
    args = ap.parse_args()

    cases = yaml.safe_load(GOLDEN.read_text(encoding="utf-8"))["cases"]
    cases = [c for c in cases if args.only in c["incident_id"]]
    if args.list:
        for c in cases:
            print(f"{c['incident_id']:<50} {c['kind']:<15} -> {c['expected_action']}")
        print(f"{len(cases)} cases x {args.repeats} repeats = {len(cases) * args.repeats} runs")
        return

    rows = []
    total = len(cases) * args.repeats
    done = 0
    for case in cases:
        for run_no in range(1, args.repeats + 1):
            done += 1
            row = run_once(case, run_no)
            rows.append(row)
            status = row["error"] or f"{row['final_action']} correct={row['correct']}"
            print(f"[{done}/{total}] {case['incident_id']} run {run_no}: {status}", flush=True)

    out_dir = get_project_root() / "data" / "eval"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"results_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.csv"
    fields = sorted({k for r in rows for k in r})
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"\nresults written to {out}")
    summarize(rows)


if __name__ == "__main__":
    main()

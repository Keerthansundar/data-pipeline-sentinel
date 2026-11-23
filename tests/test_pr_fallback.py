from pathlib import Path

from responder import pr


def _audit() -> dict:
    return {"diagnosis": {}, "sandbox_results": {}}


def test_github_mode_without_token_writes_preview(monkeypatch, tmp_path):
    monkeypatch.setenv("PR_MODE", "github")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_REPO", raising=False)
    monkeypatch.setattr(pr, "get_project_root", lambda: tmp_path)

    mode, path = pr.open_fix_pull_request("INC-T1", "diff text", _audit())

    assert mode == "dry_run"
    assert Path(path).exists()


def test_github_error_falls_back_without_recursing(monkeypatch, tmp_path):
    monkeypatch.setenv("PR_MODE", "github")
    monkeypatch.setenv("GITHUB_TOKEN", "x")
    monkeypatch.setenv("GITHUB_REPO", "owner/repo")
    monkeypatch.setattr(pr, "get_project_root", lambda: tmp_path)

    def boom(*args, **kwargs):
        raise RuntimeError("api down")

    monkeypatch.setattr(pr, "_open_github_pr", boom)

    mode, path = pr.open_fix_pull_request("INC-T2", "diff text", _audit())

    assert mode == "dry_run"
    assert Path(path).exists()

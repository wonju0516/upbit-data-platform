"""훅이 쓰는 git 읽기 도구. 작업 트리가 아니라 인덱스·커밋을 읽는다."""

from __future__ import annotations

import subprocess


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], capture_output=True, check=check)


def show(spec: str) -> str | None:
    """`git show <spec>`의 내용. 없으면 None. 예: ':path'(인덱스), 'HEAD:path'."""
    r = git("show", spec, check=False)
    if r.returncode != 0:
        return None
    return r.stdout.decode("utf-8", errors="replace")


def staged_files() -> list[str]:
    """이번 커밋에 들어갈(추가·수정·이름 변경) 파일 경로."""
    out = git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z").stdout
    return [p for p in out.decode("utf-8").split("\0") if p]


def staged_size(path: str) -> int:
    return int(git("cat-file", "-s", f":{path}").stdout.decode().strip())


def staged_bytes(path: str) -> bytes:
    return git("show", f":{path}").stdout


def commit_files(sha: str) -> list[str]:
    out = git("diff-tree", "--no-commit-id", "--name-only", "-r", "-z", sha).stdout
    return [p for p in out.decode("utf-8").split("\0") if p]


def commit_author_email(sha: str) -> str:
    return git("log", "-1", "--format=%ae", sha).stdout.decode().strip()


def commit_message(sha: str) -> str:
    return git("log", "-1", "--format=%B", sha).stdout.decode("utf-8", errors="replace")


def rev_list(rng: str) -> list[str]:
    out = git("rev-list", "--reverse", "--no-merges", *rng.split()).stdout.decode()
    return [s for s in out.split() if s]


def has_upstream() -> bool:
    return git("rev-parse", "--abbrev-ref", "@{u}", check=False).returncode == 0

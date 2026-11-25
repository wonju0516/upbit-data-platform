"""tools/ 훅·원장 도구 테스트. 표준 라이브러리 unittest만 쓴다."""

import datetime as dt
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

import check_commit_msg as ccm  # noqa: E402
import ledger  # noqa: E402
import pre_commit_check as pcc  # noqa: E402

SHA = "a" * 40
NOW = dt.datetime(2026, 11, 26, 3, 15, 0, tzinfo=dt.timezone.utc)


def _ledger_with_one_row(tmp: Path) -> tuple[str, str, str]:
    """임시 저장소에 원장 한 행을 쓰고 (원장 내용, run_id, config_sha256)를 돌려준다."""
    rid = ledger.append_run(
        stage="P0",
        script="ingest/p0_profile.py",
        config={"seconds": 600, "markets": "KRW-ALL"},
        sample="KRW 전체, 600초",
        status="ok",
        metrics={"orderbook_msgs_per_s": 512.3},
        repo=tmp,
        now=NOW,
        git_sha=SHA,
    )
    text = (tmp / ledger.LEDGER_PATH).read_text(encoding="utf-8")
    return text, rid, ledger.parse(text)[0]["config_sha256"]


class LedgerTest(unittest.TestCase):
    def test_append_and_validate(self):
        with tempfile.TemporaryDirectory() as d:
            text, rid, _ = _ledger_with_one_row(Path(d))
            self.assertTrue(text.startswith(ledger.HEADER))
            self.assertTrue(ledger.RUN_ID_RE.match(rid))
            self.assertEqual(ledger.validate(text), [])

    def test_bad_header(self):
        self.assertTrue(ledger.validate("x\ty\n"))

    def test_wrong_column_count(self):
        self.assertTrue(ledger.validate(ledger.HEADER + "only\tthree\tcells\n"))

    def test_append_only(self):
        with tempfile.TemporaryDirectory() as d:
            text, _, _ = _ledger_with_one_row(Path(d))
            self.assertEqual(ledger.check_append_only(text, text + "more\n"), [])
            edited = text.replace("512.3", "999.9")
            self.assertTrue(ledger.check_append_only(text, edited))
            self.assertTrue(ledger.check_append_only(text, ledger.HEADER))

    def test_rejects_tab_in_value(self):
        with tempfile.TemporaryDirectory() as d, self.assertRaises(ValueError):
            ledger.append_run(
                stage="P0",
                script="x.py",
                config={},
                sample="a\tb",
                status="ok",
                metrics={},
                repo=Path(d),
                now=NOW,
                git_sha=SHA,
            )

    def test_config_hash_is_order_independent(self):
        self.assertEqual(
            ledger.config_sha256({"a": 1, "b": 2}), ledger.config_sha256({"b": 2, "a": 1})
        )


class CommitMsgTest(unittest.TestCase):
    def ok(self, msg, ledger_text=None, files=()):
        return ccm.check(msg, ledger_text, list(files))

    def test_valid_docs(self):
        self.assertEqual(self.ok("docs(docs): 결정 문서를 정리한다\n\n이유를 적는다\n"), [])

    def test_bad_format(self):
        self.assertTrue(self.ok("update docs"))

    def test_unknown_scope_and_type(self):
        errs = self.ok("feature(db): 무언가를 한다")
        self.assertTrue(any("type" in e for e in errs))
        self.assertTrue(any("scope" in e for e in errs))

    def test_period_and_length(self):
        self.assertTrue(self.ok("docs(docs): 마침표로 끝난다."))
        self.assertTrue(self.ok("docs(docs): " + "가" * 70 + "다"))

    def test_korean_declarative(self):
        self.assertTrue(self.ok("docs(docs): add rules"))

    def test_blank_line_after_title(self):
        self.assertTrue(self.ok("docs(docs): 규칙을 고친다\n본문이 바로 붙음"))

    def test_bot_author(self):
        self.assertTrue(ccm.is_bot("49699333+dependabot[bot]@users.noreply.github.com"))
        self.assertFalse(ccm.is_bot("21000000+someone@users.noreply.github.com"))

    def test_merge_is_allowed(self):
        self.assertEqual(self.ok("Merge pull request #3 from x/y"), [])

    def test_fix_needs_invalidates(self):
        self.assertTrue(self.ok("fix(ingest): 재연결을 고친다\n\n이유\n"))
        self.assertEqual(self.ok("fix(ingest): 재연결을 고친다\n\n이유\n\nInvalidates: none\n"), [])

    def test_record(self):
        with tempfile.TemporaryDirectory() as d:
            text, rid, cfg = _ledger_with_one_row(Path(d))
            msg = (
                "record(ingest): 0단계 호가 초당 건수를 기록한다\n\n이유\n\n"
                f"Run-Id: {rid}\nLedger: experiments/LEDGER.tsv\nConfig-SHA256: {cfg}\n"
            )
            self.assertEqual(self.ok(msg, text, ["experiments/LEDGER.tsv"]), [])
            self.assertTrue(self.ok(msg, text, ["experiments/LEDGER.tsv", "ingest/x.py"]))
            self.assertTrue(self.ok(msg.replace(rid, "R20261126-000000-ffffff"), text))
            self.assertTrue(self.ok(msg.replace(cfg, "b" * 64), text))
            self.assertTrue(self.ok("record(ingest): 기록한다\n\n이유\n", text))


class PreCommitTest(unittest.TestCase):
    def test_forbidden_paths(self):
        self.assertTrue(pcc.check_path("data/raw/x.json"))
        self.assertTrue(pcc.check_path("_private/LOCAL.md"))
        self.assertTrue(pcc.check_path(".env"))
        self.assertTrue(pcc.check_path("docker/.env.local"))
        self.assertEqual(pcc.check_path(".env.example"), [])
        self.assertTrue(pcc.check_path("notes/sample.parquet"))
        self.assertEqual(pcc.check_path("tools/ledger.py"), [])

    def test_size_and_binary(self):
        self.assertTrue(pcc.check_content("big.txt", b"a" * (pcc.MAX_BYTES + 1)))
        self.assertTrue(pcc.check_content("x.bin", b"\0\1\2"))

    def test_secrets(self):
        token = "gh" + "p_" + "A" * 36
        self.assertTrue(pcc.check_content("a.py", token.encode()))
        hook = "https://discord.com/api/" + "webhooks/123456/abcDEF_-x"
        self.assertTrue(pcc.check_content("a.md", hook.encode()))
        key = "-----BEGIN OPENSSH " + "PRIVATE KEY-----"
        self.assertTrue(pcc.check_content("k", key.encode()))
        self.assertEqual(pcc.check_content("a.md", b"DISCORD_WEBHOOK_GENERAL="), [])

    def test_notebook_outputs(self):
        nb = b'{"cells": [{"outputs": [{"text": "x"}]}]}'
        self.assertTrue(pcc.check_content("a.ipynb", nb))
        self.assertEqual(pcc.check_content("a.ipynb", b'{"cells": [{"outputs": []}]}'), [])

    def test_ledger_append_only(self):
        with tempfile.TemporaryDirectory() as d:
            text, _, _ = _ledger_with_one_row(Path(d))
            self.assertEqual(pcc.check_ledger(text, text), [])
            self.assertEqual(pcc.check_ledger(text, ledger.HEADER), [])  # 새 행 추가
            self.assertTrue(pcc.check_ledger(ledger.HEADER, text))  # 행 삭제
            self.assertTrue(pcc.check_ledger(text.replace("512.3", "1.0"), text))  # 행 수정


if __name__ == "__main__":
    unittest.main()

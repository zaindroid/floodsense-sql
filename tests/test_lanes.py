"""The two keys, over the real routes.

No database and no engine: an unconfigured upstream answers 503, and that
only happens once auth and the statement guard have both let the call
through. So 503 means allowed and 400 means refused.

This exists because the lane was once wired into the wrong one of the two
run routes, which left the engine route reading `lane` before it was set and
put a write guard in front of a read-only Postgres. Both routes are checked
here by name for that reason.
"""

from __future__ import annotations

import os
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATABASE_URL", "postgresql://u:p@127.0.0.1:1/none")
os.environ["SQL_API_KEY"] = "read-key-for-the-test"
os.environ["SQL_WRITE_KEY"] = "write-key-for-the-test"

from fastapi.testclient import TestClient  # noqa: E402

from app.__main__ import app  # noqa: E402

READ_KEY = "read-key-for-the-test"
WRITE_KEY = "write-key-for-the-test"
ENGINE = "/zetaris/api/v1.0/sql-editor/sqls/run"
SEED = "/api/v1.0/sql-editor/sqls/run"
CREATE = "CREATE OR REPLACE SCHEMASTORE VIEW v WITH CONTAINER floodsense AS SELECT 1 AS n"

ALLOWED, REFUSED, UNAUTHORISED = 503, 400, 401


class LaneTest(unittest.TestCase):
    def setUp(self) -> None:
        self.c = TestClient(app)

    def post(self, path: str, key: str, sql: str):
        return self.c.post(
            path,
            json={"sql": sql, "queryId": str(uuid.uuid4())},
            headers={"Authorization": f"Bearer {key}"},
        )

    def test_the_read_key_reads_and_does_nothing_else(self) -> None:
        self.assertEqual(self.post(ENGINE, READ_KEY, "SELECT 1").status_code, ALLOWED)
        refused = self.post(ENGINE, READ_KEY, CREATE)
        self.assertEqual(refused.status_code, REFUSED)
        self.assertIn("only SELECT", refused.json()["detail"])

    def test_the_write_key_runs_what_a_migration_is_made_of(self) -> None:
        for sql in (CREATE, "REFRESH TABLE t", "SELECT 1", "DESCRIBE floodsense.hazard_now"):
            self.assertEqual(self.post(ENGINE, WRITE_KEY, sql).status_code, ALLOWED, sql)

    def test_the_write_key_cannot_remove_or_edit_anything(self) -> None:
        for sql in (
            "DROP VIEW floodsense.v",
            "DELETE FROM t",
            "TRUNCATE t",
            "ALTER TABLE t ADD COLUMN c INT",
            "UPDATE t SET c = 1",
        ):
            self.assertEqual(self.post(ENGINE, WRITE_KEY, sql).status_code, REFUSED, sql)

    def test_a_statement_that_hides_a_second_one_is_refused(self) -> None:
        self.assertEqual(
            self.post(ENGINE, WRITE_KEY, CREATE + "; DROP VIEW floodsense.v").status_code,
            REFUSED,
        )

    def test_a_statement_is_read_past_its_comments(self) -> None:
        """Migrations arrive with their header comments attached.

        The first version of this guard matched the first word of the raw
        text, which refused every real migration: they all open with a
        comment block.
        """
        sql = "-- view: floodsense.v\n-- columns: n int\n-- min_rows: 1\n" + CREATE
        self.assertEqual(self.post(ENGINE, WRITE_KEY, sql).status_code, ALLOWED)

    def test_a_word_in_a_comment_is_not_the_statement(self) -> None:
        sql = "-- replaces the view 017 would otherwise drop\n" + CREATE
        self.assertEqual(self.post(ENGINE, WRITE_KEY, sql).status_code, ALLOWED)

    def test_a_comment_cannot_hide_what_follows_it(self) -> None:
        for sql in (
            "-- harmless\nDROP VIEW floodsense.v",
            "/* harmless */ DELETE FROM t",
            CREATE + " -- and then\nDROP VIEW floodsense.v",
        ):
            self.assertEqual(self.post(ENGINE, WRITE_KEY, sql).status_code, REFUSED, sql)

    def test_quoted_text_is_left_alone(self) -> None:
        # A view's own SQL may contain anything; only comments are stripped.
        sql = "CREATE OR REPLACE SCHEMASTORE VIEW v WITH CONTAINER c AS SELECT '--not a comment' AS s"
        self.assertEqual(self.post(ENGINE, WRITE_KEY, sql).status_code, ALLOWED)

    def test_the_seed_lane_stays_read_only_for_both_keys(self) -> None:
        # It is this service's own Postgres on a read-only role. The write key
        # is about the engine and buys nothing here.
        for key in (READ_KEY, WRITE_KEY):
            r = self.post(SEED, key, "CREATE TABLE t (c INT)")
            self.assertEqual(r.status_code, REFUSED, key)
            self.assertIn("only SELECT", r.json()["detail"])

    def test_an_unknown_key_gets_nowhere(self) -> None:
        self.assertEqual(self.post(ENGINE, "nonsense", "SELECT 1").status_code, UNAUTHORISED)
        self.assertEqual(self.c.post(ENGINE, json={"sql": "SELECT 1"}).status_code, UNAUTHORISED)

    def test_login_hands_back_the_key_the_caller_arrived_with(self) -> None:
        # zetaris_sync.py logs in and then uses the idToken for every
        # statement, so a write caller handed the read key would silently
        # lose the lane it asked for.
        for key in (READ_KEY, WRITE_KEY):
            r = self.c.get(
                "/zetaris/api/v1.0/auth/login", headers={"Authorization": f"Bearer {key}"}
            )
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json()["idToken"], key)


if __name__ == "__main__":
    unittest.main()

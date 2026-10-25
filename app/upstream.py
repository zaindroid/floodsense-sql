"""A keyed public door onto the real Zetaris.

Zetaris runs on servingz, which has no public IP, and its REST port is bound
to the tailnet. Teammates who are not on that tailnet cannot reach it at all.
This service can: it runs on a node that is both public and on the tailnet.

So it forwards the one call the agents make to the real engine, and adds two
things the direct route does not have:

  * one shared key instead of the engine's admin password, which stays on
    servingz and is never handed to a caller;
  * the same read-only guard this service applies to its own queries, so a
    leaked key cannot write through this door even though the engine itself
    would accept DDL.

What it does not do is pretend to be the engine. Replies are passed through
untouched, so they carry no `engine` field and no X-Engine header, and an
agent reading them reports `zetaris` -- which is true: the statement ran on
Lightning. The hop itself is recorded in X-Via for anyone looking.

Unset ZETARIS_UPSTREAM_URL and these routes answer 503; nothing else in the
service depends on them.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import urllib.error
import urllib.request
import uuid
from base64 import b64encode

log = logging.getLogger("floodsense.sql.upstream")

URL = os.environ.get("ZETARIS_UPSTREAM_URL", "").strip().rstrip("/")
ORG_ID = os.environ.get("ZETARIS_UPSTREAM_ORG_ID", "1").strip()
USER = os.environ.get("ZETARIS_UPSTREAM_USER", "").strip()
PASSWORD = os.environ.get("ZETARIS_UPSTREAM_PASSWORD", "")

LOGIN_TIMEOUT_S = 30
QUERY_TIMEOUT_S = 180

_token: str | None = None
_lock = threading.Lock()


class Unavailable(RuntimeError):
    """No upstream is configured, or it could not be reached."""


class Upstream(RuntimeError):
    """The upstream answered, but with an error."""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(body)
        self.status = status
        self.body = body


def configured() -> bool:
    return bool(URL and USER and PASSWORD)


def _request(path: str, *, data: bytes | None = None, headers: dict[str, str]) -> tuple[int, bytes]:
    req = urllib.request.Request(f"{URL}{path}", data=data, headers=headers)
    req.get_method = lambda: "POST" if data is not None else "GET"  # noqa: E731
    timeout = QUERY_TIMEOUT_S if data is not None else LOGIN_TIMEOUT_S
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 - operator-set host
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except OSError as e:
        # Connection refused, DNS, tailnet down: the engine is not reachable.
        raise Unavailable(f"{type(e).__name__}: {str(e)[:160]}") from e


def _login() -> str:
    """Trade the engine's credentials for a short-lived token.

    Zetaris wants basic auth and a numeric org id, and answers 400 without
    the header rather than 401, which is worth knowing when this breaks.
    """
    basic = b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    status, body = _request(
        "/auth/login",
        headers={
            "Authorization": f"Basic {basic}",
            "X-Org-ID": ORG_ID,
            "X-Request-ID": str(uuid.uuid4()),
        },
    )
    if status != 200:
        raise Upstream(status, body.decode("utf-8", "replace")[:300])
    token = json.loads(body).get("idToken")
    if not token:
        raise Upstream(502, "upstream login returned no idToken")
    log.info("upstream login ok")
    return token


def _token_now(force: bool = False) -> str:
    global _token
    with _lock:
        if force or not _token:
            _token = _login()
        return _token


# A statement the engine refused names an error class in brackets, or says
# what it expected. Anything else in a 400 is about the caller, not the SQL.
_STATEMENT_ERROR = re.compile(rb"\[[A-Z][A-Z0-9_.]+]|wrong type|expecting", re.I)


def _token_rejected(status: int, body: bytes) -> bool:
    """Measured, because it is not what you would guess: a stale token is a
    400, and has come back as both {"msg":"Not Allowed"} and {"msg":"Bad
    Request"}. A missing Authorization header is the only 401.

    A refused statement is 400 as well, so the rule reads the body -- but the
    other way round from the obvious one. Listing the token wordings meant the
    second one, which turned up a day later, stopped every query including
    SELECT 1 until the process was restarted. So a 400 counts as a token
    problem unless the body names the statement, and an unknown wording costs
    one extra round trip instead of a session.
    """
    if status in (401, 403):
        return True
    if status != 400:
        return False
    return not _STATEMENT_ERROR.search(body)


def run_sql(sql: str, *, limit: int, query_id: str | None) -> dict:
    """Forward one checked read. Retries once on a rejected token, because
    the engine's tokens expire and the caller should not have to care."""
    if not configured():
        raise Unavailable("no upstream configured")

    # Zetaris validates queryId as a uuid and 400s on anything else, so a
    # caller that sends none, or sends a label, still gets an answer.
    try:
        qid = str(uuid.UUID(str(query_id)))
    except (ValueError, TypeError, AttributeError):
        qid = str(uuid.uuid4())

    payload = json.dumps(
        {"queryId": qid, "sql": sql, "limit": limit, "source": "SqlEditor"}
    ).encode()

    for attempt in (0, 1):
        token = _token_now(force=attempt == 1)
        status, body = _request(
            "/sql-editor/sqls/run",
            data=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "X-Org-ID": ORG_ID,
                "X-Request-ID": str(uuid.uuid4()),
                "Content-Type": "application/json",
            },
        )
        if attempt == 0 and _token_rejected(status, body):
            continue
        if status != 200:
            raise Upstream(status, body.decode("utf-8", "replace")[:300])
        return json.loads(body)
    raise Upstream(502, "upstream refused the token twice")


def probe() -> dict:
    """Is the engine reachable from here, and does the credential still work?
    Says nothing about where it lives; that is not a caller's business."""
    if not configured():
        raise Unavailable("no upstream configured")
    _token_now(force=True)
    return {"upstream": "reachable", "org_id": ORG_ID}

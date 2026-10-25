"""A Zetaris-compatible SQL endpoint for FloodSense.

This is not Zetaris. Zetaris is a licensed engine each person runs locally,
and the monorepo is explicit that nothing in it may start one. This service
speaks the one call the agents make -- POST /api/v1.0/sql-editor/sqls/run --
over a Postgres holding the same four contract views, so the agents can run
against a real database today and be pointed at the real engine later by
changing one environment variable.

It says what it is on every answer: `engine` in the body and X-Engine on the
response. The agents record that, so a run audited in the app reports which
engine answered rather than claiming it reached Zetaris.

Views served, ported from zetaris/migrations (Urvashi) over ingestion/prep
(Saleeta): hazard_timeline, discharge_daily, exposure_join, provenance.
"""

from __future__ import annotations

import logging
import os
import pathlib
import re
import secrets
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from typing import Any

import psycopg
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi import Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from psycopg.rows import tuple_row
from starlette.concurrency import run_in_threadpool
from psycopg_pool import ConnectionPool

from base64 import b64decode

from app import live, ui, upstream

ENGINE = "floodsense-sql"
# Everything under here is the real engine answering, not this service.
FORWARDED = "/zetaris"
# Hostnames on which every call means the real engine, so the engine's own
# URL works unchanged: swap the host and nothing else.
ENGINE_HOSTS = {
    h.strip().lower()
    for h in os.environ.get("ENGINE_HOST_NAMES", "zetaris.zaindroid.me").split(",")
    if h.strip()
}


def _is_engine_host(request: Request) -> bool:
    """Whether this request is about the real engine rather than this service:
    it either arrived on the engine's own hostname, or it was addressed to the
    forwarded path."""
    if request.scope.get("path", "").startswith(FORWARDED):
        return True
    return (request.headers.get("host") or "").split(":")[0].lower() in ENGINE_HOSTS
VIEWS = ("hazard_timeline", "discharge_daily", "exposure_join", "provenance")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    stream=sys.stdout,
)
log = logging.getLogger("floodsense.sql")


def _require(name: str) -> str:
    """Fail loudly at startup, as the platform contract asks."""
    v = os.environ.get(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} is not set; refusing to start")
    return v


DATABASE_URL = _require("DATABASE_URL")
API_KEY = _require("SQL_API_KEY")
# The write lane. Absent, and nothing but reads can reach the engine through
# here, which is how this service started. It is a second key rather than a
# flag on the first so that handing someone read access stays harmless.
WRITE_KEY = os.environ.get("SQL_WRITE_KEY", "").strip()
READ, WRITE = "read", "write"
SCHEMA = os.environ.get("SQL_SCHEMA", "floodsense_seed")
ROW_CAP = int(os.environ.get("SQL_ROW_CAP", "5000"))
STATEMENT_TIMEOUT_MS = int(os.environ.get("SQL_STATEMENT_TIMEOUT_MS", "20000"))
GIT_SHA = os.environ.get("GIT_SHA", "unknown")
BUILT_AT = os.environ.get("BUILT_AT", "unknown")

pool: ConnectionPool | None = None


# --- the same read-only guard the agents apply before sending -------------

_SELECT_ONLY = re.compile(r"^\s*(select|with)\b", re.I)
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|replace|truncate|attach|detach|"
    r"pragma|vacuum|grant|revoke|reindex|begin|commit|rollback|copy|call|do)\b",
    re.I,
)


def check_sql(sql: str) -> str:
    """Decide whether a caller may run a statement. Mirrors the agents' guard.

    The database role is read-only as well; this is the first of two doors,
    not the only one.
    """
    text = (sql or "").strip()
    while text.endswith(";"):
        text = text[:-1].strip()
    if not text:
        raise ValueError("no statement")
    if not _SELECT_ONLY.match(text):
        raise ValueError("only SELECT / WITH statements may be run")
    if ";" in text:
        raise ValueError("one statement per query; ';' is not allowed")
    if _FORBIDDEN.search(text):
        raise ValueError("only reads are allowed; this statement could write")
    return text


# A migration creates things and refreshes them; it never deletes or edits
# rows. Anything outside that list is refused here rather than argued about
# with the engine, and DROP stays out on purpose: these migrations are
# append-only, and an engine object removed by a bot is not recoverable from
# this side.
_MAY_WRITE = re.compile(r"^\s*(create|refresh|select|with|show|describe|desc)\b", re.I)
_NEVER = re.compile(
    r"\b(drop|delete|truncate|alter|grant|revoke|insert|update)\b",
    re.I,
)


def check_write_sql(sql: str) -> str:
    """The guard for a caller holding the write key."""
    text = (sql or "").strip()
    while text.endswith(";"):
        text = text[:-1].strip()
    if not text:
        raise ValueError("no statement")
    if ";" in text:
        raise ValueError("one statement per query; ';' is not allowed")
    if not _MAY_WRITE.match(text):
        raise ValueError("the write key runs CREATE, REFRESH and reads, nothing else")
    if _NEVER.search(text):
        raise ValueError("dropping, deleting or altering is not available through this endpoint")
    return text


def run_seed() -> None:
    """Build the schemas and the four views. Idempotent: every object is
    CREATE OR REPLACE or DROP ... CASCADE then CREATE."""
    sql = (pathlib.Path(__file__).parent / "seed" / "seed.sql").read_text(encoding="utf-8")
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        conn.execute(sql)
    log.info("seed applied")


def view_counts() -> dict[str, int]:
    out: dict[str, int] = {}
    assert pool is not None
    with pool.connection() as conn:
        for v in VIEWS:
            row = conn.execute(f"SELECT count(*) FROM {SCHEMA}.{v}").fetchone()
            out[v] = int(row[0]) if row else 0
    return out


@asynccontextmanager
async def lifespan(_: FastAPI):
    global pool
    run_seed()
    pool = ConnectionPool(
        DATABASE_URL,
        min_size=1,
        max_size=4,
        kwargs={"row_factory": tuple_row, "options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"},
        open=True,
    )
    log.info("ready: %s", view_counts())
    yield
    pool.close()


app = FastAPI(
    title="FloodSense SQL",
    description="A Zetaris-compatible read-only SQL endpoint over the contract views.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def name_the_engine(request: Request, call_next):
    path = request.url.path
    if path.startswith("/api/v1.0") and _is_engine_host(request):
        # Rewritten before routing, so the engine's own path works as-is.
        request.scope["path"] = path = FORWARDED + path

    response = await call_next(request)
    # X-Engine means this service answered. A reply that was relayed -- the
    # forwarded lane, or the console -- says X-Via instead, so a caller is
    # never told that something came from here when it did not. The console
    # marks itself, because it answers on paths this service does not own.
    if path.startswith(FORWARDED) or "x-via" in response.headers:
        response.headers["X-Via"] = ENGINE
    else:
        response.headers["X-Engine"] = ENGINE
    return response


# --- platform contract ----------------------------------------------------


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> str:
    """Something legible for a person who opens the host in a browser.

    An API-only service answering {"detail":"Not Found"} at its root tells a
    teammate nothing about how to use it.
    """
    engine_host = _is_engine_host(request)
    host = (request.headers.get("host") or "floodsense-sql.zaindroid.me").split(":")[0]
    # Reached by path rather than by hostname: every example has to carry the
    # prefix, or a reader copies a URL that is not the one they opened.
    prefix = FORWARDED if request.scope.get("path", "").startswith(FORWARDED) else ""

    if engine_host:
        # The counts here are this service's own Postgres, which is not what
        # answers on this name, so showing them would mislead.
        rows = "".join(f"<tr><td><code>floodsense.{v}</code></td><td>-</td></tr>" for v in VIEWS)
        title = "FloodSense &rarr; Zetaris"
        lede = ("A public door onto the real Zetaris, which runs on a host with no "
                "public IP and answers only on a private network.")
        note = ("<strong>This is the real engine.</strong> Queries on this name are "
                "forwarded to Zetaris and its answers are passed back untouched, so "
                "they carry no <code>engine</code> field and no <code>X-Engine</code> "
                "-- an agent reading one reports <code>zetaris</code>, which is true. "
                "The hop is in <code>X-Via</code>. The engine's password stays on its "
                "own host; callers present the shared key instead, and only reads get "
                "through.")
    else:
        try:
            counts = view_counts()
        except Exception:
            counts = dict.fromkeys(VIEWS, 0)
        rows = "".join(
            f"<tr><td><code>{SCHEMA}.{v}</code></td><td>{n:,}</td></tr>"
            for v, n in counts.items()
        )
        title = "FloodSense SQL"
        lede = ("A read-only SQL endpoint over the four FloodSense contract views, "
                "speaking the one Zetaris REST call the agents make.")
        note = ("<strong>This is not Zetaris.</strong> Zetaris is a licensed engine each "
                "person runs locally. This serves the same four views over Postgres so "
                "agents can run against a real database now. Every answer carries "
                f"<code>X-Engine: {ENGINE}</code> and an <code>engine</code> field, so a "
                "run can report which engine answered. For the real engine, use "
                '<a href="/zetaris">/zetaris</a>.')
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>FloodSense SQL</title>
<style>
  :root {{ color-scheme: light }}
  body {{ margin:0; background:#fafaf8; color:#1b1f24;
         font:15px/1.6 Archivo,"Helvetica Neue",Arial,sans-serif; }}
  main {{ max-width:62ch; margin:0 auto; padding:48px 24px 72px }}
  h1 {{ font-size:22px; font-weight:500; margin:0 0 6px; letter-spacing:-0.01em }}
  h2 {{ font-size:12px; font-weight:500; color:#5c636c; margin:34px 0 10px;
        border-top:1px solid #1b1f24; padding-top:9px }}
  p {{ margin:0 0 12px; max-width:58ch }}
  .lede {{ color:#5c636c; margin-bottom:26px }}
  table {{ border-collapse:collapse; width:100%; font-size:13.5px }}
  td {{ padding:7px 0; border-top:1px solid #dcdcd6 }}
  td+td {{ text-align:right; font-variant-numeric:tabular-nums }}
  code,pre {{ font:12.5px/1.55 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace }}
  pre {{ background:#f2f2ee; padding:12px 14px; overflow-x:auto; margin:0 0 12px }}
  .note {{ border-left:2px solid #9a6b1f; padding:4px 0 4px 12px; color:#5c636c;
           font-size:13.5px; margin:0 0 12px }}
  a {{ color:#35607a }}
</style></head><body><main>
<h1>{title}</h1>
<p class="lede">{lede}</p>

<p class="note">{note}</p>

<h2>views</h2>
<table>{rows}</table>

<h2>point your agents here</h2>
<pre>ZETARIS_REST_URL=https://{host}{prefix}/api/v1.0
ZETARIS_API_KEY=&lt;ask Zain&gt;
ZETARIS_ORG_ID=1</pre>
<p>Nothing else changes: the agents send the same SQL they send to Zetaris, and
<code>FF_LIVE_DATA</code> keeps its meaning.</p>

<h2>or call it directly</h2>
<pre>curl -X POST https://{host}{prefix}/api/v1.0/sql-editor/sqls/run   -H "Authorization: Bearer $KEY" -H "content-type: application/json"   -d '{{"sql":"SELECT segment, count(*) FROM {SCHEMA}.exposure_join
            WHERE in_footprint GROUP BY segment","limit":20}}'</pre>
<p>Answers <code>{{"headers":[{{"name":…}}],"data":[[…]]}}</code>. Reads only: a statement
that is not a single SELECT or WITH is refused, and the connection is READ ONLY.</p>

<h2>also</h2>
<p><a href="/console">/console</a> — the engine's own console, behind the shared password.
<a href="/views">/views</a> — every view with its columns.
<a href="/zetaris/ready">/zetaris/ready</a> — row counts, 503 if a view is empty.
<a href="/openapi.json">/openapi.json</a> — the spec.</p>
</main></body></html>"""


@app.get("/views")
def views() -> JSONResponse:
    """What is queryable here, so a caller does not have to ask a person."""
    assert pool is not None
    out: dict[str, Any] = {}
    with pool.connection() as conn:
        for v in VIEWS:
            cols = conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (SCHEMA, v),
            ).fetchall()
            n = conn.execute(f"SELECT count(*) FROM {SCHEMA}.{v}").fetchone()
            out[f"{SCHEMA}.{v}"] = {
                "rows": int(n[0]) if n else 0,
                "columns": [{"name": c[0], "type": c[1]} for c in cols],
            }
    return JSONResponse({"engine": ENGINE, "schema": SCHEMA, "views": out})


@app.get("/health")
def health() -> dict:
    """Must not touch the database: a slow query here marks the node unhealthy."""
    return {"status": "ok"}


@app.get("/ready")
def ready() -> JSONResponse:
    try:
        counts = view_counts()
        live_counts, live_built = live_state()
    except Exception as e:  # not ready is an answer, not a crash
        log.warning("not ready: %s", str(e)[:200])
        return JSONResponse({"status": "not ready", "reason": str(e)[:200]}, status_code=503)
    missing = [v for v, n in counts.items() if n == 0]
    if missing:
        return JSONResponse({"status": "not ready", "empty": missing}, status_code=503)
    return JSONResponse(
        {
            "status": "ok",
            "schema": SCHEMA,
            "rows": counts,
            # floodsense.* is built from a live fetch and may not exist yet;
            # its absence is not a reason to call the service unhealthy.
            "live": {"rows": live_counts, "built_at": live_built},
        }
    )


def live_state() -> tuple[dict[str, int], str | None]:
    assert pool is not None
    with pool.connection() as conn:
        at = live.built_at(conn)
        if not at:
            return {}, None
        out = {}
        for v in VIEWS:
            try:
                row = conn.execute(f"SELECT count(*) FROM floodsense.{v}").fetchone()
                out[v] = int(row[0]) if row else 0
            except Exception:
                conn.rollback()
                out[v] = 0
        return out, at


@app.post("/admin/refresh-live")
def refresh_live(authorization: str | None = Header(default=None)) -> dict:
    """Rebuild floodsense.* from Open-Meteo now.

    This is what a federating engine would not need: Zetaris reaches the API
    at query time, this has to be told to go and look.
    """
    _bearer(authorization)
    assert pool is not None
    with pool.connection() as conn:
        conn.read_only = False
        try:
            built = live.build(conn)
        except Exception as e:
            log.warning("live refresh failed: %s", str(e)[:200])
            raise HTTPException(status_code=502, detail=str(e)[:300]) from e
    counts, at = live_state()
    return {"engine": ENGINE, "built": built, "rows": counts, "built_at": at}


@app.get("/version")
def version() -> dict:
    return {"sha": GIT_SHA, "built": BUILT_AT, "engine": ENGINE}


# --- the Zetaris calls the agents make ------------------------------------


def _bearer(authorization: str | None) -> str:
    """Which lane this caller is in, by the key they presented.

    Accepts the key as a bearer token or as the password half of basic auth:
    a Zetaris client given a user and password sends it that way.
    """
    raw = (authorization or "").strip()
    if raw.startswith("Basic "):
        try:
            token = b64decode(raw[6:]).decode("utf-8", "replace").split(":", 1)[1]
        except (ValueError, IndexError):
            token = ""
    else:
        token = raw.removeprefix("Bearer ").strip()
    if WRITE_KEY and token and secrets.compare_digest(token, WRITE_KEY):
        return WRITE
    if token and secrets.compare_digest(token, API_KEY):
        return READ
    raise HTTPException(status_code=401, detail="bad or missing bearer token")


@app.get("/api/v1.0/auth/login")
def login(authorization: str | None = Header(default=None)) -> dict:
    """The agents call this only when no API key is configured. The token it
    returns is the same key, so there is one secret to rotate, not two."""
    _bearer(authorization)
    return {"idToken": API_KEY}


@app.post("/api/v1.0/sql-editor/sqls/run")
def run_sql(
    body: dict,
    authorization: str | None = Header(default=None),
    x_request_id: str | None = Header(default=None),
) -> dict:
    # Reads only, whichever key is presented: the seed lane is this service's
    # own Postgres, and the write key is about the engine.
    _bearer(authorization)
    try:
        sql = check_sql(body.get("sql", ""))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    limit = min(int(body.get("limit") or ROW_CAP), ROW_CAP)
    assert pool is not None
    try:
        with pool.connection() as conn:
            conn.read_only = True
            with conn.cursor() as cur:
                cur.execute(sql)  # already checked: one read-only statement
                cols = [d.name for d in (cur.description or [])]
                rows = cur.fetchmany(limit)
    except psycopg.Error as e:
        log.warning("query failed rid=%s: %s", x_request_id, str(e)[:200])
        raise HTTPException(status_code=400, detail=str(e)[:300]) from e

    log.info("ran rid=%s rows=%d view=%s", x_request_id, len(rows), _view_of(sql))
    return {
        "engine": ENGINE,
        "queryId": body.get("queryId"),
        "headers": [{"name": c} for c in cols],
        "data": [[_json_safe(v) for v in row] for row in rows],
    }


_VIEW = re.compile(r"\b(?:from|join)\s+([A-Za-z_][\w.]*)", re.I)


def _view_of(sql: str) -> str:
    m = _VIEW.search(sql)
    return m.group(1) if m else "?"


def _json_safe(v):
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    if hasattr(v, "isoformat"):  # date
        return v.isoformat()
    return v


# --- the same calls, forwarded to the real engine ------------------------


@app.get(FORWARDED, response_class=HTMLResponse)
@app.get(FORWARDED + "/api/v1.0", response_class=HTMLResponse)
def forwarded_index(request: Request) -> str:
    """The forwarded lane, opened in a browser.

    Everything under this prefix is the engine's own call shape, so the base
    path had no route of its own and answered {"detail":"Not Found"} -- which
    is the first thing a person sees when they are handed the URL to put in
    their environment.
    """
    return index(request)


@app.get("/zetaris/ready")
def zetaris_ready(authorization: str | None = Header(default=None)) -> dict:
    """Whether the engine is reachable from here and the credential still
    works. Where it lives is not a caller's business."""
    _bearer(authorization)
    try:
        return upstream.probe()
    except upstream.Unavailable as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except upstream.Upstream as e:
        raise HTTPException(status_code=502, detail=e.body) from e


@app.get("/zetaris/api/v1.0/auth/login")
def zetaris_login(authorization: str | None = Header(default=None)) -> dict:
    """Callers present their key here too, and get it back as the token.

    The engine's own password never leaves its host. The token handed back is
    the one the caller came with, so a client that logs in and then uses the
    idToken -- zetaris_sync.py does exactly that -- stays in its own lane.
    """
    lane = _bearer(authorization)
    key = WRITE_KEY if lane == WRITE else API_KEY
    return {"idToken": key, "refreshToken": key}


@app.post("/zetaris/api/v1.0/sql-editor/sqls/run")
def zetaris_run(
    body: dict,
    authorization: str | None = Header(default=None),
    x_request_id: str | None = Header(default=None),
) -> dict:
    lane = _bearer(authorization)
    try:
        sql = check_write_sql(body.get("sql", "")) if lane == WRITE else check_sql(body.get("sql", ""))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    limit = min(int(body.get("limit") or ROW_CAP), ROW_CAP)
    try:
        js = upstream.run_sql(sql, limit=limit, query_id=body.get("queryId"))
    except upstream.Unavailable as e:
        log.warning("upstream unreachable rid=%s: %s", x_request_id, e)
        raise HTTPException(status_code=503, detail=str(e)) from e
    except upstream.Upstream as e:
        raise HTTPException(status_code=e.status, detail=e.body) from e

    # The lane is in the line because a write is worth being able to find.
    log.info(
        "forwarded rid=%s lane=%s view=%s rows=%s",
        x_request_id,
        lane,
        _view_of(sql),
        js.get("total"),
    )
    return js


CONSOLE = "/console"


def _console_gate(request: Request) -> bool:
    """Has this browser given the shared password? Basic the first time, the
    cookie after, because the application fetches from the root and browsers
    do not reliably re-send basic credentials for every path of an origin."""
    if not ui.gated():
        return True
    if ui.cookie_ok(request.cookies.get(ui.COOKIE)):
        return True
    return ui.password_ok(request.headers.get("authorization"))


def _ask_for_the_password() -> Response:
    return Response(
        status_code=401,
        content="the console needs the shared password",
        headers={"WWW-Authenticate": ui.REALM},
        media_type="text/plain",
    )


@app.get(CONSOLE)
def console(request: Request) -> Response:
    """Where a person starts. The console itself lives at its own paths, so
    this only takes the password and hands over."""
    if not ui.configured():
        raise HTTPException(status_code=503, detail="no console configured")
    if not _console_gate(request):
        return _ask_for_the_password()
    r = RedirectResponse("/login", status_code=302)
    if not ui.gated():
        return r
    r.set_cookie(
        ui.COOKIE, ui.issue(), max_age=ui.TTL_S, httponly=True, secure=True, samesite="lax"
    )
    return r


# Registered last, so every route above answers for itself and only what is
# left over reaches the console. The console claims the rest of the namespace
# (/login, /_next, /assets) and redirects anything it does not know to its
# own login, which is why it cannot be mounted under a prefix.
@app.api_route(
    "/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
)
async def console_proxy(path: str, request: Request) -> Response:
    if not ui.configured():
        raise HTTPException(status_code=404, detail="not found")
    if not _console_gate(request):
        return _ask_for_the_password()

    # Async so the request body can be read, then the blocking relay runs in
    # the threadpool where every other route in this service already runs.
    body = await request.body()
    try:
        status, headers, payload = await run_in_threadpool(
            ui.forward,
            request.method,
            "/" + path,
            request.url.query,
            list(request.headers.items()),
            body,
        )
    except ui.Unavailable as e:
        log.warning("console unreachable: %s", e)
        raise HTTPException(status_code=502, detail=str(e)) from e

    # Logged because the console's login failure is only visible from here:
    # the engine authenticates, hands the console a session, and the console's
    # next call arrives without it. Names of the cookies each way, never
    # values, and assets are left out so the trail stays readable.
    if not path.startswith(("_next/", "assets/", "favicon", "icon")):
        sent = ",".join(sorted(request.cookies)) or "-"
        got = ",".join(
            sorted(v.split("=", 1)[0].strip() for k, v in headers if k.lower() == "set-cookie")
        ) or "-"
        log.info(
            "console %s /%s -> %s  cookies in=[%s] out=[%s]",
            request.method, path, status, sent, got,
        )

    r = Response(content=payload, status_code=status)
    r.headers["X-Via"] = ENGINE
    # Nothing relayed may rest in a shared cache: the gate is the only thing
    # in front of the console, and a cached reply skips it entirely.
    r.headers["Cache-Control"] = "no-store"
    r.headers["Vary"] = "Authorization, Cookie"
    for k, v in headers:
        if k.lower() == "set-cookie":
            r.raw_headers.append((b"set-cookie", v.encode("latin-1", "replace")))
        else:
            r.headers[k] = v
    return r


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))

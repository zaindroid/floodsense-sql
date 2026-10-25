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

import psycopg
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from psycopg.rows import tuple_row
from psycopg_pool import ConnectionPool

ENGINE = "floodsense-sql"
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
    response = await call_next(request)
    response.headers["X-Engine"] = ENGINE
    return response


# --- platform contract ----------------------------------------------------


@app.get("/health")
def health() -> dict:
    """Must not touch the database: a slow query here marks the node unhealthy."""
    return {"status": "ok"}


@app.get("/ready")
def ready() -> JSONResponse:
    try:
        counts = view_counts()
    except Exception as e:  # not ready is an answer, not a crash
        log.warning("not ready: %s", str(e)[:200])
        return JSONResponse({"status": "not ready", "reason": str(e)[:200]}, status_code=503)
    missing = [v for v, n in counts.items() if n == 0]
    if missing:
        return JSONResponse({"status": "not ready", "empty": missing}, status_code=503)
    return JSONResponse({"status": "ok", "schema": SCHEMA, "rows": counts})


@app.get("/version")
def version() -> dict:
    return {"sha": GIT_SHA, "built": BUILT_AT, "engine": ENGINE}


# --- the Zetaris calls the agents make ------------------------------------


def _bearer(authorization: str | None) -> None:
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not token or not secrets.compare_digest(token, API_KEY):
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))

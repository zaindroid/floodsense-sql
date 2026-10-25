"""The live contract views, fetched rather than frozen.

floodsense_seed.* holds rows copied into a migration once. floodsense.* is
built from the same two Open-Meteo endpoints that Urvashi's Lightning REST
tables wrap, with the same parameters and the same window, fetched when this
runs rather than when the migration was written.

  zetaris/migrations/003  REST table over flood-api  -> discharge_daily (004)
  zetaris/migrations/006  REST table over archive    -> hazard_timeline (007)

What Zetaris does and this does not: Zetaris federates, so a query against
floodsense.hazard_timeline reaches Open-Meteo at query time and nothing is
stored. This materialises the same answer into Postgres and has to be
refreshed. The rows and the view shapes are the same; how they get there is
not. The endpoint that triggers a refresh says so, and /ready reports when
each schema was last built.

Endpoints and parameters are Saleeta's, from ingestion/feeds/feeds.py.
Data: Open-Meteo, CC BY 4.0.
"""

from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from datetime import datetime, timezone

log = logging.getLogger("floodsense.sql.live")

# The eight Valencia points, in the order Urvashi's REST tables request them.
# Lightning returns location_id as the request index, and migration 004 maps
# it back to a point_id by that order, so the order here is part of the
# contract, not a detail. Source: ingestion/prep/valencia_points.csv.
POINTS = [
    ("VAL01", 39.4278, -0.4172),
    ("VAL02", 39.4019, -0.4036),
    ("VAL03", 39.4267, -0.3836),
    ("VAL04", 39.4181, -0.3792),
    ("VAL05", 39.4244, -0.3969),
    ("VAL06", 39.1906, -0.4367),
    ("VAL07", 39.4694, -0.7186),
    ("VAL08", 39.5683, -1.2056),
]

# The window migrations 003 and 006 request: the EMSR773 event and a margin.
START = "2024-10-20"
END = "2024-11-05"

FLOOD_API = "https://flood-api.open-meteo.com/v1/flood"
ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
UA = "FloodSenseEU-hackathon/0.1 (non-commercial)"
TIMEOUT_S = 60


def _get(url: str, params: dict) -> list[dict]:
    """One request for all eight points. Open-Meteo answers a bare object for
    a single coordinate and a list for several; always work with a list."""
    q = urllib.parse.urlencode(params)
    req = urllib.request.Request(f"{url}?{q}", headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:  # noqa: S310 - fixed https host
        body = json.loads(r.read().decode("utf-8"))
    return body if isinstance(body, list) else [body]


def _coords() -> dict:
    return {
        "latitude": ",".join(str(p[1]) for p in POINTS),
        "longitude": ",".join(str(p[2]) for p in POINTS),
        "start_date": START,
        "end_date": END,
        "timezone": "GMT",
    }


def fetch_discharge() -> list[tuple]:
    """Daily river discharge per point. Migration 004's shape."""
    out: list[tuple] = []
    for (point_id, _, _), d in zip(POINTS, _get(FLOOD_API, {**_coords(), "daily": "river_discharge"})):
        daily = d.get("daily") or {}
        for day, value in zip(daily.get("time") or [], daily.get("river_discharge") or []):
            if value is not None:
                out.append((point_id, day, float(value), "open-meteo-flood-glofas"))
    return out


def fetch_rain() -> list[tuple]:
    """Hourly precipitation per point. Migration 007's left side."""
    out: list[tuple] = []
    for (point_id, _, _), d in zip(POINTS, _get(ARCHIVE_API, {**_coords(), "hourly": "precipitation"})):
        hourly = d.get("hourly") or {}
        for stamp, mm in zip(hourly.get("time") or [], hourly.get("precipitation") or []):
            out.append((point_id, stamp.replace("T", " "), float(mm or 0.0)))
    return out


def build(conn) -> dict:
    """Rebuild floodsense.* from a live fetch. Idempotent: the tables are
    replaced whole, inside one transaction, so a failed fetch leaves the
    previous answer in place rather than an empty schema."""
    discharge = fetch_discharge()
    rain = fetch_rain()
    if not discharge or not rain:
        raise RuntimeError(f"empty fetch: {len(discharge)} discharge rows, {len(rain)} rain rows")

    with conn.transaction():
        conn.execute("CREATE SCHEMA IF NOT EXISTS floodsense")
        conn.execute("DROP TABLE IF EXISTS floodsense.discharge_daily_rows CASCADE")
        conn.execute(
            "CREATE TABLE floodsense.discharge_daily_rows "
            "(point_id text, date text, discharge_m3s double precision, source text)"
        )
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO floodsense.discharge_daily_rows VALUES (%s, %s, %s, %s)", discharge
            )
            conn.execute("DROP TABLE IF EXISTS floodsense.rain_hourly_rows CASCADE")
            conn.execute(
                "CREATE TABLE floodsense.rain_hourly_rows "
                "(point_id text, ts_utc text, rain_mm double precision)"
            )
            cur.executemany("INSERT INTO floodsense.rain_hourly_rows VALUES (%s, %s, %s)", rain)

        # migration 004
        conn.execute(
            "CREATE OR REPLACE VIEW floodsense.discharge_daily AS "
            "SELECT point_id, CAST(date AS date) AS date, "
            "CAST(discharge_m3s AS double precision) AS discharge_m3s, source "
            "FROM floodsense.discharge_daily_rows"
        )
        # migration 007: hourly rain joined to that day's discharge, so the
        # daily value repeats across the 24 hours of its day.
        conn.execute(
            "CREATE OR REPLACE VIEW floodsense.hazard_timeline AS "
            "SELECT r.point_id, CAST(r.ts_utc AS timestamp) AS ts_utc, "
            "CAST(r.rain_mm AS double precision) AS rain_mm, "
            "d.discharge_m3s, CAST(NULL AS text) AS warning_level "
            "FROM floodsense.rain_hourly_rows r "
            "LEFT JOIN floodsense.discharge_daily d "
            "  ON d.point_id = r.point_id AND d.date = CAST(r.ts_utc AS date)"
        )
        # The portfolio, the sites and the provenance do not vary with time,
        # so the live container shows the same rows as the seed one rather
        # than a second copy that could drift.
        conn.execute(
            "CREATE OR REPLACE VIEW floodsense.exposure_join AS "
            "SELECT * FROM floodsense_seed.exposure_join"
        )
        conn.execute(
            "CREATE OR REPLACE VIEW floodsense.provenance AS "
            "SELECT * FROM floodsense_seed.provenance"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS floodsense.built_at "
            "(schema_name text primary key, at timestamptz)"
        )
        conn.execute(
            "INSERT INTO floodsense.built_at VALUES ('floodsense', %s) "
            "ON CONFLICT (schema_name) DO UPDATE SET at = EXCLUDED.at",
            (datetime.now(timezone.utc),),
        )

    log.info("live rebuilt: %d discharge rows, %d rain rows", len(discharge), len(rain))
    return {"discharge_rows": len(discharge), "rain_rows": len(rain), "window": [START, END]}


def built_at(conn) -> str | None:
    try:
        row = conn.execute(
            "SELECT at FROM floodsense.built_at WHERE schema_name = 'floodsense'"
        ).fetchone()
    except Exception:
        return None
    return row[0].isoformat() if row and row[0] else None

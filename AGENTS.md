# Using the shared Zetaris from an agent

Paste this file into your agent's context. Ask Zain for the two keys; they are
not in this repository.

## What this is

One Zetaris engine, shared by the team, reachable over the internet through a
gateway. The gateway speaks the shape Zetaris speaks, so the team's own
`scripts/zetaris_sync.py` works against it. The engine's admin password stays
on its host and is never handed out.

Zetaris federates rather than stores: a query on a `floodsense.*` view calls
the source API at that moment. Adding data therefore means declaring a table
or a view, not inserting rows.

```
Endpoint: https://floodsense-sql.zaindroid.me/zetaris/api/v1.0
Auth:     Authorization: Bearer <key>
```

## Two keys

| | read key | write key |
| --- | --- | --- |
| `SELECT`, `WITH`, `SHOW`, `DESCRIBE` | yes | yes |
| `CREATE` (views, REST tables, FILESTORE tables) | no | yes |
| `REFRESH TABLE` | no | yes |
| `DROP`, `DELETE`, `TRUNCATE`, `ALTER`, `GRANT`, `REVOKE`, `INSERT`, `UPDATE` | no | no |

Nothing can be dropped or altered through the gateway, whichever key is used.
The migrations are append-only by the team's own rule, and an engine object a
bot removes cannot be restored from this side. If something must go, ask Zain,
who has host access.

Use the read key wherever reading is all you do. It is the one that can be
shared without consequences.

## Reading

One statement per call, and a fresh UUID per call: the engine rejects a
repeated `queryId`.

```bash
curl -X POST https://floodsense-sql.zaindroid.me/zetaris/api/v1.0/sql-editor/sqls/run \
  -H "Authorization: Bearer $ZETARIS_READ_KEY" \
  -H "content-type: application/json" \
  -d '{"sql":"SELECT segment, count(*) FROM floodsense.exposure_join GROUP BY segment",
       "queryId":"'"$(uuidgen)"'","limit":100}'
```

Every value comes back as a string, including numbers and timestamps. Cast on
read.

### The views

`floodsense.*` is live federation. `floodsense_seed.*` is the frozen offline
copy with the same columns, for working without network.

| View | Rows | What |
| --- | --- | --- |
| `hazard_timeline` | 3,264 | hourly rain and daily discharge, 20 Oct – 5 Nov 2024, the EMSR773 window |
| `hazard_now` | 1,152 | the same five columns on a rolling window: three days back, three forward |
| `discharge_daily` | 136 | daily river discharge per point |
| `exposure_join` | 359 | policies against the footprint. `synthetic = 'yes'`: not a real book |
| `provenance` | 7 | the cleared datasets, their licences and attribution |

A reply built on `floodsense_seed.*`, or on `hazard_timeline`, is answering
about the 2024 event, not about today. Say so. Never present it as live.

## Adding data

Write a migration; do not improvise statements against the engine. The
engine's state should be reproducible from `zetaris/migrations/` alone, and a
view made by hand in a SQL editor exists nowhere in the repository.

1. Add the next numbered file in `zetaris/migrations/`. Append only; never
   edit a merged file.
2. A file that creates a view opens with `-- view:`, `-- columns:` and
   `-- min_rows:`, and every `floodsense.<name>` needs a `floodsense_seed.<name>`
   with identical columns plus an entry in `contracts/views.yaml`.
   `scripts/tests/test_migration_contract.py` enforces all of this.
3. Separate statements within a file by a line reading exactly `-- statement`.
4. Apply and verify:

```bash
export ZETARIS_REST_URL=https://floodsense-sql.zaindroid.me/zetaris/api/v1.0
export ZETARIS_API_KEY=<the write key>
python zsync.py sync      # applies what is pending, then verifies
python zsync.py verify    # checks every declared view
```

`sync` skips what already exists, so running it twice is safe. The ledger of
what has been applied is per-machine, so the first run on a new laptop
re-sends every migration and skips its way through.

### zsync.py

Cloudflare answers the default `Python-urllib` user agent with 403 error 1010,
so `zetaris_sync.py` cannot reach this host directly. `urlopen` honours a
globally installed opener, so naming the client is enough. Save this beside
the repository root and run it in place of the script:

```python
"""Run zetaris_sync against the Cloudflare-fronted gateway."""
import sys
import urllib.request
from pathlib import Path

opener = urllib.request.build_opener()
opener.addheaders = [("User-Agent", "floodsense-sync/1.0")]
urllib.request.install_opener(opener)

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))
import zetaris_sync

raise SystemExit(zetaris_sync.main())
```

Any client that sets a user agent of its own -- curl, requests, httpx -- needs
none of this.

## Limits

- One statement per call. A second one after a `;` is refused.
- 5,000 rows and 20 seconds per statement.
- A REST-federated view cannot be self-joined.
- `DROP` is unavailable through SQL on this engine at all, by Lightning's own
  object rules, not only by this gateway's.

## Keys are not committed

They go in your environment or your agent's secret store, never in a file that
is pushed. The read key can be rotated without coordination; rotating the write
key means telling whoever is syncing.

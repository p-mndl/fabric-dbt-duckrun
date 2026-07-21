# PR-Material für djouallah/duckrun#16 (Branch `perf-16-bulk-discovery` in C:\git\duckrun)

Der Fix ist implementiert, getestet und committet (Commit `perf: cross-schema discovery
prefetch + concurrent delta_scan view binds (#16)`). Zum Einreichen:

```powershell
# 1. Fork anlegen (einmalig): https://github.com/djouallah/duckrun -> "Fork"
#    (oder: gh repo fork djouallah/duckrun --clone=false)
cd C:\git\duckrun
git remote add fork https://github.com/p-mndl/duckrun.git
git push -u fork perf-16-bulk-discovery
# 2. PR eröffnen: base djouallah/duckrun@main, head p-mndl:perf-16-bulk-discovery
#    Titel + Body von unten übernehmen.
```

---

## PR-Titel

```
perf: cross-schema discovery prefetch + concurrent delta_scan view binds (#16)
```

## PR-Body

Follow-up to #16 — thanks for the 0.4.28 fix! It helped, but on my multi-schema project
`dbt show` was still slow (see the debug log I attached in the issue). I dug into where the
remaining time goes and this PR addresses it. Numbers below are from the same project as the
issue report (~80 tables over 8 schemas across two catalogs, residential connection,
Windows 11, dbt-core 1.11.12).

### What was still slow after 0.4.28

Two things, both visible as silent gaps in a `--log-level-file debug` log (the work runs on
raw cursors, so it never shows up as SQL events):

1. **The per-schema work still runs schema-by-schema.** dbt populates its relation cache by
   listing every manifest schema, serially on duckrun's single thread. The 0.4.28 pool makes
   the opens concurrent *within* one schema, but an 8-schema project still pays 8 sequential
   rounds of (REST listing → open pool → view loop).
2. **The `delta_scan` view registrations are serial, and each one replays the log again.**
   `CREATE OR REPLACE VIEW … AS SELECT * FROM delta_scan(…)` binds at creation — DuckDB's
   delta extension does a full log read to resolve the view's types, through its own HTTP
   stack with no metadata cache shared with delta-rs. Verified: `CREATE VIEW` over a
   nonexistent path fails immediately with `DeltaKernel InvalidTableLocationError`, so the
   bind provably happens at CREATE. In my log this loop was the bigger half of each schema's
   cost (up to ~25s for the elementary schema in the 90s run from the issue).

Gap profile of a baseline run (0.4.29, 27.9s total): per schema ~1–2s in the open pool, then
~3–4.5s silent in the serial view loop.

### What this PR changes

- **One cross-schema prefetch per cache population.** `_relations_cache_for_schemas` is the
  one place dbt announces the whole burst up front, so discovery now runs there: list every
  schema, then open **all** tables' logs in ONE pool, then bind **all** views in ONE pool,
  and let each `list_relations_without_caching` call consume its precomputed slice. The
  prefetch is cleared in `finally`, so nothing is ever served stale to a later call; direct
  `list_relations` calls outside a burst keep the previous per-schema path (now with
  concurrent view binds too), and a prefetch failure falls back to that path
  (`OneLakeAccessError` still fails loud).
- **Concurrent view binds on raw child cursors.** Each worker gets its own
  `conn.cursor()` of the shared DuckDB database — catalog, attached databases and
  (temporary) secrets are instance-global, and concurrent DDL on distinct view names doesn't
  conflict (tested). The wrapped shared cursor never crosses a thread.
- **Pool workers 8 → 32** (`engine.POOL_WORKERS`, shared by the open pool and the bind
  pool). The work is latency-bound — a handful of sequential HTTPS round trips per table at
  30–100ms+ from outside Azure — so the cap sets how many waves a project-wide discovery
  pays: ~80 tables at 8 workers was 10 waves, at 32 it's 3.

Semantics are unchanged: same tombstone hiding (drop-tombstones never surface, including the
DuckDB-side fallback probe when delta-rs can't open a table), same persisted-docs re-apply
(so `dbt docs generate` still shows them), same best-effort error handling, discovered Delta
tables still reported table-typed so `is_incremental()` survives fresh processes.

### Numbers

`dbt show -s <one small model>` on the project above, warm parse, 2nd run of each variant:

| | wall clock | adapter discovery share |
|---|---|---|
| duckrun 0.4.29 | 27.9s | ~20s (gaps across 8 schemas) |
| this PR | **8.3s** | **~2s** (one batch) |

The remaining ~6s is dbt parse + connection open, not discovery. Inside a Fabric notebook the
effect will be smaller (low latency to OneLake), but the round-trip count drops the same way.

### Tests

- New: `test_show_discovery_batches_across_schemas_and_binds_views_concurrently` — a real
  `dbt show` over two physical schemas asserts ONE cross-schema open batch and every view
  bind off the main thread.
- The existing #16 test (opens concurrent, schema created once) and the tombstone/docs tests
  pass unchanged on the new path.
- Full `tests/adapter` + `tests/connection_api`: 456 passed, 1 skipped (live OneLake test
  without token).

---

## Kommentar-Entwurf für Issue #16 (als p-mndl zu posten)

```
Reported back with data :-) I profiled where the remaining time goes after 0.4.28: the
per-schema work still runs schema-by-schema (dbt lists every manifest schema serially on
duckrun's single thread), and the delta_scan view registrations are serial — each CREATE
VIEW binds at creation, which replays the table's Delta log a second time through DuckDB's
delta extension (no cache shared with delta-rs). The listing itself is cheap (one REST call
per schema) — it's the per-table log replays that dominate, so a OneLake table-listing API
alone wouldn't move much.

I've opened #<PR-NUMMER> which batches the whole cache population into one cross-schema
pass (one open pool, one view-bind pool, 32 workers): on my project `dbt show` went from
~28s to ~8s, with the adapter's share dropping from ~20s to ~2s. The rest is dbt parse +
connection open.
```

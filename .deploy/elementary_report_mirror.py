"""Build a local DuckDB mirror of the elementary schema so `edr report` can run.

duckrun materializes dbt `view` models only inside the invocation's in-memory DuckDB
catalog -- OneLake Lakehouses have no view concept, so elementary's report views never
persist and edr's queries against them fail. The underlying data does persist (all
elementary base tables are Delta in LH_Gold/Tables/elementary), so this script rebuilds
the complete schema locally: base tables are copied from OneLake via delta-rs, views are
recreated from dbt's compiled SQL (target/compiled, produced by the preceding dbt run)
in manifest dependency order. edr then reads the mirror through a plain duckdb profile
(profiles.yml, profile "elementary").

Usage:
    python .deploy/elementary_report_mirror.py
    (needs WORKSPACE_ID / LH_GOLD_ID / FABRIC_STORAGE_TOKEN env vars, i.e. a terminal
    opened through .vscode/terminal-init.ps1, and a prior `dbt run`/`dbt build`)

Output: dbt/target/elementary_mirror.duckdb (derived state, safe to delete)
"""

import json
import os
from pathlib import Path

import duckdb
from deltalake import DeltaTable

ROOT = Path(__file__).parent.parent
PROJ = ROOT / "dbt"
MIRROR = PROJ / "target" / "elementary_mirror.duckdb"

manifest = json.loads((PROJ / "target" / "manifest.json").read_text(encoding="utf-8"))
nodes = {
    uid: n
    for uid, n in manifest["nodes"].items()
    if n["package_name"] == "elementary" and n["resource_type"] == "model"
}

base = (
    f"abfss://{os.environ['WORKSPACE_ID']}@onelake.dfs.fabric.microsoft.com/"
    f"{os.environ['LH_GOLD_ID']}/Tables/elementary"
)
storage = {"bearer_token": os.environ["FABRIC_STORAGE_TOKEN"]}

MIRROR.unlink(missing_ok=True)
con = duckdb.connect(str(MIRROR))
con.execute("create schema if not exists elementary")

tables = {u: n for u, n in nodes.items() if n["config"]["materialized"] != "view"}
views = {u: n for u, n in nodes.items() if n["config"]["materialized"] == "view"}

for n in tables.values():
    name = n.get("alias") or n["name"]
    arrow = DeltaTable(f"{base}/{name}", storage_options=storage).to_pyarrow_table()
    con.register("_src", arrow)
    con.execute(f'create table elementary."{name}" as select * from _src')
    con.unregister("_src")
print(f"{len(tables)} tables mirrored from OneLake")

# Views can reference each other -> create in dependency order (Kahn).
pending = dict(views)
created = 0
while pending:
    progressed = False
    for uid, n in list(pending.items()):
        deps = set(n["depends_on"]["nodes"])
        if deps & set(pending) - {uid}:
            continue
        compiled = PROJ / "target" / "compiled" / "elementary" / n["original_file_path"]
        # compiled refs are fully qualified against duckrun's in-memory catalog; in the
        # mirror file the schema lives in the file's own catalog, so drop the prefix.
        sql = compiled.read_text(encoding="utf-8").replace('"memory".', "")
        name = n.get("alias") or n["name"]
        con.execute(f'create or replace view elementary."{name}" as {sql}')
        del pending[uid]
        created += 1
        progressed = True
    if not progressed:
        raise RuntimeError(f"circular/unresolvable view deps: {list(pending)}")
print(f"{created} views created from compiled SQL -> {MIRROR}")

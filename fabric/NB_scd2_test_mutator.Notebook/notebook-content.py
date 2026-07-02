# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   }
# META }

# CELL ********************

# Disposable test scaffolding for the dbt-workflow-validation phase (see README "Offene Punkte"):
# writes/mutates fake rows in LH_Bronze so `dbt snapshot` has something real to version (SCD2).
# Triggered manually from the Fabric UI before a test cycle — NOT wired into PL_Orchestration,
# since this data has no relationship to any real domain.
#
# No duckdb/duckrun here (this notebook never runs dbt) — only the Delta write library duckrun
# itself uses (delta_rs), so no kernel-preloaded-version conflict and no restartPython() needed.
%pip install -q deltalake pandas pyarrow

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import notebookutils
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

vl = notebookutils.variableLibrary.getLibrary("VL")
WORKSPACE_ID = vl.workspace_id
LH_BRONZE_ID = vl.lh_bronze

STORAGE_TOKEN = notebookutils.credentials.getToken("storage")
storage_options = {"bearer_token": STORAGE_TOKEN}

TABLE_PATH = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
    f"{LH_BRONZE_ID}/Tables/dbo/scd2_test_source"
)

# Every N mutation runs, the oldest surviving "insert-fodder" row (id >= 3) is deleted instead of
# only mutated/inserted — gives the snapshot a hard-delete case to catch (invalidate_hard_deletes).
DELETE_EVERY_N_RUNS = 5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Deterministic, state-based mutation: every run's effect is derived from the table's own current
# content (max id), not randomness or external counters — so a given run's output is predictable
# and the resulting snapshot history can be checked against what SHOULD have happened.
#
# Fixed rows (created once, never removed) carry the two SCD2 test signals `check_cols` needs to
# prove itself against:
#   id=1  price/category change every run  -> must produce a NEW snapshot version each time
#   id=2  only `notes` changes every run   -> must NOT produce a new version (notes isn't tracked)
# Every run also inserts one new row (id 3, 4, 5, ...), and every DELETE_EVERY_N_RUNS-th run
# deletes the oldest surviving one of those (never id=1/2) to exercise a hard delete.
try:
    existing = DeltaTable(TABLE_PATH, storage_options=storage_options).to_pandas()
except TableNotFoundError:
    genesis = pd.DataFrame(
        [
            {"id": 1, "price": 100.0, "category": "A", "notes": "genesis"},
            {"id": 2, "price": 200.0, "category": "B", "notes": "genesis"},
        ]
    )
    write_deltalake(TABLE_PATH, genesis, mode="overwrite", storage_options=storage_options)
    print(f"genesis: created {TABLE_PATH} with rows id=1,2")
else:
    max_id = int(existing["id"].max())
    run_number = max_id - 2 + 1  # rows 3..max_id are past inserts -> that many runs already happened

    rows = existing.set_index("id", drop=False).to_dict("index")

    # id=1: tracked columns change -> dbt must see a new version.
    rows[1]["price"] = round(100.0 + run_number * 1.5, 2)
    rows[1]["category"] = ["A", "B", "C"][run_number % 3]

    # id=2: only the untracked column changes -> dbt must NOT see a new version.
    rows[2]["notes"] = f"note-run-{run_number}"

    # Periodic hard delete: the oldest insert-fodder row (id >= 3), never the fixed rows.
    if run_number % DELETE_EVERY_N_RUNS == 0:
        fodder_ids = sorted(i for i in rows if i >= 3)
        if fodder_ids:
            deleted_id = fodder_ids[0]
            del rows[deleted_id]
            print(f"run {run_number}: deleted id={deleted_id}")

    # New row for this run.
    next_id = max_id + 1
    rows[next_id] = {
        "id": next_id,
        "price": round(next_id * 10.0, 2),
        "category": ["A", "B", "C"][next_id % 3],
        "notes": "inserted",
    }

    updated = pd.DataFrame(sorted(rows.values(), key=lambda r: r["id"]))
    write_deltalake(
        TABLE_PATH, updated, mode="overwrite", schema_mode="overwrite",
        storage_options=storage_options,
    )
    print(f"run {run_number}: id=1 -> {rows[1]}, id=2.notes -> {rows[2]['notes']!r}, inserted id={next_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

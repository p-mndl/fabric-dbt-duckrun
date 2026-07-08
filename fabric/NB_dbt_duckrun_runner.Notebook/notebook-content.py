# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   }
# META }

# CELL ********************

# Prerequisites: dbt files must be deployed to LH_Gold/Files/dbt_duckrun/ before running
# (see .deploy/deploy_dbt_files.py). No default lakehouse is attached to this notebook — the dbt project is
# downloaded from OneLake via abfss/SDK in the next cell instead of relying on a
# /lakehouse/default/ mount.
#
# No `[local]` extra here (that pulls azure-identity for the local AzureCliCredential path) —
# in Fabric the storage token comes from notebookutils instead.
# Pinned so Fabric runs the same adapter version as local dev/CI — keep in sync with
# requirements.txt (an unpinned install once shipped a silent behavior change, duckrun#8).

%pip install -q duckrun==0.3.37

# The Fabric kernel image ships with duckdb 1.4.4 already loaded; duckrun needs >=1.5.4. The
# pip install above puts the newer version on disk, but the running kernel process still has
# the old one in memory — a session restart is required to actually pick it up (this is
# duckrun's own documented fix for the "needs a newer duckdb" error in Fabric notebooks).
import notebookutils
notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

# Defaults for standalone execution. A Fabric Data Pipeline overrides these per activity,
# allowing domain-specific pipelines (e.g. PL_dbt_hr, PL_dbt_erp) to control what runs
# without separate notebook files.
dbt_command = "run"      # dbt subcommand: "build" (run+test+snapshot in dep-order), "run", "test", "snapshot", "source freshness"
dbt_select = ""          # --select value; empty string = all models
dbt_full_refresh = "false"  # "true" passes --full-refresh (forces full rebuild of incremental models)
dbt_vars = "{}"          # JSON string passed as --vars, e.g. '{"country": "Germany"}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import os
import tempfile
import notebookutils
from azure.core.credentials import AccessToken, TokenCredential
from azure.storage.filedatalake import DataLakeServiceClient

vl = notebookutils.variableLibrary.getLibrary("VL")
WORKSPACE_ID = vl.workspace_id
LH_GOLD_ID = vl.lh_gold

DBT_PROJECT_DIR = os.path.join(tempfile.gettempdir(), "dbt_duckrun")

# Fetch the storage token explicitly via notebookutils instead of relying on a credential_chain
# that would try (and fail) to reach IMDS from here.
STORAGE_TOKEN = notebookutils.credentials.getToken("storage")

# profiles.yml / sources.yml resolve every GUID from env vars; here they come from the
# workspace's active Variable Library value set (locally the terminal profile sets the same
# vars from the value set named in .dev-env).
os.environ["FABRIC_STORAGE_TOKEN"] = STORAGE_TOKEN
os.environ["WORKSPACE_ID"] = WORKSPACE_ID
os.environ["LH_GOLD_ID"] = LH_GOLD_ID
os.environ["LH_SILVER_ID"] = vl.lh_silver
os.environ["SRC_WORKSPACE_ID"] = vl.src_workspace_id
os.environ["LH_BRONZE_ID"] = vl.lh_bronze


class _StaticToken(TokenCredential):
    def get_token(self, *_, **__):
        return AccessToken(STORAGE_TOKEN, 9999999999)


def download_dbt_from_onelake(lakehouse_id, local_dir):
    """Mirror LH_Gold/Files/dbt_duckrun from OneLake into a local dir, no lakehouse mount needed."""
    fs = DataLakeServiceClient(
        "https://onelake.dfs.fabric.microsoft.com",
        credential=_StaticToken(),
    ).get_file_system_client(WORKSPACE_ID)
    rel = f"{lakehouse_id}/Files/dbt_duckrun"
    for p in fs.get_paths(path=rel, recursive=True):
        if p.is_directory:
            continue
        suffix = os.path.relpath(p.name, rel)
        target = os.path.join(local_dir, suffix)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as fh:
            fh.write(fs.get_file_client(p.name).download_file().readall())


download_dbt_from_onelake(LH_GOLD_ID, DBT_PROJECT_DIR)

print(f"dbt project : {DBT_PROJECT_DIR}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
import re

from dbt.cli.main import dbtRunner, dbtRunnerResult

runner = dbtRunner()

# dbt packages (elementary) are not part of the deploy: dbt_packages/ is gitignored and
# deploy_dbt_files.py copies git-tracked files only. Install them here instead —
# package-lock.yml IS deployed, so this resolves to the exact pinned versions.
deps_result: dbtRunnerResult = runner.invoke(
    ["deps", "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR]
)
if not deps_result.success:
    raise RuntimeError("dbt deps failed — check the log output above for details")

# No --target flag: the downloaded profiles.yml has a single target whose GUIDs were already
# rewritten for this environment at deploy time (see .deploy/deploy_dbt_files.py).
args = [
    dbt_command,
    "--project-dir", DBT_PROJECT_DIR,
    "--profiles-dir", DBT_PROJECT_DIR,
]

if dbt_select:
    args += ["--select", dbt_select]

if str(dbt_full_refresh).lower() == "true":
    args.append("--full-refresh")

if dbt_vars and dbt_vars.strip() not in ("", "{}"):
    args += ["--vars", dbt_vars]

print(f"Running: dbt {' '.join(str(a) for a in args)}")

result: dbtRunnerResult = runner.invoke(args)

# --- Classify the outcome for the pipeline ------------------------------------------------
# dbt failures no longer raise here: an exception would fail the notebook activity before the
# pipeline could read a structured exitValue. Instead the outcome is classified and handed
# over via notebookutils.notebook.exit(); PL_Orchestration's Switch turns the status into
# alerts plus an explicit Fail activity (so failed runs still turn the pipeline red). Hard
# crashes — deps above, token expiry, or anything that leaves no node results — still raise
# and are caught by the notebook activity's on-failure path as the fallback alert.

if result.exception is not None:
    raise result.exception

node_results = list(getattr(result.result, "results", None) or [])
if not result.success and not node_results:
    raise RuntimeError(f"dbt {dbt_command} failed before executing any node — check the log output above")

errors, fails, warns = [], [], []
skipped = 0
for r in node_results:
    status = str(r.status)
    node = getattr(r, "node", None)
    entry = {
        "unique_id": getattr(node, "unique_id", "<unknown>"),
        "name": getattr(node, "name", "<unknown>"),
        "resource_type": str(getattr(node, "resource_type", "")),
        "status": status,
        "message": (r.message or "")[:300],
    }
    if status in ("error", "runtime error"):
        # Includes tests that ERROR (broken test SQL): that's infrastructure, not data
        # quality, so it escalates with the models.
        errors.append(entry)
    elif status == "fail":
        fails.append(entry)
    elif status == "warn":
        warns.append(entry)
    elif status == "skipped":
        skipped += 1

if errors:
    overall = "models_failed"
elif fails:
    overall = "tests_failed"
elif warns:
    overall = "tests_warned"
else:
    overall = "success"

if not result.success and overall not in ("models_failed", "tests_failed"):
    raise RuntimeError(
        f"dbt {dbt_command} reported failure but no node classified as failed — check the log above"
    )

# --- Failing-row samples from elementary ---------------------------------------------------

SAMPLE_TESTS = 3  # exitValue is a size-limited string: cap enrichment hard
SAMPLE_ROWS = 5


def _elementary_failure_samples(failed_test_ids):
    """One-off read of elementary's Delta tables right after the run, in the same session —
    deliberately NOT a Fabric Activator listening on the table. Names verified against
    elementary 0.25.x: samples live in test_result_rows (store_result_rows_in_own_table
    defaults to true), linked to elementary_test_results via its id."""
    from deltalake import DeltaTable

    with open(os.path.join(DBT_PROJECT_DIR, "target", "run_results.json"), encoding="utf-8") as fh:
        invocation_id = json.load(fh)["metadata"]["invocation_id"]

    base = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_GOLD_ID}/Tables/elementary"
    storage_options = {"bearer_token": STORAGE_TOKEN}

    # One row per test result / sample row -> column-pruned full reads are fine at this scale.
    test_results = (
        DeltaTable(f"{base}/elementary_test_results", storage_options=storage_options)
        .to_pyarrow_table(columns=["id", "invocation_id", "test_unique_id"])
        .to_pylist()
    )
    id_to_test = {
        row["id"]: row["test_unique_id"]
        for row in test_results
        if row["invocation_id"] == invocation_id and row["test_unique_id"] in failed_test_ids
    }
    if not id_to_test:
        return {}

    result_rows = (
        DeltaTable(f"{base}/test_result_rows", storage_options=storage_options)
        .to_pyarrow_table(columns=["elementary_test_results_id", "result_row"])
        .to_pylist()
    )
    samples = {}
    for row in result_rows:
        test_id = id_to_test.get(row["elementary_test_results_id"])
        if test_id is None or (test_id not in samples and len(samples) >= SAMPLE_TESTS):
            continue
        bucket = samples.setdefault(test_id, [])
        if len(bucket) < SAMPLE_ROWS:
            bucket.append(str(row["result_row"])[:200])
    return samples


samples, samples_note = {}, ""
if fails:
    try:
        samples = _elementary_failure_samples({f["unique_id"] for f in fails})
    except Exception as exc:  # enrichment must never kill the alert itself
        # collapse ANSI-colored, multi-line library messages into one plain line
        cleaned = " ".join(re.sub(r"\x1b\[[0-9;]*m", "", str(exc)).split())
        samples_note = "failing-row samples unavailable: " + cleaned[:200]

# --- Compose the alert and hand over to the pipeline ---------------------------------------

icon = {"models_failed": "🔴", "tests_failed": "🟡", "tests_warned": "⚪", "success": "✅"}[overall]
lines = [
    f"{icon} dbt {dbt_command} — {overall.replace('_', ' ')} "
    f"(errors: {len(errors)}, test failures: {len(fails)}, warnings: {len(warns)}, skipped: {skipped})"
]
if dbt_select:
    lines.append(f"selection: {dbt_select}")
for entry in (errors + fails + warns)[:10]:
    lines.append(f"- **{entry['name']}** ({entry['resource_type']}, {entry['status']}): {entry['message']}")
    for sample in samples.get(entry["unique_id"], []):
        lines.append(f"    · {sample}")
if samples_note:
    lines.append(samples_note)

alert_message = "\n".join(lines)
if len(alert_message) > 3500:
    alert_message = alert_message[:3500] + "… (truncated)"

payload = {
    "status": overall,
    "counts": {
        "error": len(errors),
        "fail": len(fails),
        "warn": len(warns),
        "skipped": skipped,
        "total": len(node_results),
    },
    "dbt_command": dbt_command,
    "dbt_select": dbt_select,
    "alert_message": alert_message,
}

exit_value = json.dumps(payload, ensure_ascii=False)
print(f"dbt {dbt_command} finished with status '{overall}'")
print(exit_value)
notebookutils.notebook.exit(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

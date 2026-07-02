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

%pip install -q duckrun

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

from dbt.cli.main import dbtRunner, dbtRunnerResult

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

runner = dbtRunner()
result: dbtRunnerResult = runner.invoke(args)

if not result.success:
    raise RuntimeError(f"dbt {dbt_command} failed — check the log output above for details")

print(f"dbt {dbt_command} completed successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

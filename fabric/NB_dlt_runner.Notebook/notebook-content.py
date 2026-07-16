# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   }
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "vCores": 2
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# %%configure above pins the session to the smallest Python-notebook size (2 vCores / 16 GB —
# REST extraction is I/O-bound, that's plenty); it's a cell magic, so it must stand alone as
# the first cell.
#
# Generic dlt ingestion runner — the EL counterpart to NB_dbt_duckrun_runner. The pipeline
# definition (which API, which resources, incremental cursors) is NOT in this notebook: it
# lives as a declarative YAML config in the repo's ingest/ folder, deployed to
# LH_Gold/Files/dlt_ingest/ (deploy --project ingest) and downloaded in the next cell.
# This notebook only authenticates, downloads the config, and runs it.
#
# Pinned so Fabric runs the same dlt version the config was written against. [az] = adlfs
# for dlt's filesystem operations, [deltalake] = delta-rs writer for Delta output.

%pip install -q "dlt[az,deltalake]==1.29.0"

# The pip install upgrades pyarrow/deltalake, which the Fabric kernel image ships in older
# versions; a session restart makes sure the upgraded versions are actually loaded (same
# reasoning as the duckdb restart in NB_dbt_duckrun_runner).
import notebookutils
notebookutils.session.restartPython()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

# Defaults for standalone execution. A Fabric Data Pipeline can override these per activity,
# same pattern as the dbt runner.
dlt_config = "github_issues.yml"  # config file under LH_Gold/Files/dlt_ingest/ to run
dlt_resources = ""                # comma-separated resource names; empty = all resources
dlt_full_refresh = "false"        # "true" drops state + tables of this source and reloads from scratch

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

# dlt phones home by default; the run reports below carry everything we need.
os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"

vl = notebookutils.variableLibrary.getLibrary("VL")
WORKSPACE_ID = vl.workspace_id
LH_GOLD_ID = vl.lh_gold

# Ingestion writes to the bronze that dbt's sources READ (src_workspace_id/lh_bronze): in the
# single-workspace setup that is this workspace's own LH_Bronze. Team caveat: developer value
# sets usually point the read side at the shared TEST bronze — running this notebook with such
# a value set would WRITE into shared bronze. Ingestion is an environment-level concern; in
# team setups run it only in the workspace that owns the bronze data.
SRC_WORKSPACE_ID = vl.src_workspace_id
LH_BRONZE_ID = vl.lh_bronze

DLT_CONFIG_DIR = os.path.join(tempfile.gettempdir(), "dlt_ingest")

# Fetch the storage token explicitly via notebookutils instead of relying on a credential
# chain that would try (and fail) to reach IMDS from here. The token is OneLake-wide: the
# same one downloads the config from Gold and writes the Delta output to Bronze.
STORAGE_TOKEN = notebookutils.credentials.getToken("storage")


class _StaticToken(TokenCredential):
    def get_token(self, *_, **__):
        return AccessToken(STORAGE_TOKEN, 9999999999)


def download_dlt_config_from_onelake(lakehouse_id, local_dir):
    """Mirror LH_Gold/Files/dlt_ingest from OneLake into a local dir, no lakehouse mount needed."""
    fs = DataLakeServiceClient(
        "https://onelake.dfs.fabric.microsoft.com",
        credential=_StaticToken(),
    ).get_file_system_client(WORKSPACE_ID)
    rel = f"{lakehouse_id}/Files/dlt_ingest"
    for p in fs.get_paths(path=rel, recursive=True):
        if p.is_directory:
            continue
        suffix = os.path.relpath(p.name, rel)
        target = os.path.join(local_dir, suffix)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as fh:
            fh.write(fs.get_file_client(p.name).download_file().readall())


download_dlt_config_from_onelake(LH_GOLD_ID, DLT_CONFIG_DIR)

print(f"dlt configs : {DLT_CONFIG_DIR}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json

import dlt
import yaml
from dlt.common.configuration.specs import AzureCredentials
from dlt.destinations import filesystem
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.rest_api import rest_api_source

with open(os.path.join(DLT_CONFIG_DIR, dlt_config), encoding="utf-8") as fh:
    cfg = yaml.safe_load(fh)

# --- Destination: LH_Bronze/Tables via OneLake --------------------------------------------
# dlt reaches OneLake on two paths, and the "external session" credential feeds both:
#   * adlfs (fsspec) for dlt's own bookkeeping files — gets the credential OBJECT, plus the
#     explicit account_host because adlfs only auto-detects *.core.windows.net accounts;
#   * delta-rs (object_store) for the Delta writes — dlt freezes the credential into a
#     bearer token (to_object_store_rs_credentials); use_fabric_endpoint makes the OneLake
#     host explicit rather than relying on delta-rs URL sniffing.
credentials = AzureCredentials.from_credential(_StaticToken())
credentials.azure_storage_account_name = "onelake"

destination = filesystem(
    bucket_url=(
        f"abfss://{SRC_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LH_BRONZE_ID}/Tables"
    ),
    credentials=credentials,
    kwargs={"account_host": "onelake.blob.fabric.microsoft.com"},
    deltalake_storage_options={"use_fabric_endpoint": "true"},
)

# --- Build source + pipeline from the declarative config ----------------------------------

source = rest_api_source(cfg["source"])
if dlt_resources.strip():
    source = source.with_resources(*[r.strip() for r in dlt_resources.split(",")])

pipeline = dlt.pipeline(
    pipeline_name=cfg["pipeline_name"],
    destination=destination,
    dataset_name=cfg["dataset_name"],  # -> Tables/<dataset_name>/<table> in LH_Bronze
)

# drop_sources wipes this source's tables AND its stored incremental state, so the reload
# starts over from the config's initial_value — the EL equivalent of dbt --full-refresh.
run_kwargs = {"refresh": "drop_sources"} if str(dlt_full_refresh).lower() == "true" else {}

print(f"Running dlt pipeline '{cfg['pipeline_name']}' from {dlt_config}")

# --- Run and classify the outcome for the pipeline -----------------------------------------
# Same contract as the dbt runner: a failed LOAD does not raise (an exception would fail the
# notebook activity before the pipeline could read a structured exitValue) — it exits with
# status "ingestion_failed" for an orchestrating pipeline to switch on. Failures before the
# pipeline object can report anything (config download, auth) still raise and are caught by
# the notebook activity's on-failure path as the fallback alert.

status, error_message = "success", ""
try:
    pipeline.run(source, table_format="delta", **run_kwargs)
except PipelineStepFailed as exc:
    status = "ingestion_failed"
    error_message = f"{exc.step}: {str(exc)[:500]}"

# Rows that reached the destination in THIS run, per table (incremental no-op runs load 0
# rows and report an empty dict). _dlt_pipeline_state is dlt bookkeeping — dropped here.
normalize_info = pipeline.last_trace.last_normalize_info if pipeline.last_trace else None
row_counts = {
    table: count
    for table, count in (normalize_info.row_counts if normalize_info else {}).items()
    if not table.startswith("_dlt")
}

icon = {"success": "✅", "ingestion_failed": "🔴"}[status]
lines = [f"{icon} dlt {cfg['pipeline_name']} — {status.replace('_', ' ')}"]
if row_counts:
    lines += [f"- **{table}**: {row_counts[table]} rows" for table in sorted(row_counts)]
elif status == "success":
    lines.append("- no new rows (incremental cursor up to date)")
if error_message:
    lines.append(f"- {error_message}")

payload = {
    "status": status,
    "pipeline": cfg["pipeline_name"],
    "config": dlt_config,
    "row_counts": row_counts,
    "alert_message": "\n".join(lines),
}

exit_value = json.dumps(payload, ensure_ascii=False)
print(f"dlt pipeline finished with status '{status}'")
print(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# notebookutils.notebook.exit() raises to halt the run, which Fabric renders as a failed
# cell even on a successful pipeline — kept in its own cell so that stray traceback doesn't
# get attributed to the ingestion logic above.
notebookutils.notebook.exit(exit_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

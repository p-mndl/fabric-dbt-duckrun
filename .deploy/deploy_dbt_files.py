"""Copy a runner project's files (dbt/ or ingest/) to LH_Gold/Files/ in the target workspace.

A plain file copy: the uploaded files carry no environment-specific GUIDs -- profiles.yml,
sources.yml and the dlt configs resolve everything at runtime (env vars / Variable Library,
set by the respective runner notebook from the workspace's active value set).

Callers: .pipelines/azure-pipelines.yml, .vscode/terminal-init.ps1 (deploy shell function)

Usage:
    python .deploy/deploy_dbt_files.py --env dev_pm                    # dbt project
    python .deploy/deploy_dbt_files.py --env dev_pm --project ingest   # dlt configs
    (--env defaults to $DBT_VL_ENV, which the terminal profile sets from .dev-env)
"""

import argparse
import os
import subprocess
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

from fabric_vl import get_variables

ROOT = Path(__file__).parent.parent

# Source dir in the repo -> target subdir under LH_Gold/Files/.
PROJECTS = {
    "dbt": "dbt_duckrun",
    "ingest": "dlt_ingest",
}


def git_tracked_files(directory: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(directory), "ls-files"],
        capture_output=True, text=True, check=True,
    )
    return [directory / rel for rel in result.stdout.splitlines()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=os.environ.get("DBT_VL_ENV", "dev"),
                        help="Variable Library value set name (dev, dev_<initials>, test, prod)")
    parser.add_argument("--project", default="dbt", choices=sorted(PROJECTS),
                        help="which project to deploy: dbt (duckrun POC) or ingest (dlt configs)")
    args = parser.parse_args()

    dbt_dir = ROOT / args.project
    target_subdir = PROJECTS[args.project]

    variables = get_variables(args.env)  # raises for unknown value set names
    workspace_id = variables["workspace_id"]
    lh_gold_id = variables["lh_gold"]

    credential = AzureCliCredential()
    service = DataLakeServiceClient(
        "https://onelake.dfs.fabric.microsoft.com", credential=credential
    )
    fs = service.get_file_system_client(workspace_id)

    files = git_tracked_files(dbt_dir)

    try:
        fs.get_directory_client(f"{lh_gold_id}/Files/{target_subdir}").delete_directory()
        print(f"deleted existing Files/{target_subdir}/")
    except ResourceNotFoundError:
        pass

    dirs = set()
    for f in files:
        rel_dir = (Path(target_subdir) / f.relative_to(dbt_dir)).parent
        while rel_dir.parts:
            dirs.add(rel_dir.as_posix())
            rel_dir = rel_dir.parent

    for d in sorted(dirs):
        fs.get_directory_client(f"{lh_gold_id}/Files/{d}").create_directory()

    for f in files:
        rel = Path(target_subdir) / f.relative_to(dbt_dir)
        file_client = fs.get_directory_client(
            f"{lh_gold_id}/Files/{rel.parent.as_posix()}"
        ).get_file_client(f.name)
        file_client.upload_data(f.read_bytes(), overwrite=True)
        print(f"uploaded {rel.as_posix()}")

    print(f"\nDeployed {len(files)} file(s) to env '{args.env}' (workspace {workspace_id}).")


if __name__ == "__main__":
    main()

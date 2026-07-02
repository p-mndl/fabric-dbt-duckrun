"""Promote this repo's Fabric items (Lakehouses, Notebook, VariableLibrary) from the dev
workspace (git-synced) to the target workspace via fabric-cicd.

No parameter.yml: none of these items embed environment-specific GUIDs (the Notebook reads
workspace/lakehouse IDs at runtime via notebookutils.variableLibrary, not from hardcoded values
in its own definition) -- fabric-cicd just needs to publish the items as-is.

Usage (run from a context with an authenticated `az` session, e.g. inside an AzureCLI@2
pipeline task):
    python .deploy/deploy_fabric_items.py --env test
"""

import argparse
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

ROOT = Path(__file__).parent.parent
from fabric_vl import get_variables


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["test", "prod"])
    args = parser.parse_args()

    workspace_id = get_variables(args.env)["workspace_id"]

    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        environment=args.env,
        repository_directory=str(ROOT / "fabric"),
        token_credential=AzureCliCredential(),
    )

    publish_all_items(workspace)
    unpublish_all_orphan_items(workspace)
    print(f"Deployed Fabric items to env '{args.env}' (workspace {workspace_id}).")


if __name__ == "__main__":
    main()

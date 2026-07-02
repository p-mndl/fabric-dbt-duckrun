"""Read Fabric VariableLibrary values (fabric/VL.VariableLibrary) outside of a Fabric runtime.

Callers: .deploy/deploy_dbt_files.py, .deploy/deploy_fabric_items.py, and
.vscode/terminal-init.ps1 (CLI mode: `python fabric_vl.py <env>` prints the merged values as
JSON so the terminal profile can export them as env vars).
"""

import json
import sys
from pathlib import Path

_VL_DIR = Path(__file__).parent.parent / "fabric" / "VL.VariableLibrary"


def get_variables(env: str) -> dict:
    """Return the merged variable values for `env` (dev, test, prod)."""
    base = json.loads((_VL_DIR / "variables.json").read_text())
    values = {v["name"]: v["value"] for v in base["variables"]}

    if env == "dev":
        return values

    valueset_path = _VL_DIR / "valueSets" / f"{env}.json"
    if not valueset_path.exists():
        raise ValueError(f"No value set found for env '{env}' at {valueset_path}")
    overrides = json.loads(valueset_path.read_text())
    for o in overrides.get("variableOverrides", []):
        values[o["name"]] = o["value"]
    return values


if __name__ == "__main__":
    print(json.dumps(get_variables(sys.argv[1] if len(sys.argv) > 1 else "dev")))

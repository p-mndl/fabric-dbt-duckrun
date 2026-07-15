# dbt on Microsoft Fabric with duckrun

A template for running [dbt](https://www.getdbt.com/) against Microsoft Fabric **without Spark
and without a Fabric Warehouse**: models execute in an in-memory DuckDB and read/write OneLake
Delta tables directly over `abfss://` paths, using the community
[duckrun](https://djouallah.github.io/duckrun/dbt-adapter.html) dbt adapter. The repo covers the
full loop: local development, a parameterized Fabric notebook runner, an orchestration pattern
with Fabric Data Pipelines, and dev → test → prod promotion via Azure DevOps + `fabric-cicd`.

**The focus is the read/write path between dbt and Fabric.** The included models (a public
holidays sample) are deliberately trivial — ingestion and transformation logic are placeholders
you will replace with your own.

---

## Table of contents

- [Architecture](#architecture)
- [How dbt reaches OneLake](#how-dbt-reaches-onelake)
- [Configuring write targets](#configuring-write-targets)
- [Repository layout](#repository-layout)
- [Setup guide](#setup-guide)
  - [0. Prerequisites](#0-prerequisites)
  - [1. Create the DEV workspace and connect git](#1-create-the-dev-workspace-and-connect-git)
  - [2. Fill in the GUIDs](#2-fill-in-the-guids)
  - [3. Load sample data](#3-load-sample-data)
  - [4. Local development](#4-local-development)
  - [5. Run dbt inside Fabric](#5-run-dbt-inside-fabric)
  - [6. Snapshots (SCD2 demo)](#6-snapshots-scd2-demo)
  - [7. CI/CD to TEST and PROD](#7-cicd-to-test-and-prod)
- [Working in a team](#working-in-a-team)
- [Observability (elementary)](#observability-elementary)
- [Alerting (failed models and tests)](#alerting-failed-models-and-tests)
- [Documentation & lineage (docglow)](#documentation--lineage-docglow)
- [Placeholder reference](#placeholder-reference)
- [Design decisions](#design-decisions)
- [Troubleshooting](#troubleshooting)
- [Dependencies](#dependencies)
- [License](#license)

---

## Architecture

One Fabric workspace per environment (DEV / TEST / PROD), three lakehouses per workspace
(medallion layout):

| Lakehouse | Purpose | Managed by |
|---|---|---|
| `LH_Bronze` | Raw data (ingestion layer — **not** dbt's job) | External tools (Copy Job, pipelines, notebooks) |
| `LH_Silver` | dbt snapshots (SCD2 history) | dbt via duckrun's multi-catalog config |
| `LH_Gold` | Curated output tables + the deployed dbt project files (`Files/dbt_duckrun/`) | dbt via duckrun / `.deploy/deploy_dbt_files.py` |

Only the **DEV** workspace is git-synced (root folder `fabric/` — a portal setting, see setup).
TEST and PROD workspaces are populated exclusively by the CI/CD pipeline via `fabric-cicd`.

Fabric items shipped in this repo:

| Item | Role |
|---|---|
| `NB_dbt_duckrun_runner` | Parameterized notebook: downloads the dbt project from `LH_Gold/Files/` and runs exactly one dbt command (`dbt_command`, `dbt_select`, `dbt_full_refresh`, `dbt_vars`) |
| `PL_Orchestration` | Demo Data Pipeline showing the pattern: one activity calling the runner notebook with `dbt build` (IDs resolved through the Variable Library) |
| `NB_scd2_test_mutator` | Disposable test scaffolding: deterministically mutates a fake source table so `dbt snapshot` has something to version |
| `VL` (Variable Library) | **Single source of truth for all environment GUIDs** — read locally by the deploy scripts and at runtime by the notebook; `valueSets/{test,prod}.json` override per environment |

## How dbt reaches OneLake

- **Writes** — `dbt/profiles.yml` sets `type: duckrun` and a `root_path` of the form
  `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>/Tables`. `dbt run`
  writes Delta tables straight to OneLake via delta-rs; there is no separate publish step.
  Snapshots are routed to `LH_Silver` through a second catalog (`catalogs.silver` in
  `profiles.yml` + `+database: silver` in `dbt_project.yml`, requires duckrun >= 0.3.29).
- **Reads** — sources are declared in `dbt/models/sources.yml` with `meta.plugin: duckrun` and
  `meta.delta_table_path` pointing at the Bronze Delta table (duckrun's documented source
  mechanism; a plain `{{ source() }}` call in the model). The read side is addressed
  separately (`SRC_WORKSPACE_ID`/`LH_BRONZE_ID`), so it can point at a *different* workspace
  than the write side — see [Working in a team](#working-in-a-team).
- **All GUIDs flow from the Variable Library** — `profiles.yml` and `sources.yml` contain only
  `{{ env_var(...) }}` references (`WORKSPACE_ID`, `LH_GOLD_ID`, `LH_SILVER_ID`,
  `SRC_WORKSPACE_ID`, `LH_BRONZE_ID`), never GUID literals. Locally the VS Code terminal
  profile resolves them from the value set named in `.dev-env`; in Fabric the runner notebook
  resolves them from the workspace's **active** value set. Missing env var → dbt fails loudly
  instead of writing to the wrong place (deliberately no defaults).
- **Auth** — a short-lived bearer token in the `FABRIC_STORAGE_TOKEN` env var
  (`storage_options.bearer_token`). Locally it comes from `az account get-access-token
  --resource https://storage.azure.com` (the VS Code terminal profile does this on startup);
  inside Fabric the notebook uses `notebookutils.credentials.getToken("storage")`. The token is
  OneLake-wide, so one token serves all lakehouses.
- OneLake paths must use **GUIDs, not friendly names** (workspace/lakehouse names in abfss
  paths are unreliable upstream).
- **Environments are workspaces, not dbt targets.** `profiles.yml` has a single target
  (`fabric`); which workspace a run reads from and writes to is decided entirely by which
  Variable Library value set resolves the env vars (locally: `.dev-env`; in Fabric: the
  workspace's active value set, which `fabric-cicd` switches per environment during promotion).
  The deployed files are byte-identical in every environment.

## Configuring write targets

The default write root is the `root_path` of the single target in `dbt/profiles.yml` (here:
`LH_Gold`). Every additional lakehouse you want dbt to write to is declared as a named catalog:

```yaml
catalogs:
  silver:
    root_path: "abfss://{{ env_var('WORKSPACE_ID') }}@onelake.dfs.fabric.microsoft.com/{{ env_var('LH_SILVER_ID') }}/Tables"
    storage_options:
      bearer_token: "{{ env_var('FABRIC_STORAGE_TOKEN') }}"
```

Each alias becomes addressable through dbt's standard `database` config — from any model or
snapshot, at any granularity. Nodes without a `database` config write to the default
`root_path`. Recipes:

```yaml
# dbt_project.yml — route ALL snapshots to one lakehouse (this template's default):
snapshots:
  fabric_duckrun:
    +database: silver

# dbt_project.yml — route a whole model folder:
models:
  fabric_duckrun:
    curated:
      +database: silver
```

```sql
-- per node, in the model/snapshot itself:
{{ config(database='silver') }}
```

Want snapshots in `LH_Bronze` instead? Add a `bronze` catalog with `LH_Bronze`'s GUID and set
`snapshots: +database: bronze`. Want per-snapshot targets? Drop the project-level default and
set `database` in each snapshot's YAML `config:` block. Tables land at
`<root_path>/<schema>/<table>` inside the chosen lakehouse. For profile options beyond what
this template uses, see the [duckrun dbt-adapter docs](https://djouallah.github.io/duckrun/dbt-adapter.html).

## Repository layout

```
.
├── requirements.txt                duckrun[local] (pinned) + azure-storage-file-datalake
├── .vscode/
│   ├── settings.json               Terminal profile "dbt (dev)" (default profile)
│   └── terminal-init.ps1           Startup: venv, value-set choice (.dev-env), GUID env vars,
│                                   storage token, cd dbt/, deploy + Show-Fails helpers
├── .pipelines/
│   └── azure-pipelines.yml         ADO pipeline: triggers on test/prod branches, one AzureCLI@2 task
├── .deploy/
│   ├── fabric_vl.py                Reads fabric/VL.VariableLibrary locally (merged per value set)
│   ├── deploy_dbt_files.py         Uploads dbt/ (git-tracked files) to LH_Gold/Files/dbt_duckrun/
│   │                               of the chosen value set's workspace (plain copy, no rewriting)
│   └── deploy_fabric_items.py      Promotes Fabric items to test/prod via fabric-cicd (pipeline only)
├── fabric/                         Git-sync root of the DEV workspace (portal setting!)
│   ├── VL.VariableLibrary/         All environment GUIDs + valueSets for test/prod
│   ├── LH_Bronze / LH_Silver / LH_Gold (.Lakehouse)
│   ├── NB_dbt_duckrun_runner.Notebook/
│   ├── NB_scd2_test_mutator.Notebook/
│   └── PL_Orchestration.DataPipeline/
└── dbt/
    ├── dbt_project.yml             Project "fabric_duckrun"; models → schema "demo"; snapshots → silver
    ├── profiles.yml                type: duckrun; single target "fabric" + silver catalog (env vars only)
    ├── macros/generate_schema_name.sql
    ├── snapshots/scd2_test_source_snapshot.yml   SCD2 check-strategy snapshot (YAML-only syntax)
    └── models/
        ├── sources.yml             duckrun source plugin → LH_Bronze Delta tables
        ├── staging/stg_publicholidays.sql
        └── marts/dim_publicholidays.sql, test_incremental_holidays.sql (incremental merge demo)
```

## Setup guide

### 0. Prerequisites

- A Microsoft Fabric capacity (trial works) and permission to create workspaces.
- **Python 3.12** — dbt-core/`mashumaro` are not yet compatible with newer Python versions.
- Azure CLI (`az`), VS Code.
- This repo pushed to a git host Fabric can sync with (Azure DevOps Repos or GitHub).
- For CI/CD (step 7): an Azure DevOps project.

### 1. Create the DEV workspace and connect git

1. Create a workspace (e.g. `DEV_dbt`), assign it to your capacity.
2. Workspace settings → **Git integration** → connect to this repo, branch `main` (switch to
   the feature branch you are working on as needed), and set the **folder to `fabric/`**. This
   is a portal-only setting — nothing in the repo enforces it, but without it the sync would
   try to treat the whole repo as Fabric items.
3. Sync. Fabric creates the three lakehouses, both notebooks, the pipeline, and the Variable
   Library in the workspace.

### 2. Fill in the GUIDs

The template ships with placeholder GUIDs (see [Placeholder reference](#placeholder-reference)),
and they live in exactly **one file**: `fabric/VL.VariableLibrary/variables.json`. After the
first sync, collect the real GUIDs (workspace and item GUIDs are visible in the portal URL when
the item is open) and fill in all six values: `workspace_id`, `lh_gold`, `lh_silver` (write
side), `src_workspace_id`, `lh_bronze` (read side — for a single-developer setup these point at
the same workspace), and `nb_dbt_runner`. Commit and let the workspace sync the updated
Variable Library. `dbt/profiles.yml` and `dbt/models/sources.yml` never contain GUIDs — they
resolve everything from env vars at runtime.

### 3. Load sample data

The demo models read `dbo.publicholidays` from `LH_Bronze`. Easiest way to get it: open
`LH_Bronze` → *Get data* → *New Dataflow / sample data* and load the built-in **Public Holidays**
sample as a Delta table named `publicholidays` in the `dbo` schema. Alternatively point
`dbt/models/sources.yml` at any Delta table you already have and adjust the two demo models.

### 4. Local development

One-time setup:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Daily use: open a VS Code terminal — the default profile `dbt (dev)` activates the venv, asks
**once** which Variable Library value set you work in (just press Enter for `dev` in a
single-developer setup; the answer is stored in the gitignored `.dev-env`), exports the GUID
env vars from that value set, acquires `FABRIC_STORAGE_TOKEN` (runs `az login` first if there
is no Azure session), changes into `dbt/`, and defines two helpers:

```powershell
dbt run                       # or build / test / snapshot — plain dbt CLI, no wrappers
dbt run --select dim_publicholidays
deploy                        # upload the dbt project to YOUR workspace's LH_Gold (from .dev-env)
deploy --env test             # ... or any other value set's workspace
Show-Fails <compiled-sql>     # re-run a failed test's compiled SQL via dbt show
```

Token expired (~60–90 min) or no Azure session? Open a new terminal — the profile re-acquires
everything.

Note `threads: 1` semantics: duckrun is single-threaded within a run and stateless across
invocations (a fresh in-memory DuckDB each time) — so `--select model_name` needs upstream views
included (e.g. `--select +model_name`) because views from previous runs no longer exist.

### 5. Run dbt inside Fabric

1. `deploy --env dev` — uploads the git-tracked `dbt/` files to `LH_Gold/Files/dbt_duckrun/`.
2. Run `NB_dbt_duckrun_runner` (manually, or via `PL_Orchestration`). The notebook reads the
   GUIDs from the Variable Library, fetches a storage token, downloads the project files from
   OneLake (no default lakehouse / mount required), and invokes exactly one dbt command.
   Parameters: `dbt_command` (default `run`; `build` is the recommended command for
   orchestration — it runs snapshot + run + test in dependency order), `dbt_select`,
   `dbt_full_refresh`, `dbt_vars`. `PL_Orchestration` exposes the same four as pipeline
   parameters, so a manual pipeline run can override them without editing anything.

Orchestration pattern: **one pipeline per domain** (Fabric supports only one schedule per
pipeline), each passing `dbt_select=tag:<domain>` to the same runner notebook. The pipeline is
the orchestrator; the notebook always executes a single command and reports its outcome back
to the pipeline as a structured exit value (see [Alerting](#alerting-failed-models-and-tests)).

### 6. Snapshots (SCD2 demo)

`dbt/snapshots/scd2_test_source_snapshot.yml` demonstrates a `check`-strategy snapshot
(`check_cols: [price, category]`, `invalidate_hard_deletes: true`) that writes to `LH_Silver`
via the `silver` catalog.

1. Run `NB_scd2_test_mutator` in Fabric once — it creates `dbo.scd2_test_source` in `LH_Bronze`
   (subsequent runs mutate it deterministically: `id=1` changes tracked columns → new snapshot
   version; `id=2` changes only an untracked column → no new version; one insert per run; a
   hard delete every 5th run).
2. Run `dbt snapshot` (locally or via the runner) and inspect the history table in `LH_Silver`.

### 7. CI/CD to TEST and PROD

Branch model: work happens on `main` or a feature branch (whichever the DEV workspace is
git-synced to) → PR to `test` → PR to `prod`. The
pipeline (`.pipelines/azure-pipelines.yml`) triggers on pushes to `test`/`prod` and runs two
scripts: `deploy_fabric_items.py` (promotes lakehouses/notebooks/pipeline/VL via `fabric-cicd`,
switching the VL's active value set to the target environment) and `deploy_dbt_files.py`
(uploads the dbt project files — a plain copy, since the files resolve all GUIDs at runtime).

1. **Workspaces**: create the TEST and PROD workspaces (empty, no git sync), assign capacity.
   Put their workspace GUIDs into `fabric/VL.VariableLibrary/valueSets/test.json` / `prod.json`
   (both `workspace_id` and `src_workspace_id` — test and prod read their own bronze).
2. **Service connection**: in ADO, create an *Azure Resource Manager* service connection using
   the **Workload Identity Federation (automatic)** flow. Replace `<SERVICE_CONNECTION_NAME>`
   in `.pipelines/azure-pipelines.yml` with its name.
3. **Fabric access for the service principal**: add the connection's service principal as
   **Admin** (or Member) on the TEST and PROD workspaces (*Manage access*). If service
   principals are blocked tenant-wide, enable *"Service principals can use Fabric APIs"* in the
   Fabric admin portal.
4. **Register the pipeline** in ADO pointing at `.pipelines/azure-pipelines.yml`.
5. **Bootstrap the lakehouse GUIDs** (chicken-and-egg by design): the first pipeline run per
   environment creates the lakehouses via `fabric-cicd`, but `deploy_dbt_files.py` then fails
   with `ResourceNotFoundError` because `valueSets/<env>.json` still carries placeholder
   lakehouse GUIDs. Copy the real GUIDs from the freshly created lakehouses into the value set,
   merge, and re-run — from then on the pipeline is fully hands-off.

## Working in a team

The Variable Library value sets scale naturally from one developer to many. The pattern:

- **Each developer gets their own DEV workspace** (created like in setup step 1, synced to the
  same repo/branch layout) **and their own value set**: add
  `fabric/VL.VariableLibrary/valueSets/dev_<initials>.json` (e.g. `dev_pm.json`) overriding
  `workspace_id`, `lh_gold`, `lh_silver`, `nb_dbt_runner` with the GUIDs of their workspace.
- **Read from shared data, write in isolation.** DEV workspaces rarely hold real data, so a
  developer's value set typically points the read side (`src_workspace_id`, `lh_bronze`) at
  the **shared TEST bronze**: local `dbt run` then reads real data but writes only to the
  developer's own workspace. TEST stays read-only for humans — nothing a developer does
  locally can touch the real TEST/PROD tables (those env vars simply never point there).
- **In Fabric**, each developer sets their value set as the **active** one in their own
  workspace (Variable Library item → set active value set, once). The runner notebook then
  resolves the same GUIDs the local terminal would.
- **Locally**, each developer answers the one-time terminal prompt with their value set name
  (stored in the gitignored `.dev-env`). `deploy` uploads to their workspace automatically;
  `dbt run` reads TEST bronze and writes to their gold/silver.

Because everything is just another value set, no script or config file changes when a
developer joins — one JSON file in `valueSets/`, one portal click, one terminal prompt.

## Observability (elementary)

The project ships with the [elementary](https://docs.elementary-data.com/) dbt package for
run/test monitoring. Its `on-run-end` hooks write every invocation's results — run history,
per-model timings, individual test outcomes — as regular Delta tables into an `elementary`
schema next to your models. No extra step per run: the scheduled Fabric pipeline runs collect
their own history automatically (the runner notebook executes `dbt deps` before the main
command, resolving the pinned version from the committed `package-lock.yml`).

Consuming the data:

- **In Fabric** (the primary path): the elementary tables (`dbt_run_results`,
  `elementary_test_results`, `dbt_invocations`, …) are plain Delta tables in the Lakehouse —
  build a Power BI report on them (Direct Lake works). Failure notifications do NOT go
  through these tables (see [Alerting](#alerting-failed-models-and-tests) — deliberately no
  Activator).
- **Locally** (deep dive): `edr-report` in the VS Code terminal generates elementary's
  standard HTML report (`dbt/edr_target/elementary_report.html`). Because Lakehouses have no
  view concept, duckrun cannot persist elementary's report views to OneLake; the wrapper
  first runs `.deploy/elementary_report_mirror.py`, which mirrors the Delta tables into a
  local DuckDB file and recreates the views from dbt's compiled SQL, then points `edr` at
  that mirror (profile `elementary` in `profiles.yml`, path via `ELEMENTARY_MIRROR`).
  Pointing your value set at another workspace lets you deep-dive its history the same way.

## Alerting (failed models and tests)

Alerting is driven by the pipeline, not by an Activator listening on the elementary Delta
tables — an always-on trigger costs capacity, while the run itself already knows everything
the alert needs the moment it finishes.

How it works:

- The runner notebook **never raises on dbt failures**. It classifies the outcome —
  `models_failed` (any node ERROR: broken model/snapshot SQL, incl. tests that error) >
  `tests_failed` (error-severity test failures) > `tests_warned` (warn-severity findings
  only) > `success` — and hands a compact JSON to the pipeline via
  `notebookutils.notebook.exit()`: the status, per-status counts, and a ready-to-post
  `alert_message`. For failed tests the message includes up to 5 failing-row samples per
  test (3 tests max), read once from elementary's `test_result_rows` Delta table in the same
  session.
- `PL_Orchestration` **switches on `status`**: `models_failed` and `tests_failed` run the
  alert slot and then an explicit **Fail activity**, so the pipeline still turns red and the
  alert text lands in the run history; `tests_warned` only informs and leaves the pipeline
  green; `success` falls through silently.
- A separate **on-failure branch** (`Fail_notebook_crashed`) catches runs that never
  produced an exit value at all — token expiry, `dbt deps` failure, kernel death.

Plugging in a channel: the template ships channel-less (the Fail activities carry the alert
message into the run history). To notify a team, add a Teams or Office 365 Outlook activity
**in front of** each Fail activity and post `@json(activity('NB_dbt_build').output.result.exitValue).alert_message`
— the activity descriptions in the pipeline mark the exact slots, including the escalation
convention (🔴 model errors with @mention, 🟡 data quality, ⚪ warnings). Keep the Fail
activities: an on-failure branch that ends successfully would otherwise flip the whole
pipeline to *Succeeded*. Note that a Teams/Outlook connection is environment-specific and
lands in the git-synced pipeline JSON.

Verifying end-to-end: three var-gated smoke nodes exercise each path without touching real
models. Run `PL_Orchestration` manually with the matching `dbt_vars` parameter:

| `dbt_vars`                      | node                                  | exercises                                  |
| ------------------------------- | ------------------------------------- | ------------------------------------------ |
| `{"smoke_test_fail": "true"}`   | `tests/smoke_test_failure.sql`        | 🟡 `tests_failed` + failing-row samples    |
| `{"smoke_test_warn": "true"}`   | `tests/smoke_test_warning.sql`        | ⚪ `tests_warned`, pipeline stays green    |
| `{"smoke_model_error": "true"}` | `models/marts/smoke_model_error.sql`  | 🔴 `models_failed` + downstream skipping   |

## Documentation & lineage (docglow)

[docglow](https://github.com/docglow/docglow) generates a static documentation site —
model/column docs, interactive lineage (including column-level), full-text search, health
scoring — from dbt's compile-time artifacts. It never connects to the warehouse itself; the
column catalog comes from `dbt docs generate`, which works over duckrun.

The `docs` terminal function does everything: builds the catalog, generates the site
(single self-contained HTML under `dbt/target/docglow/index.html`) and opens it. Regenerate
whenever models change; the file can be shared as-is (mail, wiki attachment) — no server
needed. This is developer-facing documentation; run/test monitoring is elementary's job
(section above).

## Placeholder reference

Every environment-specific value in this template is a recognizable placeholder. Replace them
all; nothing else in the repo is tenant-specific.

| Placeholder | Meaning | Files |
|---|---|---|
| `11111111-1111-1111-1111-111111111111` | DEV workspace ID (`workspace_id` + `src_workspace_id`) | `variables.json` |
| `22222222-2222-2222-2222-222222222222` | DEV `LH_Bronze` ID | `variables.json` |
| `33333333-3333-3333-3333-333333333333` | DEV `LH_Silver` ID | `variables.json` |
| `44444444-4444-4444-4444-444444444444` | DEV `LH_Gold` ID | `variables.json` |
| `55555555-5555-5555-5555-555555555555` | DEV `NB_dbt_duckrun_runner` ID | `variables.json` |
| `aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1` … `a5` | TEST workspace / bronze / silver / gold / notebook IDs | `valueSets/test.json` |
| `bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1` … `b5` | PROD workspace / bronze / silver / gold / notebook IDs | `valueSets/prod.json` |
| `<SERVICE_CONNECTION_NAME>` | ADO service connection | `.pipelines/azure-pipelines.yml` |

`dbt/profiles.yml` and `dbt/models/sources.yml` contain no GUIDs at all — only
`{{ env_var(...) }}` references resolved from the Variable Library at runtime.

## Design decisions

- **Why duckrun instead of the official `dbt-fabric` adapter?** `dbt-fabric` targets Fabric
  *Warehouse* (T-SQL). This template targets *lakehouses*: duckrun wraps DuckDB execution +
  Delta materialization (delta-rs) directly as a dbt adapter, so `dbt run` writes Delta tables
  itself — no warehouse, no Spark session, no custom post-run publish step.
- **GUIDs are injected via env vars, set once per terminal/session — never per dbt call.** dbt
  renders `profiles.yml` with a restricted Jinja context that only knows `env_var()`, so the
  Variable Library can't be read from there directly; instead the terminal profile (locally)
  and the runner notebook (in Fabric) resolve the chosen value set once and export plain env
  vars. Native CLI/IDE usage stays intact (no wrapper per invocation), the git-tracked files
  are identical for every developer and environment, and a missing variable fails loudly
  (deliberately no `env_var` defaults) instead of writing to the wrong workspace.
- **Variable Library as single source of truth**: deploy scripts read it locally
  (`.deploy/fabric_vl.py`), the notebook reads it at runtime
  (`notebookutils.variableLibrary.getLibrary("VL")`), pipelines reference it via
  `libraryVariables`. No `parameter.yml` needed for `fabric-cicd` — no item definition embeds
  environment-specific GUIDs.
- **Snapshots in a separate lakehouse via multi-catalog** (duckrun >= 0.3.29): the default
  `root_path` points at `LH_Gold`; `catalogs.silver` adds `LH_Silver`, and
  `snapshots: +database: silver` routes every snapshot there by default so none can silently
  land in Gold.
- **`restartPython()` after `%pip install duckrun`** in the runner notebook: the Fabric kernel
  ships with an older duckdb already loaded; without a session restart duckrun fails with
  "needs a newer duckdb". This is duckrun's own documented fix.
- **No default lakehouse on the runner notebook**: the dbt project is downloaded via the ADLS
  SDK from `LH_Gold/Files/` instead of relying on a `/lakehouse/default/` mount — keeps the
  notebook free of manual attach steps and portable across environments.
- **Deliberately lightweight CI/CD**: no Key Vault, no client secrets (WIF), no approval gates,
  no path filters — add them when the setup grows beyond a template.

## Troubleshooting

- **`RuntimeError: duckrun needs a newer duckdb ...` in Fabric** — the kernel restart in cell 1
  handles this; if you removed it, put it back.
- **401/403 on OneLake locally** — token expired (~60–90 min). Open a new terminal; the profile
  fetches a fresh one.
- **`dbt test` failures**: dbt prints the compiled SQL path; pass it to `Show-Fails
  <path>` to see the offending rows (it rewrites the in-memory relation back to a
  `{{ source() }}` call and runs it via `dbt show --inline`).
- **`ResourceNotFoundError` in the pipeline on first test/prod run** — expected bootstrap step,
  see [7. CI/CD](#7-cicd-to-test-and-prod), point 5.

## Dependencies

| Package | Maintainer | Version |
|---|---|---|
| duckrun | Community (djouallah) | pinned in `requirements.txt` (0.3.33, same pin in the runner notebook) |
| dbt-core / dbt-duckdb | dbt Labs / Community | transitive via duckrun |
| azure-storage-file-datalake | Microsoft | >= 12.14.0 (deploy script + notebook) |
| fabric-cicd | Microsoft | unpinned, installed in the pipeline only |

## License

[MIT](LICENSE)

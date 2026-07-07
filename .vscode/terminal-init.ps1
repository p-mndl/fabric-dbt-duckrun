$root = $PWD.Path
.\.venv\Scripts\Activate.ps1

# Which Variable Library value set to work against (write side = your own workspace).
# Stored in .dev-env (gitignored, one line, e.g. dev_pm); asked once if missing.
$devEnvFile = Join-Path $root '.dev-env'
if (Test-Path $devEnvFile) {
    $devEnv = (Get-Content $devEnvFile -Raw).Trim()
} else {
    $devEnv = Read-Host 'Variable Library value set to work in (Enter = dev)'
    if (-not $devEnv) { $devEnv = 'dev' }
    Set-Content $devEnvFile $devEnv
}
$env:DBT_VL_ENV = $devEnv

# Resolve the GUIDs for that value set and export them for profiles.yml / sources.yml.
$vl = python "$root\.deploy\fabric_vl.py" $devEnv | ConvertFrom-Json
$env:WORKSPACE_ID     = $vl.workspace_id
$env:LH_GOLD_ID       = $vl.lh_gold
$env:LH_SILVER_ID     = $vl.lh_silver
$env:SRC_WORKSPACE_ID = $vl.src_workspace_id
$env:LH_BRONZE_ID     = $vl.lh_bronze
Write-Host "Variable Library env '$devEnv' resolved (write -> workspace $($vl.workspace_id))." -ForegroundColor Green

$token = az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv 2>$null
if (-not $token) {
    az login
    $token = az account get-access-token --resource https://storage.azure.com --query accessToken -o tsv
}

if ($token) {
    $env:FABRIC_STORAGE_TOKEN = $token
    Write-Host 'FABRIC_STORAGE_TOKEN set.' -ForegroundColor Green
} else {
    Write-Host 'Could not acquire token.' -ForegroundColor Red
}

function deploy { python "$root\.deploy\deploy_dbt_files.py" @args }

# elementary edr CLI reads a local DuckDB mirror of the elementary schema (duckrun can't
# persist the report views to OneLake — see .deploy/elementary_report_mirror.py).
$env:ELEMENTARY_MIRROR = "$root\dbt\target\elementary_mirror.duckdb"

function edr-report {
    python "$root\.deploy\elementary_report_mirror.py"
    if ($LASTEXITCODE -eq 0) {
        edr report --project-dir "$root\dbt" --profiles-dir "$root\dbt" @args
    }
}

# Lineage/documentation site (docglow): reads compile-time artifacts only; the dbt docs
# generate step builds the column catalog from the Lakehouse first. Single-file HTML output.
# --exclude models/edr*: hide the elementary package's own models (they'd drag down the
# health scores); --enable-erd: render relationships from `relationships` tests as an ERD.
function docs {
    dbt docs generate --project-dir "$root\dbt" --profiles-dir "$root\dbt"
    if ($LASTEXITCODE -ne 0) { return }
    docglow generate --project-dir "$root\dbt" --static --output-dir "$root\dbt\target\docglow" `
        --exclude "models/edr*" --enable-erd @args
    if ($LASTEXITCODE -eq 0) { Invoke-Item "$root\dbt\target\docglow\index.html" }
}

function Show-Fails {
    param(
        [Parameter(Mandatory)][string]$SqlFile,
        [int]$Limit = 50
    )
    if (-not (Test-Path $SqlFile)) {
        Write-Error "File not found: $SqlFile"
        return
    }
    $sql = (Get-Content $SqlFile -Raw) -replace '"memory"\."([^"]+)"\."([^"]+)"', '{{ source(''$1'', ''$2'') }}'
    dbt show --inline $sql --limit $Limit
}

Set-Location dbt

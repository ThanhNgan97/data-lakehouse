param(
    [switch]$PreflightOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_common.ps1"

Set-Location $script:RepoRoot
Import-RootDotEnv

$exportZip = Join-Path `
    $script:RepoRoot `
    "superset_exports\day10\day10_superset_dashboards.zip"

if (-not (Test-Path $exportZip)) {
    throw "Superset export artifact is missing: $exportZip"
}

$supersetCid = Assert-ServiceRunning "superset"

$help = Invoke-ComposeCapture @(
    "exec","-T","superset",
    "superset","import-dashboards","--help"
)

if ($help.ExitCode -ne 0) {
    throw "Superset import-dashboards is unavailable."
}

$helpText = $help.Lines -join "`n"

if ($helpText -notmatch '(?m)(-p|--path)') {
    throw "Unable to determine Superset import path option."
}

Write-Host "SUPERSET_IMPORT_CLI_AVAILABLE=True"
Write-Host "SUPERSET_EXPORT_ARTIFACT_PRESENT=True"

if ($PreflightOnly) {
    Write-Host "SUPERSET_IMPORT_PREFLIGHT=PASS"
    exit 0
}

$containerZip = "/tmp/team_superset_import.zip"

& docker cp `
    $exportZip `
    "${supersetCid}:${containerZip}"

if ($LASTEXITCODE -ne 0) {
    throw "Failed to copy Superset export into container."
}

$user = $env:SUPERSET_ADMIN_USERNAME

if ([string]::IsNullOrWhiteSpace($user)) {
    $user = "admin"
}

$importArgs = @(
    "exec","-T","superset",
    "superset","import-dashboards",
    "-p",$containerZip,
    "-u",$user
)

if ($helpText -match '--overwrite') {
    $importArgs += "--overwrite"
}

$result = Invoke-ComposeCapture $importArgs

if ($result.ExitCode -ne 0) {
    foreach ($line in $result.Lines) {
        Write-Host $line
    }

    throw "Superset dashboard import failed."
}

Write-Host "SUPERSET_IMPORT_EXECUTED=True"

# --------------------------------------------------
# Post-import inventory
# --------------------------------------------------

$pythonPath = Join-Path `
    $env:TEMP `
    "team_superset_verify.py"

$pythonLines = @(
    "import json",
    "from superset.app import create_app",
    "app = create_app()",
    "with app.app_context():",
    "    from superset import db",
    "    from superset.models.dashboard import Dashboard",
    "    from superset.models.slice import Slice",
    "    from superset.connectors.sqla.models import SqlaTable",
    "    from superset.models.core import Database",
    "    dashboards = db.session.query(Dashboard).all()",
    "    filters = 0",
    "    for dashboard in dashboards:",
    "        try:",
    "            metadata = json.loads(dashboard.json_metadata or '{}')",
    "            filters += len(metadata.get('native_filter_configuration') or [])",
    "        except Exception:",
    "            pass",
    "    print('DASHBOARD_COUNT=' + str(len(dashboards)))",
    "    print('DATASET_COUNT=' + str(db.session.query(SqlaTable).count()))",
    "    print('CHART_COUNT=' + str(db.session.query(Slice).count()))",
    "    print('FILTER_COUNT=' + str(filters))",
    "    print('DATABASE_COUNT=' + str(db.session.query(Database).count()))"
)

Set-Content `
    -Path $pythonPath `
    -Value $pythonLines `
    -Encoding UTF8

& docker cp `
    $pythonPath `
    "${supersetCid}:/tmp/team_superset_verify.py" |
    Out-Null

if ($LASTEXITCODE -ne 0) {
    throw "Failed to copy Superset verification script."
}

$inventory = Invoke-ComposeCapture @(
    "exec","-T","superset",
    "python","/tmp/team_superset_verify.py"
)

if ($inventory.ExitCode -ne 0) {
    throw "Superset post-import inventory failed."
}

$counts = @{}

foreach ($line in $inventory.Lines) {
    if (
        $line -match
        '^(DASHBOARD_COUNT|DATASET_COUNT|CHART_COUNT|FILTER_COUNT|DATABASE_COUNT)=(\d+)$'
    ) {
        $counts[$Matches[1]] = [int]$Matches[2]
        Write-Host $line
    }
}

$pass = (
    $counts["DASHBOARD_COUNT"] -eq 2 -and
    $counts["DATASET_COUNT"] -eq 2 -and
    $counts["CHART_COUNT"] -eq 18 -and
    $counts["FILTER_COUNT"] -eq 7 -and
    $counts["DATABASE_COUNT"] -eq 1
)

Write-Host "SUPERSET_IMPORT_VERIFY=$pass"

if (-not $pass) {
    throw "Superset import counts do not match expected demo state."
}

Write-Host "SUPERSET_IMPORT=PASS"

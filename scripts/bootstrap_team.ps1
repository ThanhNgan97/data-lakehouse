param(
    [switch]$PreflightOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_common.ps1"

Set-Location $script:RepoRoot

Write-Host "=================================================="
Write-Host "TEAM BOOTSTRAP"
Write-Host "=================================================="

Assert-Tool "docker"
Assert-Tool "git"

Import-RootDotEnv

if (-not (Test-Path ".env.example")) {
    throw ".env.example is missing."
}

if (-not (Test-DockerEngine)) {
    throw "Docker engine is not ready. Start Docker Desktop first."
}

$composeArgs = $script:ComposeArgs

& docker compose @composeArgs config -q

if ($LASTEXITCODE -ne 0) {
    throw "Compose validation failed."
}

Write-Host "TOOLS=PASS"
Write-Host "ENV=PASS"
Write-Host "COMPOSE_PARSE=PASS"

& "$PSScriptRoot\import_superset.ps1" -PreflightOnly

if ($LASTEXITCODE -ne 0) {
    throw "Superset import preflight failed."
}

if ($PreflightOnly) {
    Write-Host "BOOTSTRAP_TEAM_PREFLIGHT=PASS"
    exit 0
}

Write-Host ""
Write-Host "Starting team stack..."

& docker compose @composeArgs up -d --build

if ($LASTEXITCODE -ne 0) {
    throw "docker compose up -d --build failed."
}

# PostgreSQL
$postgresReady = $false

for ($i = 1; $i -le 60; $i++) {
    $probe = Invoke-ComposeCapture @(
        "exec","-T","postgres",
        "sh","-lc",
        'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
    )

    if ($probe.ExitCode -eq 0) {
        $postgresReady = $true
        break
    }

    Start-Sleep -Seconds 2
}

if (-not $postgresReady) {
    throw "PostgreSQL readiness timeout."
}

Write-Host "READY=postgres"

# MinIO
$p = Get-ComposePort "minio" 9000

if (-not $p) {
    throw "Unable to resolve MinIO port."
}

Wait-HttpEndpoint `
    -Name "minio" `
    -Url "http://127.0.0.1:$p/minio/health/live"

# Nessie
$p = Get-ComposePort "nessie" 19120

if (-not $p) {
    throw "Unable to resolve Nessie port."
}

Wait-HttpEndpoint `
    -Name "nessie" `
    -Url "http://127.0.0.1:$p/api/v2/config"

# Airflow webserver
$p = Get-ComposePort "airflow-webserver" 8080

if (-not $p) {
    throw "Unable to resolve Airflow webserver port."
}

Wait-HttpEndpoint `
    -Name "airflow-webserver" `
    -Url "http://127.0.0.1:$p/health"

# Airflow scheduler
$schedulerReady = $false

for ($i = 1; $i -le 60; $i++) {
    $probe = Invoke-ComposeCapture @(
        "exec","-T","airflow-scheduler",
        "airflow","jobs","check",
        "--job-type","SchedulerJob"
    )

    if ($probe.ExitCode -eq 0) {
        $schedulerReady = $true
        break
    }

    Start-Sleep -Seconds 2
}

if (-not $schedulerReady) {
    throw "Airflow scheduler readiness timeout."
}

Write-Host "READY=airflow-scheduler"

# Trino
$p = Get-ComposePort "trino" 8080

if (-not $p) {
    throw "Unable to resolve Trino port."
}

Wait-HttpEndpoint `
    -Name "trino" `
    -Url "http://127.0.0.1:$p/v1/info"

# Superset
$p = Get-ComposePort "superset" 8088

if (-not $p) {
    throw "Unable to resolve Superset port."
}

Wait-HttpEndpoint `
    -Name "superset" `
    -Url "http://127.0.0.1:$p/health"

Assert-ServiceRunning "ctu-ioc-mock-api" | Out-Null
Write-Host "READY=ctu-ioc-mock-api"

# --------------------------------------------------
# Airflow import integrity
# --------------------------------------------------

$importResult = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","list-import-errors",
    "--output","json"
)

if ($importResult.ExitCode -ne 0) {
    throw "Airflow import-error check failed."
}

$importErrors = @(Get-JsonPayload $importResult.Lines)

if ($importErrors.Count -ne 0) {
    throw "Airflow has DAG import errors."
}

Write-Host "AIRFLOW_IMPORT_ERRORS=0"

# --------------------------------------------------
# Required DAGs
# --------------------------------------------------

$dagResult = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","list",
    "--output","json"
)

if ($dagResult.ExitCode -ne 0) {
    throw "Unable to list Airflow DAGs."
}

$dags = @(Get-JsonPayload $dagResult.Lines)

$dagIds = @(
    $dags |
        ForEach-Object {
            if ($_.dag_id) { "$($_.dag_id)" }
        }
)

foreach ($requiredDag in @(
    "lakehouse_pipeline",
    "api_dataset_pipeline"
)) {
    if ($dagIds -notcontains $requiredDag) {
        throw "Required DAG missing: $requiredDag"
    }

    Write-Host "AIRFLOW_DAG_VISIBLE=$requiredDag"
}

# --------------------------------------------------
# Superset bootstrap/import
# --------------------------------------------------

& "$PSScriptRoot\import_superset.ps1"

if ($LASTEXITCODE -ne 0) {
    throw "Superset bootstrap/import failed."
}

Write-Host ""
Write-Host "Browser endpoints:"

foreach ($item in @(
    @{ Service="airflow-webserver"; Port=8080; Label="Airflow" },
    @{ Service="superset";          Port=8088; Label="Superset" },
    @{ Service="minio";             Port=9001; Label="MinIO Console" }
)) {
    $hostPort = Get-ComposePort `
        $item.Service `
        $item.Port

    if ($hostPort) {
        Write-Host "$($item.Label)=http://localhost:$hostPort"
    }
}

Write-Host ""
Write-Host "BOOTSTRAP_TEAM=PASS"

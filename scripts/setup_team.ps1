param(
    [switch]$Fresh,
    [switch]$SkipSeed,
    [switch]$PreflightOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_common.ps1"

Set-Location $script:RepoRoot

Write-Host "=================================================="
Write-Host "ONE-COMMAND TEAM SETUP"
Write-Host "=================================================="

Assert-Tool "docker"
Assert-Tool "git"

if (-not (Test-DockerEngine)) {
    throw "Docker engine is not ready. Start Docker Desktop first."
}

$envPath = Join-Path $script:RepoRoot ".env"
$examplePath = Join-Path $script:RepoRoot ".env.example"

if (-not (Test-Path $examplePath)) {
    throw ".env.example is missing."
}

if (-not (Test-Path $envPath)) {
    Copy-Item -LiteralPath $examplePath -Destination $envPath
    Write-Host "ENV_CREATED=.env"
}

function Read-DotEnvMap {
    param([string]$Path)

    $map = [ordered]@{}

    foreach ($line in @(Get-Content $Path)) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$') {
            $map[$Matches[1]] = $Matches[2].Trim()
        }
    }

    return $map
}

function Set-DotEnvDefaults {
    param(
        [string]$Path,
        [hashtable]$Defaults,
        [string[]]$ForceNames = @()
    )

    $lines = [System.Collections.Generic.List[string]]::new()
    $seen = @{}

    foreach ($line in @(Get-Content $Path)) {
        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$') {
            $name = $Matches[1]
            $current = $Matches[2].Trim().Trim('"').Trim("'")
            $seen[$name] = $true

            if (
                $Defaults.ContainsKey($name) -and
                (
                    $ForceNames -contains $name -or
                    [string]::IsNullOrWhiteSpace($current) -or
                    $current -eq "CHANGE_ME"
                )
            ) {
                $lines.Add("$name=$($Defaults[$name])")
                continue
            }
        }

        $lines.Add($line)
    }

    foreach ($name in $Defaults.Keys) {
        if (-not $seen.ContainsKey($name)) {
            $lines.Add("$name=$($Defaults[$name])")
        }
    }

    [System.IO.File]::WriteAllLines(
        $Path,
        $lines,
        [System.Text.UTF8Encoding]::new($false)
    )
}

function New-RandomSecret {
    param([int]$Bytes = 32)

    $buffer = New-Object byte[] $Bytes
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()

    try {
        $rng.GetBytes($buffer)
    }
    finally {
        $rng.Dispose()
    }

    return ([Convert]::ToBase64String($buffer)).TrimEnd('=')
}

$defaults = @{
    AIRFLOW_ADMIN_PASSWORD       = "airflow"
    AIRFLOW_API_USERNAME         = "airflow"
    AIRFLOW_WEBSERVER_URL        = "http://localhost:8080"
    GEMINI_DOCUMENT_MODELS       = "gemini-3.5-flash-lite"
    MOCK_API_INTERNAL_URL        = "http://ctu-ioc-mock-api:8000"
    MOCK_API_PUBLIC_URL          = "http://localhost:8090"
    MOCK_AUTH_REQUIRED           = "true"
    MOCK_API_USERNAME            = "lakehouse_test"
    MOCK_API_PASSWORD            = "Lakehouse@123"
    MOCK_API_TOKEN_SECRET        = (New-RandomSecret)
    MOCK_API_TOKEN_TTL_SECONDS   = "3600"
    NESSIE_API_URL               = "http://localhost:19120/api/v1"
    PG_DATABASE                  = "university_db"
    PG_HOST                      = "127.0.0.1"
    PG_PASSWORD                  = "lakehouse-postgres"
    PG_PORT                      = "5433"
    PG_USER                      = "postgres"
    S3_ACCESS_KEY                = "lakehouse"
    S3_BUCKET_NAME               = "university-lakehouse"
    S3_ENDPOINT                  = "http://127.0.0.1:9878"
    S3_REGION                    = "us-east-1"
    S3_SECRET_KEY                = "lakehouse-secret"
    AIRBYTE_PORT                 = "8006"
    TRINO_HOST                   = "127.0.0.1"
    TRINO_PORT                   = "8081"
    SUPERSET_SECRET_KEY          = (New-RandomSecret)
    SECRET_KEY                   = (New-RandomSecret)
    AIRFLOW_WEBSERVER_SECRET_KEY = (New-RandomSecret)
    API_SOURCE_CREDENTIAL_KEY    = (New-RandomSecret)
}

Set-DotEnvDefaults `
    -Path $envPath `
    -Defaults $defaults `
    -ForceNames @(
        "AIRFLOW_WEBSERVER_URL",
        "GEMINI_DOCUMENT_MODELS",
        "MOCK_API_INTERNAL_URL",
        "MOCK_API_PUBLIC_URL",
        "NESSIE_API_URL",
        "PG_HOST",
        "PG_PORT",
        "S3_ACCESS_KEY",
        "S3_BUCKET_NAME",
        "S3_ENDPOINT",
        "S3_REGION",
        "S3_SECRET_KEY",
        "TRINO_HOST",
        "TRINO_PORT"
    )

# Make this invocation deterministic even when the caller has stale variables
# left over from a previous MinIO-based development shell.
Import-RootDotEnv -Override

$envMap = Read-DotEnvMap -Path $envPath

foreach ($requiredName in @(
    "PG_PASSWORD",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_WEBSERVER_SECRET_KEY",
    "MOCK_API_TOKEN_SECRET",
    "SUPERSET_SECRET_KEY",
    "SECRET_KEY"
)) {
    $value = "$($envMap[$requiredName])".Trim().Trim('"').Trim("'")

    if ([string]::IsNullOrWhiteSpace($value) -or $value -eq "CHANGE_ME") {
        throw "Required .env value is not configured: $requiredName"
    }
}

Write-Host "ENV_READY=.env"

$legacyProjects = @(
    "lakehouse_clean",
    "lakehouse_ozone_local"
)

# Discover MinIO-era Compose projects even when the old checkout directory
# produced a different project name.
$legacyServiceIds = @(
    docker ps -aq --filter "label=com.docker.compose.service=minio"
    docker ps -aq --filter "label=com.docker.compose.service=minio-init"
) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

foreach ($containerId in @($legacyServiceIds)) {
    $container = @(
        docker inspect $containerId | ConvertFrom-Json
    )[0]

    $project = $container.Config.Labels.'com.docker.compose.project'

    if (-not [string]::IsNullOrWhiteSpace($project)) {
        $legacyProjects += "$project"
    }
}

$legacyProjects = @($legacyProjects | Select-Object -Unique)

$legacyContainerIds = @()

foreach ($project in $legacyProjects) {
    $legacyContainerIds += @(
        docker ps -aq `
            --filter "label=com.docker.compose.project=$project"
    )
}

$legacyContainerIds = @(
    $legacyContainerIds |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Select-Object -Unique
)

if ($legacyContainerIds.Count -gt 0 -and -not $Fresh) {
    Write-Host "LEGACY_RUNTIME_DETECTED=True"
    Write-Host "Run this one-time migration command if demo data may be reset:"
    Write-Host ".\scripts\setup_team.ps1 -Fresh"
    throw "Legacy MinIO/Ozone runtime must be reconciled before setup."
}

if ($Fresh) {
    Write-Host "FRESH_RESET_REQUESTED=True"
    Write-Host "FRESH_RESET_SCOPE=legacy containers and newdirection demo volumes"

    foreach ($containerId in $legacyContainerIds) {
        & docker rm -f $containerId | Out-Null

        if ($LASTEXITCODE -ne 0) {
            throw "Unable to remove legacy container $containerId"
        }
    }

    & docker compose @script:ComposeArgs down --volumes --remove-orphans

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to reset the newdirection demo runtime."
    }

    $legacyMinioVolume = "newdirection_minio_data"
    $volumeExists = @(
        docker volume ls -q --filter "name=^$legacyMinioVolume$"
    )

    if ($volumeExists -contains $legacyMinioVolume) {
        $volumeUsers = @(
            @(
                docker ps -aq --filter "volume=$legacyMinioVolume"
            ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )

        if ($volumeUsers.Count -eq 0) {
            & docker volume rm $legacyMinioVolume | Out-Null

            if ($LASTEXITCODE -ne 0) {
                throw "Unable to remove legacy MinIO volume $legacyMinioVolume"
            }
        }
    }

    Write-Host "FRESH_RESET=PASS"
}

if ($PreflightOnly) {
    & "$PSScriptRoot\bootstrap_team.ps1" -PreflightOnly

    if ($LASTEXITCODE -ne 0) {
        throw "Bootstrap preflight failed."
    }

    Write-Host "SETUP_TEAM_PREFLIGHT=PASS"
    exit 0
}

& "$PSScriptRoot\bootstrap_team.ps1"

if ($LASTEXITCODE -ne 0) {
    throw "Team bootstrap failed."
}

if (-not $SkipSeed) {
    & "$PSScriptRoot\seed_demo.ps1"

    if ($LASTEXITCODE -ne 0) {
        throw "Demo seed failed."
    }
}

& "$PSScriptRoot\verify_team_environment.ps1"

if ($LASTEXITCODE -ne 0) {
    throw "Team verification failed."
}

Write-Host ""
Write-Host "SETUP_TEAM=PASS"
Write-Host "Airflow=http://localhost:8080"
Write-Host "Superset=http://localhost:8088"
Write-Host "OzoneRecon=http://localhost:9888"
Write-Host "Backend command=.\scripts\start_backend.ps1"

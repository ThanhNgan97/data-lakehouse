Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_common.ps1"

Push-Location $script:RepoRoot

try {

    Write-Host "=================================================="
    Write-Host "TEAM STACK SAFE STOP"
    Write-Host "=================================================="

    Assert-Tool "docker"

    if (-not (Test-DockerEngine)) {
        throw "Docker engine is not ready."
    }

    $composeArgs = $script:ComposeArgs

    $before = @(
        & docker compose @composeArgs ps -a -q
    )

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to inspect Compose project before stop."
    }

    $before = @(
        $before |
            Where-Object {
                -not [string]::IsNullOrWhiteSpace($_)
            }
    )

    Write-Host "TARGET_PROJECT=$($script:ComposeProjectName)"
    Write-Host "CONTAINER_COUNT_BEFORE=$($before.Count)"

    & docker compose @composeArgs stop

    if ($LASTEXITCODE -ne 0) {
        throw "docker compose stop failed."
    }

    $running = @(
        & docker compose @composeArgs `
            ps `
            --status running `
            -q
    )

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to verify Compose runtime after stop."
    }

    $running = @(
        $running |
            Where-Object {
                -not [string]::IsNullOrWhiteSpace($_)
            }
    )

    if ($running.Count -ne 0) {
        throw "TEAM STACK STOP verification failed: containers are still running."
    }

    Write-Host ""
    Write-Host "TEAM_STACK_STOP=PASS"
    Write-Host "RUNNING_CONTAINER_COUNT=0"
    Write-Host "VOLUMES_PRESERVED=True"
    Write-Host "NO_CONTAINER_REMOVAL=True"
    Write-Host "NO_PRUNE=True"
}
finally {
    Pop-Location
}

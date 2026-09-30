param(
    [switch]$PreflightOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_common.ps1"

Set-Location $script:RepoRoot
Import-RootDotEnv

$dagId = "api_dataset_pipeline"

$datasets = @(
    "education.learning_outcomes",
    "education.teaching_progress"
)

$expectedTasks = @(
    "api_preflight",
    "ingest_bronze",
    "validate_bronze",
    "process_silver",
    "validate_silver",
    "process_gold",
    "validate_gold",
    "trino_smoke_test"
)

if (-not (Test-DockerEngine)) {
    throw "Docker engine unavailable."
}

Assert-ServiceRunning "airflow-webserver" | Out-Null

$dagSource = Join-Path `
    $script:RepoRoot `
    "lakehouse\dags\api_dataset_pipeline.py"

if (-not (Test-Path $dagSource)) {
    throw "api_dataset_pipeline.py is missing."
}

$dagText = Get-Content -Raw $dagSource

if ($dagText -notmatch 'dataset_id') {
    throw "Unable to verify dataset_id trigger contract in api_dataset_pipeline.py."
}

$dagList = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","list",
    "--output","json"
)

if ($dagList.ExitCode -ne 0) {
    throw "Unable to list Airflow DAGs."
}

$dags = @(Get-JsonPayload $dagList.Lines)

if (
    @(
        $dags |
            Where-Object { $_.dag_id -eq $dagId }
    ).Count -eq 0
) {
    throw "DAG not visible: $dagId"
}

$triggerHelp = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","trigger","--help"
)

if (
    $triggerHelp.ExitCode -ne 0 -or
    (($triggerHelp.Lines -join "`n") -notmatch '--conf')
) {
    throw "Airflow DAG trigger CLI does not expose --conf."
}

$unpauseHelp = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","unpause","--help"
)

if ($unpauseHelp.ExitCode -ne 0) {
    throw "Airflow DAG unpause CLI is unavailable."
}

Write-Host "SEED_DAG_UNPAUSE_CLI=PASS"
Write-Host "SEED_DAG=$dagId"

foreach ($dataset in $datasets) {
    Write-Host "SEED_DATASET=$dataset"
}

if ($PreflightOnly) {
    Write-Host "EXPECTED_TASK_COUNT=$($expectedTasks.Count)"
    Write-Host "SEED_DEMO_PREFLIGHT=PASS"
    exit 0
}

# A clean Airflow metadata database may create DAGs paused.
# Seed owns making only its target DAG runnable; do not
# change the global dags_are_paused_at_creation setting.
$unpause = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","unpause",$dagId
)

if ($unpause.ExitCode -ne 0) {
    foreach ($line in $unpause.Lines) {
        Write-Host $line
    }

    throw "Unable to unpause seed DAG: $dagId"
}

$dagStateResult = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","list",
    "--output","json"
)

if ($dagStateResult.ExitCode -ne 0) {
    throw "Unable to verify DAG state after unpause."
}

$dagStateRows = @(Get-JsonPayload $dagStateResult.Lines)

$targetDagState = @(
    $dagStateRows |
        Where-Object { $_.dag_id -eq $dagId }
) | Select-Object -First 1

if (-not $targetDagState) {
    throw "Seed DAG disappeared after unpause: $dagId"
}

if (
    $targetDagState.PSObject.Properties.Name -notcontains "is_paused"
) {
    throw "Unable to verify is_paused for seed DAG."
}

if ("$($targetDagState.is_paused)" -match '^(?i:true)$') {
    throw "Seed DAG remains paused after unpause: $dagId"
}

Write-Host "SEED_DAG_UNPAUSED=$dagId"
Write-Host "SEED_DAG_RUNNABLE=True"

function Get-DagRunState {
    param(
        [string]$DagId,
        [string]$RunId
    )

    $runs = Invoke-ComposeCapture @(
        "exec","-T","airflow-webserver",
        "airflow","dags","list-runs",
        "-d",$DagId,
        "--output","json"
    )

    if ($runs.ExitCode -ne 0) {
        throw "Unable to query Airflow DAG runs."
    }

    $json = @(Get-JsonPayload $runs.Lines)

    $run = @(
        $json |
            Where-Object {
                $_.run_id -eq $RunId
            }
    ) | Select-Object -First 1

    if (-not $run) {
        return $null
    }

    return "$($run.state)"
}

foreach ($dataset in $datasets) {
    $safeDataset = $dataset -replace '[^A-Za-z0-9]+','_'

    $runId = "team_seed_${safeDataset}_$([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ'))"

    $conf = @{
        dataset_id = $dataset
    } | ConvertTo-Json -Compress

    # Windows PowerShell 5.1 uses legacy native-command
    # argument serialization. Preserve embedded JSON quotes
    # when the value crosses powershell -> docker compose.
    $confArg = $conf

    if ($PSVersionTable.PSEdition -eq "Desktop") {
        $confArg = $conf.Replace('"','\"')
    }

    $trigger = Invoke-ComposeCapture @(
        "exec","-T","airflow-webserver",
        "airflow","dags","trigger",
        $dagId,
        "--run-id",$runId,
        "--conf",$confArg
    )

    if ($trigger.ExitCode -ne 0) {
        foreach ($line in $trigger.Lines) {
            Write-Host $line
        }

        throw "Failed to trigger $dataset"
    }

    Write-Host "DAG_RUN_TRIGGERED=$runId"

    $finished = $false

    for ($i = 1; $i -le 120; $i++) {
        Start-Sleep -Seconds 5

        $state = Get-DagRunState `
            -DagId $dagId `
            -RunId $runId

        if ($state) {
            Write-Host "RUN_STATE DATASET=$dataset STATE=$state"
        }

        if ($state -eq "success") {
            $finished = $true
            break
        }

        if ($state -in @("failed","upstream_failed")) {
            throw "DAG run failed for $dataset"
        }
    }

    if (-not $finished) {
        throw "DAG run timeout for $dataset"
    }

    $taskResult = Invoke-ComposeCapture @(
        "exec","-T","airflow-webserver",
        "airflow","tasks","states-for-dag-run",
        $dagId,
        $runId,
        "--output","json"
    )

    if ($taskResult.ExitCode -ne 0) {
        throw "Unable to inspect task states for $runId"
    }

    $taskRows = @(Get-JsonPayload $taskResult.Lines)

    foreach ($taskId in $expectedTasks) {
        $row = @(
            $taskRows |
                Where-Object {
                    $_.task_id -eq $taskId
                }
        ) | Select-Object -First 1

        if (-not $row) {
            throw "Expected task missing: $taskId"
        }

        if ("$($row.state)" -ne "success") {
            throw "Task not successful: $taskId state=$($row.state)"
        }

        Write-Host "TASK_PASS DATASET=$dataset TASK=$taskId"
    }
}

Write-Host "SEED_DEMO=PASS"

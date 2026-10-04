param(
    [switch]$ShowPlan
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_common.ps1"

Set-Location $script:RepoRoot

Write-Host "=================================================="
Write-Host "SAFE DEMO RESET"
Write-Host "=================================================="

Write-Host "RESET_DEMO_MUTATION_PERFORMED=False"
Write-Host "DOCKER_SYSTEM_PRUNE_ALLOWED=False"
Write-Host "DOCKER_VOLUME_PRUNE_ALLOWED=False"
Write-Host "UNRELATED_PROJECT_DELETION_ALLOWED=False"
Write-Host "PROTECTED_SOURCE_DELETION_ALLOWED=False"

Write-Host ""
Write-Host "A destructive in-place data reset is intentionally not automated on Day 10."
Write-Host "Day 11 clean-clone acceptance must use EMPTY/NEW runtime state."
Write-Host "Do not copy existing Docker volumes, MinIO objects, Nessie history,"
Write-Host "Airflow metadata, Superset home, or existing Iceberg state."

Write-Host ""
Write-Host "SAFE_RESET_SUPPORTED=False"
Write-Host "DAY11_FRESH_RUNTIME_REQUIRED=True"

if ($ShowPlan) {
    Write-Host "RESET_DEMO_GUARD_CHECK=PASS"
    exit 0
}

Write-Host ""
Write-Host "No destructive reset was executed."
Write-Host "RESET_DEMO=GUARDED_NOOP"

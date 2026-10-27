param(
    [switch]$NoReload
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_common.ps1"

Set-Location $script:RepoRoot
Import-RootDotEnv -Override

$python = Join-Path $script:RepoRoot "backend\.venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "Backend virtual environment is missing. Create backend/.venv and install backend/requirements.txt first."
}

$arguments = @(
    "-m","uvicorn","main:app",
    "--host","127.0.0.1",
    "--port","8000"
)

if (-not $NoReload) {
    $arguments += "--reload"
}

Set-Location (Join-Path $script:RepoRoot "backend")
& $python @arguments

if ($LASTEXITCODE -ne 0) {
    throw "Backend exited with code $LASTEXITCODE"
}

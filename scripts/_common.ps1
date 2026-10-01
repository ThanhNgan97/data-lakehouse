Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:RepoRoot = Split-Path -Parent $PSScriptRoot

$script:ComposeProjectName = $env:LAKEHOUSE_COMPOSE_PROJECT_NAME

if ([string]::IsNullOrWhiteSpace($script:ComposeProjectName)) {
    $script:ComposeProjectName = "newdirection"
}

$script:ComposeArgs = @(
    "-p",$script:ComposeProjectName,
    "-f",(Join-Path $script:RepoRoot "docker-compose.yml"),
    "-f",(Join-Path $script:RepoRoot "docker-compose.mock-api.yml")
)

function Assert-Tool {
    param([Parameter(Mandatory=$true)][string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required tool not found: $Name"
    }
}

function Test-DockerEngine {
    cmd /c "docker info >nul 2>nul"
    return $?
}

function Import-RootDotEnv {
    $path = Join-Path $script:RepoRoot ".env"

    if (-not (Test-Path $path)) {
        throw ".env is missing. Copy .env.example to .env and fill required local values."
    }

    foreach ($line in @(Get-Content $path)) {
        if (
            [string]::IsNullOrWhiteSpace($line) -or
            $line.TrimStart().StartsWith("#")
        ) {
            continue
        }

        if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$') {
            $name = $Matches[1]
            $value = $Matches[2].Trim()

            if (
                ($value.StartsWith('"') -and $value.EndsWith('"')) -or
                ($value.StartsWith("'") -and $value.EndsWith("'"))
            ) {
                if ($value.Length -ge 2) {
                    $value = $value.Substring(1, $value.Length - 2)
                }
            }

            $existing = [Environment]::GetEnvironmentVariable(
                $name,
                "Process"
            )

            if ([string]::IsNullOrWhiteSpace($existing)) {
                [Environment]::SetEnvironmentVariable(
                    $name,
                    $value,
                    "Process"
                )
            }
        }
    }
}

function Invoke-ComposeCapture {
    param(
        [Parameter(Mandatory=$true)]
        [string[]]$Arguments
    )

    $composeArgs = $script:ComposeArgs
    $oldEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    try {
        $lines = @(
            & docker compose @composeArgs @Arguments 2>&1 |
                ForEach-Object { "$_" }
        )

        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $oldEap
    }

    return [PSCustomObject]@{
        ExitCode = $exitCode
        Lines    = $lines
    }
}

function Invoke-ComposeChecked {
    param(
        [Parameter(Mandatory=$true)]
        [string[]]$Arguments
    )

    $composeArgs = $script:ComposeArgs

    & docker compose @composeArgs @Arguments

    if ($LASTEXITCODE -ne 0) {
        throw "docker compose command failed: $($Arguments -join ' ')"
    }
}

function Get-JsonPayload {
    param(
        [Parameter(Mandatory=$true)]
        [string[]]$Lines
    )

    $text = $Lines -join "`n"

    $arrayStart = $text.IndexOf("[")
    $arrayEnd   = $text.LastIndexOf("]")

    if (
        $arrayStart -ge 0 -and
        $arrayEnd -gt $arrayStart
    ) {
        return (
            $text.Substring(
                $arrayStart,
                $arrayEnd - $arrayStart + 1
            ) | ConvertFrom-Json
        )
    }

    $objectStart = $text.IndexOf("{")
    $objectEnd   = $text.LastIndexOf("}")

    if (
        $objectStart -ge 0 -and
        $objectEnd -gt $objectStart
    ) {
        return (
            $text.Substring(
                $objectStart,
                $objectEnd - $objectStart + 1
            ) | ConvertFrom-Json
        )
    }

    throw "No JSON payload found in command output."
}

function Get-ComposePort {
    param(
        [Parameter(Mandatory=$true)][string]$Service,
        [Parameter(Mandatory=$true)][int]$ContainerPort
    )

    $result = Invoke-ComposeCapture @(
        "port",
        $Service,
        "$ContainerPort"
    )

    if ($result.ExitCode -ne 0) {
        return $null
    }

    foreach ($line in $result.Lines) {
        if ($line -match ':(\d+)\s*$') {
            return $Matches[1]
        }
    }

    return $null
}

function Wait-HttpEndpoint {
    param(
        [Parameter(Mandatory=$true)][string]$Name,
        [Parameter(Mandatory=$true)][string]$Url,
        [int]$Attempts = 60,
        [int]$SleepSeconds = 2
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        try {
            $response = Invoke-WebRequest `
                -Uri $Url `
                -UseBasicParsing `
                -TimeoutSec 5 `
                -ErrorAction Stop

            if (
                $response.StatusCode -ge 200 -and
                $response.StatusCode -lt 500
            ) {
                Write-Host "READY=$Name HTTP=$($response.StatusCode)"
                return
            }
        }
        catch {}

        Start-Sleep -Seconds $SleepSeconds
    }

    throw "Readiness timeout: $Name ($Url)"
}

function Assert-ServiceRunning {
    param([Parameter(Mandatory=$true)][string]$Service)

    $result = Invoke-ComposeCapture @(
        "ps",
        "-q",
        $Service
    )

    $cid = @(
        $result.Lines |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    ) | Select-Object -First 1

    if ([string]::IsNullOrWhiteSpace($cid)) {
        throw "Required service is not running: $Service"
    }

    return $cid
}

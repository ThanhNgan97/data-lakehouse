Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\_common.ps1"

Set-Location $script:RepoRoot
Import-RootDotEnv

Write-Host "=================================================="
Write-Host "TEAM REPRODUCIBILITY VERIFY — READ ONLY"
Write-Host "=================================================="

if (-not (Test-DockerEngine)) {
    throw "Docker engine unavailable."
}

$requiredServices = @(
    "postgres",
    "scm",
    "om",
    "datanode-1",
    "datanode-2",
    "datanode-3",
    "s3g",
    "recon",
    "nessie",
    "airflow-webserver",
    "airflow-scheduler",
    "trino",
    "superset",
    "ctu-ioc-mock-api"
)

foreach ($service in $requiredServices) {
    Assert-ServiceRunning $service | Out-Null
    Write-Host "SERVICE_RUNNING=$service"
}

# --------------------------------------------------
# Apache Ozone runtime ownership
# --------------------------------------------------

foreach ($portSpec in @(
    @{ Port=9878; Service="s3g" },
    @{ Port=9888; Service="recon" }
)) {
    $hostPort = $portSpec.Port

    $ownerIds = @(
        & docker ps `
            --filter "publish=$hostPort" `
            --format "{{.ID}}"
    )

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to inspect Apache Ozone host port $hostPort."
    }

    $ownerIds = @(
        $ownerIds |
            Where-Object {
                -not [string]::IsNullOrWhiteSpace($_)
            }
    )

    if ($ownerIds.Count -ne 1) {
        throw "Expected exactly one owner for Apache Ozone host port $hostPort."
    }

    $ownerJson = @(
        & docker inspect $ownerIds[0]
    )

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to inspect Apache Ozone port owner."
    }

    $owner = @(
        $ownerJson |
            ConvertFrom-Json
    )[0]

    $ownerProject = $null
    $ownerService = $null

    if ($null -ne $owner.Config.Labels) {
        $ownerProject =
            $owner.Config.Labels.'com.docker.compose.project'

        $ownerService =
            $owner.Config.Labels.'com.docker.compose.service'
    }

    $ownerName =
        $owner.Name.TrimStart("/")

    if (
        $ownerProject -ne $script:ComposeProjectName -or
        $ownerService -ne $portSpec.Service
    ) {
        throw (
            "Wrong Apache Ozone runtime owner on port $hostPort. " +
            "Expected project=$($script:ComposeProjectName),service=$($portSpec.Service); " +
            "actual project=$ownerProject,service=$ownerService,name=$ownerName"
        )
    }

    Write-Host (
        "OZONE_PORT_OWNER_PASS=" +
        "PORT=$hostPort|" +
        "PROJECT=$ownerProject|" +
        "SERVICE=$ownerService|" +
        "NAME=$ownerName"
    )
}

Write-Host "OZONE_RUNTIME_OWNERSHIP=PASS"

# --------------------------------------------------
# Apache Ozone cluster + bucket readiness
# --------------------------------------------------

$datanodes = Invoke-ComposeCapture @(
    "exec","-T","scm",
    "ozone","admin","datanode","list"
)

if ($datanodes.ExitCode -ne 0) {
    throw "Unable to list Apache Ozone DataNodes."
}

$healthyDatanodes = @(
    $datanodes.Lines |
        Where-Object { $_ -match '^Health State:\s+HEALTHY\s*$' }
).Count

if ($healthyDatanodes -ne 3) {
    throw "Expected 3 healthy Apache Ozone DataNodes; found $healthyDatanodes."
}

Write-Host "OZONE_HEALTHY_DATANODES=$healthyDatanodes"

$bucket = Invoke-ComposeCapture @(
    "exec","-T","om",
    "ozone","sh","bucket","info",
    "/s3v/$env:S3_BUCKET_NAME"
)

if ($bucket.ExitCode -ne 0) {
    throw "Required Apache Ozone bucket is unavailable."
}

Write-Host "OZONE_BUCKET_PASS=$env:S3_BUCKET_NAME"

$s3Probe = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "python","-c",
    "import os,boto3; from botocore.config import Config; boto3.client('s3',endpoint_url=os.environ['S3_ENDPOINT'],aws_access_key_id=os.environ['S3_ACCESS_KEY'],aws_secret_access_key=os.environ['S3_SECRET_KEY'],config=Config(s3={'addressing_style':'path'})).head_bucket(Bucket=os.environ['S3_BUCKET_NAME'])"
)

if ($s3Probe.ExitCode -ne 0) {
    throw "Signed Apache Ozone S3 bucket probe failed."
}

Write-Host "OZONE_SIGNED_S3_PROBE=PASS"

# --------------------------------------------------
# HTTP readiness
# --------------------------------------------------

foreach ($probe in @(
    @{ Name="ozone-recon"; Service="recon";          Port=9888; Path="/" },
    @{ Name="nessie";   Service="nessie";            Port=19120; Path="/api/v2/config" },
    @{ Name="airflow";  Service="airflow-webserver"; Port=8080; Path="/health" },
    @{ Name="trino";    Service="trino";             Port=8080; Path="/v1/info" },
    @{ Name="superset"; Service="superset";          Port=8088; Path="/health" }
)) {
    $port = Get-ComposePort `
        $probe.Service `
        $probe.Port

    if (-not $port) {
        throw "Port unavailable: $($probe.Service):$($probe.Port)"
    }

    try {
        $response = Invoke-WebRequest `
            -Uri "http://127.0.0.1:$port$($probe.Path)" `
            -UseBasicParsing `
            -TimeoutSec 5 `
            -ErrorAction Stop

        Write-Host "HTTP_PASS=$($probe.Name)"
    }
    catch {
        throw "HTTP verification failed: $($probe.Name)"
    }
}

# --------------------------------------------------
# Airflow import errors + DAG visibility
# --------------------------------------------------

$imports = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","list-import-errors",
    "--output","json"
)

if ($imports.ExitCode -ne 0) {
    throw "Airflow import-error command failed."
}

$importRows = @(Get-JsonPayload $imports.Lines)

if ($importRows.Count -ne 0) {
    throw "Airflow DAG import errors detected."
}

Write-Host "AIRFLOW_IMPORT_ERRORS=0"

$dagList = Invoke-ComposeCapture @(
    "exec","-T","airflow-webserver",
    "airflow","dags","list",
    "--output","json"
)

if ($dagList.ExitCode -ne 0) {
    throw "Unable to list Airflow DAGs."
}

$dags = @(Get-JsonPayload $dagList.Lines)

foreach ($dagId in @(
    "lakehouse_pipeline",
    "api_dataset_pipeline",
    "ozone_read_validation",
    "ozone_archive_cleanup"
)) {
    if (
        @(
            $dags |
                Where-Object { $_.dag_id -eq $dagId }
        ).Count -eq 0
    ) {
        throw "Required Airflow DAG missing: $dagId"
    }

    Write-Host "AIRFLOW_DAG_PASS=$dagId"
}

# --------------------------------------------------
# Trino
# --------------------------------------------------

function Invoke-Trino {
    param([string]$Sql)

    $result = Invoke-ComposeCapture @(
        "exec","-T","trino",
        "trino",
        "--output-format","TSV",
        "--execute",$Sql
    )

    if ($result.ExitCode -ne 0) {
        foreach ($line in $result.Lines) {
            Write-Host $line
        }

        throw "Trino query failed."
    }

    return @(
        $result.Lines |
            ForEach-Object { $_.Trim() } |
            Where-Object {
                -not [string]::IsNullOrWhiteSpace($_)
            }
    )
}

function Find-Table {
    param(
        [string]$Schema,
        [string]$Preferred,
        [string]$Pattern
    )

    $tables = Invoke-Trino `
        "SHOW TABLES FROM lakehouse.$Schema"

    if ($tables -contains $Preferred) {
        return $Preferred
    }

    $candidate = @(
        $tables |
            Where-Object {
                $_ -match $Pattern
            }
    ) | Select-Object -First 1

    return $candidate
}

function Assert-DemoTable {
    param(
        [string]$DatasetLabel,
        [string]$Layer,
        [string]$Preferred,
        [string]$Pattern
    )

    $schema = $Layer.ToLowerInvariant()

    $table = Find-Table `
        -Schema $schema `
        -Preferred $Preferred `
        -Pattern $Pattern

    if (-not $table) {
        throw "$DatasetLabel $Layer table not found."
    }

    if ($table -notmatch '^[A-Za-z0-9_]+$') {
        throw "Unsafe table identifier returned by Trino."
    }

    $countOutput = Invoke-Trino `
        "SELECT COUNT(*) FROM lakehouse.$schema.$table"

    $rowCount = @(
        $countOutput |
            Where-Object { $_ -match '^\d+$' }
    ) | Select-Object -Last 1

    if (-not $rowCount) {
        throw "Unable to read row count from $schema.$table"
    }

    if ([int64]$rowCount -le 0) {
        throw "$DatasetLabel $Layer has no demo rows."
    }

    Write-Host "$DatasetLabel $Layer PASS TABLE=$table ROWS=$rowCount"
}

Assert-DemoTable `
    "LEARNING" `
    "SILVER" `
    "learning_outcomes" `
    '(?i)learning.*outcome'

Assert-DemoTable `
    "LEARNING" `
    "GOLD" `
    "learning_outcomes" `
    '(?i)learning.*outcome'

Assert-DemoTable `
    "TEACHING" `
    "SILVER" `
    "teaching_progress" `
    '(?i)teaching.*progress'

Assert-DemoTable `
    "TEACHING" `
    "GOLD" `
    "teaching_progress" `
    '(?i)teaching.*progress'

Write-Host "TRINO_LEARNING_QUERY=PASS"
Write-Host "TRINO_TEACHING_QUERY=PASS"

# --------------------------------------------------
# Superset inventory
# --------------------------------------------------

$supersetCid = Assert-ServiceRunning "superset"

$pythonPath = Join-Path `
    $env:TEMP `
    "verify_team_superset.py"

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
    "    required_dashboard_slugs = {'ctu-ioc-learning-outcomes', 'ctu-ioc-teaching-progress'}",
    "    required_dataset_names = {'learning_outcomes_metrics', 'teaching_progress_metrics'}",
    "    datasets = db.session.query(SqlaTable).all()",
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
    "    print('DATABASE_COUNT=' + str(db.session.query(Database).count()))",
    "    print('REQUIRED_DASHBOARD_COUNT=' + str(sum(1 for item in dashboards if item.slug in required_dashboard_slugs)))",
    "    print('REQUIRED_DATASET_COUNT=' + str(sum(1 for item in datasets if item.table_name in required_dataset_names)))"
)

Set-Content `
    -Path $pythonPath `
    -Value $pythonLines `
    -Encoding UTF8

& docker cp `
    $pythonPath `
    "${supersetCid}:/tmp/verify_team_superset.py" |
    Out-Null

if ($LASTEXITCODE -ne 0) {
    throw "Unable to copy Superset verification script."
}

$superset = Invoke-ComposeCapture @(
    "exec","-T","superset",
    "python","/tmp/verify_team_superset.py"
)

if ($superset.ExitCode -ne 0) {
    throw "Superset verification failed."
}

$counts = @{}

foreach ($line in $superset.Lines) {
    if (
        $line -match
        '^(DASHBOARD_COUNT|DATASET_COUNT|CHART_COUNT|FILTER_COUNT|DATABASE_COUNT|REQUIRED_DASHBOARD_COUNT|REQUIRED_DATASET_COUNT)=(\d+)$'
    ) {
        $counts[$Matches[1]] = [int]$Matches[2]
        Write-Host $line
    }
}

$supersetPass = (
    $counts["REQUIRED_DASHBOARD_COUNT"] -eq 2 -and
    $counts["REQUIRED_DATASET_COUNT"] -eq 2 -and
    $counts["CHART_COUNT"] -ge 18 -and
    $counts["FILTER_COUNT"] -ge 7 -and
    $counts["DATABASE_COUNT"] -ge 1
)

if (-not $supersetPass) {
    throw "Superset assets do not match required demo baseline."
}

Write-Host "SUPERSET_VERIFY=PASS"

Write-Host ""
Write-Host "TEAM_REPRODUCIBILITY_VERIFY=PASS"

param(
    [ValidateRange(1,10000)][int]$MaxSourceRows = 10000
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if ($repoRoot -ne 'D:\lakehouse') { throw 'This acceptance runner is scoped to D:\lakehouse' }
$branch = (& git -C $repoRoot branch --show-current).Trim()
if ($LASTEXITCODE -ne 0 -or $branch -ne 'feature/dbt-trino-integration') { throw 'Unexpected branch' }
$baselineCommit = 'b3fd26a9c4abc45fa3004008be5e660c47f04798'
$tag = [Guid]::NewGuid().ToString('N').Substring(0,10)
$outputRoot = Join-Path $repoRoot ".runtime\dbt-integration\step4\$tag"
if (Test-Path -LiteralPath $outputRoot) { throw 'Output directory already exists' }
New-Item -ItemType Directory -Path $outputRoot | Out-Null
$baselineRoot = Join-Path $outputRoot 'baseline'
New-Item -ItemType Directory -Path $baselineRoot | Out-Null
Write-Host "STEP4_DIR=$outputRoot"
$metadata = @()
foreach ($name in @('demo-airflow-scheduler','newdirection-dbt-worker-1','demo-trino','demo-nessie','demo-ozone-s3g')) {
    $raw = & docker inspect $name
    if ($LASTEXITCODE -ne 0) { throw "Missing runtime container: $name" }
    $item = ($raw | ConvertFrom-Json)[0]
    if ($item.Config.Labels.'com.docker.compose.project' -ne 'newdirection' -or -not $item.State.Running) {
        throw "Container is not running in newdirection: $name"
    }
    $metadata += [pscustomobject]@{name=$name; id=$item.Id; image=$item.Image}
}
$metadata | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $outputRoot 'runtime-before.json') -Encoding UTF8
$baselineManifest = @()
foreach ($name in @('spark_learning_outcomes_to_gold.py','spark_teaching_progress_to_gold.py')) {
    $destination = Join-Path $baselineRoot $name
    # Preserve the exact Git blob bytes; do not pipe source through PS text encoding.
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = 'git'
    $start.Arguments = '-C "' + $repoRoot + '" show ' + $baselineCommit + ':lakehouse/spark/' + $name
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $process = [Diagnostics.Process]::Start($start)
    $stream = [IO.File]::Open($destination, [IO.FileMode]::CreateNew)
    try { $process.StandardOutput.BaseStream.CopyTo($stream) } finally { $stream.Dispose() }
    $errorText = $process.StandardError.ReadToEnd()
    $process.WaitForExit()
    if ($process.ExitCode -ne 0) { throw "Cannot read pinned baseline: $name" }
    $baselineManifest += [pscustomobject]@{file=$name; commit=$baselineCommit; sha256=(Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant()}
}
$baselineManifest | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $outputRoot 'baseline-manifest.json') -Encoding UTF8
$remoteRoot = "/tmp/dbt-step4-$tag"
$remoteBaseline = "$remoteRoot/baseline"
& docker exec demo-airflow-scheduler mkdir -p $remoteBaseline
if ($LASTEXITCODE -ne 0) { throw 'Cannot prepare temporary acceptance directory' }
& docker cp (Join-Path $repoRoot 'lakehouse\spark\tests\run_dbt_gold_sandbox.py') "demo-airflow-scheduler:$remoteRoot/run_dbt_gold_sandbox.py"
if ($LASTEXITCODE -ne 0) { throw 'Cannot copy acceptance runner' }
foreach ($item in $baselineManifest) {
    & docker cp (Join-Path $baselineRoot $item.file) "demo-airflow-scheduler:$remoteBaseline/$($item.file)"
    if ($LASTEXITCODE -ne 0) { throw 'Cannot copy temporary baseline source' }
}
$logPath = Join-Path $outputRoot 'acceptance.log'
$start = [Diagnostics.ProcessStartInfo]::new()
$start.FileName = 'docker'
$start.Arguments = "exec -e PYTHONPATH=/opt/airflow/spark demo-airflow-scheduler python $remoteRoot/run_dbt_gold_sandbox.py --tag $tag --baseline-dir $remoteBaseline --output-dir $remoteRoot --max-source-rows $MaxSourceRows"
$start.UseShellExecute = $false
$start.RedirectStandardOutput = $true
$start.RedirectStandardError = $true
$process = [Diagnostics.Process]::Start($start)
$stdout = $process.StandardOutput.ReadToEndAsync()
$stderr = $process.StandardError.ReadToEndAsync()
$process.WaitForExit()
$runExit = $process.ExitCode
[IO.File]::WriteAllText($logPath, $stdout.Result, [Text.UTF8Encoding]::new($false))
[IO.File]::WriteAllText((Join-Path $outputRoot 'acceptance.stderr.log'), $stderr.Result, [Text.UTF8Encoding]::new($false))
& docker cp "demo-airflow-scheduler:$remoteRoot/report.json" (Join-Path $outputRoot 'report.json')
if ($LASTEXITCODE -ne 0) { throw "No acceptance report; retain temporary artifacts. See $logPath" }
foreach ($item in $metadata) {
    $currentId = & docker inspect --format '{{.Id}}' $item.name
    if ($LASTEXITCODE -ne 0 -or $currentId.Trim() -ne $item.id) { throw 'A runtime container changed during acceptance' }
}
Write-Host "STEP4_REPORT=$(Join-Path $outputRoot 'report.json')"
if ($runExit -ne 0) { throw "Sandbox acceptance stopped. See report.json and acceptance.log; no automatic retry." }
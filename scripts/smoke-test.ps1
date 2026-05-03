param(
    [switch]$SkipBuild,
    [switch]$RunSparkSubmit,
    [switch]$RequireAnalytics
)

$ErrorActionPreference = "Stop"

$failures = [System.Collections.Generic.List[string]]::new()
$warnings = [System.Collections.Generic.List[string]]::new()

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Add-Pass {
    param([string]$Message)
    Write-Host "[PASS] $Message" -ForegroundColor Green
}

function Add-Fail {
    param([string]$Message)
    $failures.Add($Message)
    Write-Host "[FAIL] $Message" -ForegroundColor Red
}

function Add-Warn {
    param([string]$Message)
    $warnings.Add($Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Test-HttpEndpoint {
    param(
        [string]$Url,
        [string]$Name
    )

    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 10
        if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
            Add-Pass "$Name is reachable at $Url"
        }
        else {
            Add-Fail "$Name returned status code $($response.StatusCode)"
        }
    }
    catch {
        Add-Fail "$Name is not reachable at $Url"
    }
}

function Test-KafkaMessage {
    param(
        [string]$Topic,
        [int]$TimeoutMs = 12000,
        [switch]$WarningOnly
    )

    $consumeCmd = "kafka-console-consumer --bootstrap-server kafka:9092 --topic $Topic --max-messages 1 --timeout-ms $TimeoutMs"
    $output = docker exec kafka bash -lc $consumeCmd 2>&1

    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace(($output | Out-String))) {
        Add-Pass "Kafka topic '$Topic' has messages"
        return $true
    }

    if ($WarningOnly) {
        Add-Warn "No message observed yet on topic '$Topic'"
    }
    else {
        Add-Fail "No message observed on topic '$Topic'"
    }

    return $false
}

Write-Step "Preflight checks"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker command not found. Install Docker Desktop first." -ForegroundColor Red
    exit 1
}

if (-not (Test-Path ".env")) {
    Add-Fail "Missing .env file. Create it from .env.example before running smoke test"
}
else {
    Add-Pass ".env file exists"
}

try {
    docker compose config > $null
    Add-Pass "docker-compose.yml is valid"
}
catch {
    Add-Fail "docker compose config validation failed"
}

Write-Step "Starting containers"
$upArgs = @("compose", "--profile", "app", "up", "-d")
if (-not $SkipBuild) {
    $upArgs += "--build"
}

docker @upArgs > $null
if ($LASTEXITCODE -eq 0) {
    Add-Pass "docker compose stack started"
}
else {
    Add-Fail "docker compose up failed"
}

Write-Step "Verifying required services"
$requiredServices = @(
    "zookeeper",
    "kafka",
    "kafka-ui",
    "spark",
    "spark-worker",
    "hackernews-producer",
    "newsapi-producer",
    "youtube-producer",
    "nlp-service",
    "dashboard"
)

$runningServices = docker compose ps --services --filter status=running
$missingServices = $requiredServices | Where-Object { $_ -notin $runningServices }
if ($missingServices.Count -eq 0) {
    Add-Pass "All required services are running"
}
else {
    Add-Fail ("Missing running services: " + ($missingServices -join ", "))
}

Write-Step "Checking web endpoints"
Test-HttpEndpoint -Url "http://localhost:8081" -Name "Kafka UI"
Test-HttpEndpoint -Url "http://localhost:8501" -Name "Streamlit dashboard"
Test-HttpEndpoint -Url "http://localhost:8090" -Name "Spark UI"

Write-Step "Checking Kafka data flow"
$topics = docker exec kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --list" 2>&1
if ($LASTEXITCODE -eq 0) {
    Add-Pass "Kafka is reachable from broker container"
}
else {
    Add-Fail "Kafka broker command failed"
}

Test-KafkaMessage -Topic "raw.hackernews"
Test-KafkaMessage -Topic "enriched.nlp"

if ($RunSparkSubmit) {
    Write-Step "Submitting Spark job"
    $sparkCmd = "/opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/spark_streaming.py > /tmp/spark_smoke.log 2>&1 &"
    docker exec spark bash -lc $sparkCmd > $null
    if ($LASTEXITCODE -eq 0) {
        Add-Pass "Spark job submitted in background"
    }
    else {
        Add-Fail "Spark submit failed"
    }
}

$analyticsRequired = $RequireAnalytics -or $RunSparkSubmit
if ($analyticsRequired) {
    Test-KafkaMessage -Topic "analytics.sentiment"
}
else {
    Test-KafkaMessage -Topic "analytics.sentiment" -WarningOnly
}

Write-Step "Smoke test summary"
if ($warnings.Count -gt 0) {
    Write-Host "Warnings:" -ForegroundColor Yellow
    $warnings | ForEach-Object { Write-Host " - $_" -ForegroundColor Yellow }
}

if ($failures.Count -gt 0) {
    Write-Host "Failures:" -ForegroundColor Red
    $failures | ForEach-Object { Write-Host " - $_" -ForegroundColor Red }
    exit 1
}

Write-Host "All required checks passed." -ForegroundColor Green
exit 0

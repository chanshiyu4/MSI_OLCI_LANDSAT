param(
    [string]$Sensors = "MSI,OLCI,LANDSAT",
    [switch]$NoProducer,
    [ValidateRange(0, 2147483647)][int]$DownloadWorkers = 1,
    [ValidateRange(0, 2147483647)][int]$AcoliteWorkers = 1,
    [ValidateRange(0, 2147483647)][int]$PostprocessWorkers = 1
)

$ErrorActionPreference = "Stop"
$python = (Get-Command python -CommandType Application).Source
$env:ENABLED_SENSOR_KEYS = $Sensors

$logDir = Join-Path $PSScriptRoot "logs"
$null = New-Item -ItemType Directory -Path $logDir -Force
$runId = "$(Get-Date -Format yyyyMMdd-HHmmss)-$PID"

function Start-PipelineWorkers([string]$Stage, [int]$Count) {
    for ($index = 1; $index -le $Count; $index++) {
        $logPath = Join-Path $logDir "$runId-$Stage-$index.log"
        $worker = Start-Process -FilePath $python -ArgumentList "-X", "utf8", "-u", "worker_$Stage.py" `
            -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -RedirectStandardError $logPath -PassThru
        Write-Host "$Stage worker PID $($worker.Id)  $logPath"
    }
}

Start-PipelineWorkers "download" $DownloadWorkers
Start-PipelineWorkers "acolite" $AcoliteWorkers
Start-PipelineWorkers "postprocess" $PostprocessWorkers
if (-not $NoProducer) {
    & $python (Join-Path $PSScriptRoot "monitor_producer.py")
    exit $LASTEXITCODE
}

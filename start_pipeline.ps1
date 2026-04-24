param(
    [string]$Sensors = "MSI,OLCI,LANDSAT",
    [switch]$NoProducer
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$env:ENABLED_SENSOR_KEYS = $Sensors
Write-Host "ENABLED_SENSOR_KEYS=$($env:ENABLED_SENSOR_KEYS)"

Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd `"$scriptDir`"; python worker_acolite.py"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd `"$scriptDir`"; python worker_postprocess.py"

if (-not $NoProducer) {
    Start-Sleep -Seconds 3
    python monitor_producer.py
}

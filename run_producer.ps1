param(
    [string]$Sensors = "MSI,OLCI,LANDSAT"
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$env:ENABLED_SENSOR_KEYS = $Sensors
Write-Host "ENABLED_SENSOR_KEYS=$($env:ENABLED_SENSOR_KEYS)"
python monitor_producer.py

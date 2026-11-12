param(
    [string]$Sensors = "MSI,OLCI,LANDSAT"
)

$ErrorActionPreference = "Stop"
$env:ENABLED_SENSOR_KEYS = $Sensors
python (Join-Path $PSScriptRoot "monitor_producer.py")
exit $LASTEXITCODE

param(
    [switch]$Messaging
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    if ($Messaging) {
        docker compose --profile messaging up -d --wait postgres kafka
    } else {
        docker compose up -d --wait postgres
    }
    docker compose ps
} finally {
    Pop-Location
}

param(
    [string]$BaseUrl = 'http://localhost:8080'
)

$ErrorActionPreference = 'Stop'
$headers = @{
    'Idempotency-Key' = "demo-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
    'X-Correlation-Id' = 'local-demo'
}
$runAt = [DateTimeOffset]::UtcNow.AddHours(1).ToString('o')
$body = @{
    type = 'GENERATE_REPORT'
    priority = 10
    payload = @{ report = 'demo' }
    runAt = $runAt
    executionTimeoutSeconds = 30
} | ConvertTo-Json -Depth 5

Write-Host 'Submitting a future job...'
$job = Invoke-RestMethod -Method Post -Uri "$BaseUrl/jobs" `
    -Headers $headers -ContentType 'application/json' -Body $body
$job | ConvertTo-Json -Depth 5

Write-Host 'Repeating the same idempotent submission...'
$sameJob = Invoke-RestMethod -Method Post -Uri "$BaseUrl/jobs" `
    -Headers $headers -ContentType 'application/json' -Body $body
if ($sameJob.id -ne $job.id) {
    throw 'Idempotent retry returned a different job ID'
}

Write-Host 'Cancelling before eligibility...'
$cancelBody = @{
    requestedBy = 'demo-operator'
    reason = 'demonstrating pending cancellation'
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$BaseUrl/jobs/$($job.id)/cancel" `
    -ContentType 'application/json' -Body $cancelBody | ConvertTo-Json -Depth 5

Write-Host 'Reading Prometheus metrics...'
$metrics = Invoke-WebRequest -UseBasicParsing -Uri "$BaseUrl/actuator/prometheus"
$metrics.Content -split "`n" |
    Where-Object { $_ -match '^job_platform_' } |
    Select-Object -First 20

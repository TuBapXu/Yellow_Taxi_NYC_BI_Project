# ==============================================================================
# SCRIPT CHẠY BULK ETL PIPELINE (36 THÁNG) TỐC ĐỘ CAO (TASK T03/T04/S03)
# ==============================================================================
param (
    [string]$Year = "",
    [int]$MaxFiles = 0,
    [switch]$ForceReload,
    [switch]$SkipDims,
    [switch]$RunTests
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "🚀 KHỞI ĐỘNG BULK PIPELINE RUNNER (MAGE + DBT + TRANSIENT STAGING)" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

$cmdArgs = @("python3", "/home/src/scripts/run_bulk_pipeline.py")

if ($Year -ne "") {
    $cmdArgs += @("--year", $Year)
}
if ($MaxFiles -gt 0) {
    $cmdArgs += @("--max-files", $MaxFiles)
}
if ($ForceReload) {
    $cmdArgs += "--force-reload"
}
if ($SkipDims) {
    $cmdArgs += "--skip-dims"
}
if ($RunTests) {
    $cmdArgs += "--run-tests"
}

$cmdStr = $cmdArgs -join " "
Write-Host "Lệnh thực thi trong container: $cmdStr" -ForegroundColor Yellow

docker compose exec -T mage sh -c "cd /home/src && $cmdStr"

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n✅ [HOÀN TẤT THÀNH CÔNG] Quá trình nạp dữ liệu hoàn tất!" -ForegroundColor Green
} else {
    Write-Host "`n❌ [LỖI] Có sự cố trong quá trình thực thi (Exit code: $LASTEXITCODE)" -ForegroundColor Red
}

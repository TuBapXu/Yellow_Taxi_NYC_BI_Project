param (
    [string]$FilePattern = "yellow_tripdata_*.parquet",
    [int]$MaxFiles = 0
)

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "   KHOI CHAY TASK S03 (STINH): NAP MO RONG 36 THANG & AUDIT DIMS      " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan

$maxArg = ""
if ($MaxFiles -gt 0) {
    $maxArg = "--max-files $MaxFiles"
}

$cmd = "cd /home/src && PYTHONUNBUFFERED=1 python3 -u scripts/run_s03_batch_36m.py --file-pattern '$FilePattern' $maxArg --keep-last-staging"
docker compose exec -T mage sh -c $cmd

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[THANH CONG] Task S03 da hoan tat! Kiem tra bao cao tai:" -ForegroundColor Green
    Write-Host "  - docs/dim_coverage_report.md" -ForegroundColor Yellow
    Write-Host "  - docs/manifest_36_months.csv" -ForegroundColor Yellow
} else {
    Write-Host "`n[LOI] Qua trinh chay Task S03 bi gian doan. Chay lai lenh de tu dong Resume." -ForegroundColor Red
}
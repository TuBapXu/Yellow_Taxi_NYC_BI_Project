# ==============================================================================
# SCRIPT THỰC THI TASK T03: MASTER ETL PIPELINE & TEST 1 THÁNG MẪU (2021-01)
# ==============================================================================
param (
    [string]$TargetFile = "yellow_tripdata_2021-01.parquet"
)

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "   KHỞI CHẠY TASK T03: MASTER ETL PIPELINE (TTU) TRÊN MAGE CONTAINER   " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "Tệp đích: $TargetFile" -ForegroundColor Yellow

$cmd = "cd /home/src && python3 scripts/run_master_pipeline.py $TargetFile"
docker compose exec -T mage sh -c $cmd

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[THÀNH CÔNG] Pipeline T03 đã thực thi xong và xuất file benchmark_1m.csv!" -ForegroundColor Green
} else {
    Write-Host "`n[LỖI] Pipeline T03 gặp sự cố trong quá trình chạy." -ForegroundColor Red
}

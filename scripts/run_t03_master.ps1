# ==============================================================================
# SCRIPT THỰC THI TASK T03: MASTER ETL PIPELINE & TEST 1 THÁNG (INCREMENTAL)
# ==============================================================================
param (
    [string]$TargetFile = "yellow_tripdata_2021-01.parquet",
    [switch]$ForceReload
)

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "   KHỞI CHẠY MASTER ETL PIPELINE (INCREMENTAL) TRÊN MAGE CONTAINER     " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "Tệp đích: $TargetFile" -ForegroundColor Yellow
if ($ForceReload) {
    Write-Host "Chế độ: NẠP ĐÈ BẮT BUỘC (--force-reload)" -ForegroundColor Magenta
} else {
    Write-Host "Chế độ: NẠP LŨY TIẾN TIÊU CHUẨN (Incremental)" -ForegroundColor Green
}

$flag = if ($ForceReload) { "--force-reload" } else { "" }
$cmd = "cd /home/src && python3 scripts/run_master_pipeline.py $TargetFile $flag"
docker compose exec -T mage sh -c $cmd

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n[THÀNH CÔNG] Pipeline đã thực thi xong và xuất file benchmark_1m.csv!" -ForegroundColor Green
} else {
    Write-Host "`n[LỖI] Pipeline gặp sự cố trong quá trình chạy." -ForegroundColor Red
}

# ==============================================================================
# Chay Task B03 (nbinh): 13 truy van CB/MR + audit DQ tren dds."FactTaxiTrip"
# Ket qua: reports\b03\ (CSV tung cau, dq_audit_3y.xlsx, MR05_zones.geojson, run_summary.md)
# Log: logs\b03_*.log  -  Chi DOC du lieu, khong ghi vao database.
# Chay lai sau S03 (du 36 file) de ra so lieu 3 nam, khong can sua SQL.
# ==============================================================================
# Tham so -CrossCheck: tinh lai cac chi so tu Parquet goc bang pandas de so voi SQL (kiem chung doc lap).
#   Can RAM ~1,5 GB cho moi 4 trieu dong -> chi dung khi Fact co it file (vd 2 file thu), KHONG dung cho 36 file.
param([switch]$CrossCheck)
$ErrorActionPreference = 'Continue'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
New-Item -ItemType Directory -Force -Path (Join-Path $root 'logs') | Out-Null
$log = Join-Path $root ("logs\b03_" + (Get-Date -Format 'yyyyMMdd_HHmmss') + ".log")

function Log($s) { "$s" | Out-File -FilePath $log -Append -Encoding utf8; Write-Host $s }

Log "B03 start: $(Get-Date)  |  Folder: $root"
docker version --format "Server={{.Server.Version}}" *> $null
if ($LASTEXITCODE -ne 0) { Log "!!! Docker Desktop chua chay. Mo Docker Desktop roi chay lai."; exit 1 }

docker compose up -d 2>&1 | ForEach-Object { Log "$_" }
docker compose exec -T mage sh -c "cd /home/src && python3 scripts/b03_export.py" 2>&1 | ForEach-Object { Log "$_" }
$code = $LASTEXITCODE
if ($CrossCheck -and $code -eq 0) {
    Log ""; Log "===== Kiem chung doc lap tu Parquet goc ====="
    docker compose exec -T mage sh -c "cd /home/src && python3 scripts/b03_crosscheck.py" 2>&1 | ForEach-Object { Log "$_" }
    $code = $LASTEXITCODE
}

Log ""
if ($code -eq 0) { Log "B03 XONG - tat ca tu kiem PASS. Ket qua: reports\b03\" }
else { Log "B03 CO LOI (exit=$code). Xem log: $log" }
exit $code

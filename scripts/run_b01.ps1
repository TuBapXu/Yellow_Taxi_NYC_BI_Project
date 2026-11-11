# ==============================================================================
# Chay Task B01 (nbinh): dbt staging stg_yellow_trips tren MOCK roi tren du lieu THAT
# Log ghi vao logs\b01_*.log  -  Chi chay tren may local
# ==============================================================================
$ErrorActionPreference = 'Continue'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
New-Item -ItemType Directory -Force -Path (Join-Path $root 'logs') | Out-Null
$log = Join-Path $root ("logs\b01_" + (Get-Date -Format 'yyyyMMdd_HHmmss') + ".log")

function Log($s) { "$s" | Out-File -FilePath $log -Append -Encoding utf8; Write-Host $s }
function Step($title, [scriptblock]$sb) {
    Log ""; Log "===== $title ====="
    try { & $sb 2>&1 | ForEach-Object { Log "$_" }; Log "[exit=$LASTEXITCODE]" }
    catch { Log "ERROR: $_" }
}
function Sql($title, $q) { Step $title { docker compose exec -T postgres psql -U postgres -d DW_DB -c $q } }

Log "B01 start: $(Get-Date)  |  Folder: $root"
Step "0. Docker version" { docker version --format "Server={{.Server.Version}}" }
if ($LASTEXITCODE -ne 0) { Log "!!! Docker Desktop chua chay. Mo Docker Desktop roi chay lai RUN_B01.bat"; Read-Host "Nhan Enter de dong"; exit 1 }

Step "1. docker compose up -d" { docker compose up -d }
if ($LASTEXITCODE -ne 0) { Log "!!! docker compose up loi. Dung lai."; Read-Host "Nhan Enter de dong"; exit 1 }

Step "2. dbt seed / run / test (MOCK roi THAT)" { docker compose exec -T mage bash /home/src/scripts/b01_in_container.sh }

Sql "3. Doi soat raw = stg + rejects" "SELECT (SELECT COUNT(*) FROM staging.raw_yellow_trips) AS raw_rows, (SELECT COUNT(*) FROM staging.stg_yellow_trips) AS stg_rows, (SELECT COUNT(*) FROM staging.stg_yellow_trips_rejects) AS reject_rows, (SELECT COUNT(*) FROM staging.quarantine) AS quarantine_table_rows;"
Sql "4. Theo file nguon" "SELECT source_file_name, COUNT(*) AS stg_rows, COUNT(*) FILTER (WHERE passenger_count IS NULL) AS null_passenger, COUNT(*) FILTER (WHERE trip_duration_seconds <= 0) AS duration_le_0, COUNT(*) FILTER (WHERE trip_duration_seconds > 86400) AS duration_gt_24h, ROUND(AVG(trip_duration_seconds)) AS avg_duration_s, SUM(total_amount) AS sum_total_amount FROM staging.stg_yellow_trips GROUP BY 1 ORDER BY 1;"
Sql "4b. Khong mat airport_fee giua 2 schema + tien bi lam tron" "SELECT source_file_name, COUNT(airport_fee) AS airport_fee_not_null, SUM(airport_fee) AS sum_airport_fee, COUNT(money_rounded_columns) AS rows_money_rounded, COUNT(*) FILTER (WHERE pickup_datetime::date NOT BETWEEN DATE '2020-12-31' AND DATE '2024-01-01') AS pickup_out_of_domain FROM staging.stg_yellow_trips GROUP BY 1 ORDER BY 1;"
Sql "5. Ly do quarantine" "SELECT quarantine_reason, COUNT(*) FROM staging.quarantine GROUP BY 1 ORDER BY 2 DESC;"
Sql "5b. Chi tiet loi theo cot (top 20)" "SELECT e AS loi_cot, COUNT(*) FROM staging.quarantine, jsonb_array_elements_text(raw_payload->'_cast_errors') e GROUP BY 1 ORDER BY 2 DESC LIMIT 20;"
Sql "6. Mau 5 dong stg_yellow_trips" "SELECT trip_id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, store_and_fwd_flag, payment_type, total_amount, trip_duration_seconds FROM staging.stg_yellow_trips LIMIT 5;"

Log ""; Log "B01 xong: $(Get-Date). Log: $log"
Read-Host "Xong! Nhan Enter de dong cua so"

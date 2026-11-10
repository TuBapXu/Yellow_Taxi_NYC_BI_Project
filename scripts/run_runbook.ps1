# ==============================================================================
# Chay tu dong Runbook T01 (Buoc 2, 3.1-3.4) va Runbook S01 (Buoc 4.1-4.3)
# Log ghi vao thu muc logs\  -  Chi chay tren may local, khong dong cham GitHub
# ==============================================================================
$ErrorActionPreference = 'Continue'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
New-Item -ItemType Directory -Force -Path (Join-Path $root 'logs') | Out-Null
$log = Join-Path $root ("logs\runbook_" + (Get-Date -Format 'yyyyMMdd_HHmmss') + ".log")

function Log($s) { "$s" | Out-File -FilePath $log -Append -Encoding utf8; Write-Host $s }
function Step($title, [scriptblock]$sb) {
    Log ""; Log "===== $title ====="
    try { & $sb 2>&1 | ForEach-Object { Log "$_" }; Log "[exit=$LASTEXITCODE]" }
    catch { Log "ERROR: $_" }
}

Log "Runbook start: $(Get-Date)  |  Folder: $root"

Step "0. Docker version" { docker version --format "Client={{.Client.Version}} Server={{.Server.Version}}" }
if ($LASTEXITCODE -ne 0) { Log "!!! Docker Desktop chua chay. Hay mo Docker Desktop roi chay lai file RUN_RUNBOOK.bat"; Read-Host "Nhan Enter de dong"; exit 1 }

$dataDir = [IO.Path]::GetFullPath((Join-Path $root "..\..\Data"))
Step "S01-4.1 Tai du lieu mau vao $dataDir" { & (Join-Path $PSScriptRoot "download_data.ps1") -DataDir $dataDir }

if (-not (Test-Path .env)) { Copy-Item .env.example .env; Log "Da tao .env tu .env.example" }

Step "T01-2a. docker compose config --quiet" { docker compose config --quiet }
Step "T01-2b. docker compose up -d --build" { docker compose up -d --build }
if ($LASTEXITCODE -ne 0) { Log "!!! docker compose up loi (xem dong Error o tren). Dung lai."; Read-Host "Nhan Enter de dong"; exit 1 }

Log ""; Log "===== Cho PostgreSQL healthy ====="
$h = ''
for ($i = 0; $i -lt 60; $i++) {
    $h = (docker inspect -f "{{.State.Health.Status}}" lab-postgres 2>$null)
    if ($h -eq 'healthy') { break }
    Start-Sleep -Seconds 5
}
Log "lab-postgres health = $h"
if ($h -ne 'healthy') { Step "postgres logs" { docker compose logs --tail 80 postgres } }

Step "T01-2c. docker compose ps" { docker compose ps }
Step "T01-3.1 verify_t01.sql" { Get-Content postgres\verify_t01.sql | docker compose exec -T postgres psql -U postgres -d DW_DB }
Step "T01-3.2 dbt debug" { docker compose exec -T mage sh -c "cd /home/src/dbt_project && dbt debug --profiles-dir ." }
Step "T01-3.3a Python libraries" { docker compose exec -T mage python3 -c "import pyarrow, geopandas, shapely, psycopg2, dbt; print('==> ALL LIBRARIES READY!'); print('pyarrow', pyarrow.__version__, '| geopandas', geopandas.__version__, '| shapely', shapely.__version__)" }
Step "T01-3.3b Mount du lieu nguon" { docker compose exec -T mage ls -la /home/src/external_data }

Log ""; Log "===== T01-3.4 Mage Web UI ====="
$mp = (Get-Content .env | Where-Object { $_ -match "^MAGE_PORT=" }) -replace "MAGE_PORT=",""
if (-not $mp) { $mp = "6789" }
$ok = $false
for ($i = 0; $i -lt 36; $i++) {
    try { $r = Invoke-WebRequest -Uri "http://localhost:$mp" -UseBasicParsing -TimeoutSec 5; Log "HTTP $($r.StatusCode) - Mage UI san sang"; $ok = $true; break }
    catch { Start-Sleep -Seconds 5 }
}
if (-not $ok) { Log "Mage UI chua phan hoi sau 3 phut"; Step "mage logs" { docker compose logs --tail 80 mage } }

Step "S01-4.2 Pipeline S01 (ingest_zones, manifest, nap thu 2021-01 / 2023-02)" { docker compose exec -T mage bash /home/src/scripts/s01_in_container.sh }
Step "S01-4.3 meta.file_manifest" { docker compose exec -T postgres psql -U postgres -d DW_DB -c "SELECT file_name, source_schema_version, load_status, total_rows AS footer_rows, staged_rows FROM meta.file_manifest ORDER BY file_name;" }
Step "S01 dem dong staging" { docker compose exec -T postgres psql -U postgres -d DW_DB -c "SELECT 'raw_yellow_trips' t, COUNT(*) FROM staging.raw_yellow_trips UNION ALL SELECT 'raw_taxi_zone_lookup', COUNT(*) FROM staging.raw_taxi_zone_lookup UNION ALL SELECT 'raw_taxi_zones_shp', COUNT(*) FROM staging.raw_taxi_zones_shp;" }

Log ""; Log "Runbook xong: $(Get-Date). Log: $log"
Read-Host "Xong! Nhan Enter de dong cua so"

# Tai du lieu mau cho S01 tu trang chinh thuc NYC TLC vao thu muc Data (..\..\Data so voi du an)
param([string]$DataDir)
$base = "https://d37ci6vzurychx.cloudfront.net"
$files = @(
  @{ n = "yellow_tripdata_2021-01.parquet"; u = "$base/trip-data/yellow_tripdata_2021-01.parquet" },
  @{ n = "yellow_tripdata_2023-02.parquet"; u = "$base/trip-data/yellow_tripdata_2023-02.parquet" },
  @{ n = "taxi_zone_lookup.csv";            u = "$base/misc/taxi_zone_lookup.csv" },
  @{ n = "taxi_zones.zip";                  u = "$base/misc/taxi_zones.zip" }
)
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
foreach ($f in $files) {
  $out = Join-Path $DataDir $f.n
  if ((Test-Path $out) -and ((Get-Item $out).Length -gt 1000)) { Write-Output "Da co san: $($f.n)"; continue }
  Write-Output "Dang tai: $($f.n) ..."
  $tmp = "$out.part"
  try {
    if (Get-Command curl.exe -ErrorAction SilentlyContinue) {
      & curl.exe -L -f -s -S --retry 3 -o $tmp $f.u
      if ($LASTEXITCODE -ne 0) { throw "curl exit $LASTEXITCODE" }
    } else {
      $ProgressPreference = 'SilentlyContinue'
      Invoke-WebRequest -Uri $f.u -OutFile $tmp -UseBasicParsing
    }
    Move-Item -Force $tmp $out
    Write-Output ("  -> OK {0:N1} MB" -f ((Get-Item $out).Length / 1MB))
  } catch {
    Write-Output "  -> LOI tai $($f.n): $_"
    if (Test-Path $tmp) { Remove-Item -Force $tmp }
  }
}
# Giai nen shapefile (GDAL khong doc truc tiep duoc taxi_zones.zip cua TLC)
$zip = Join-Path $DataDir "taxi_zones.zip"
$hasShp = Get-ChildItem -Path $DataDir -Recurse -Filter "taxi_zones.shp" -ErrorAction SilentlyContinue
if ((Test-Path $zip) -and -not $hasShp) {
  Write-Output "Giai nen taxi_zones.zip ..."
  Expand-Archive -Path $zip -DestinationPath (Join-Path $DataDir "taxi_zones") -Force
}
Get-ChildItem -Path $DataDir -Recurse -Filter "taxi_zones.*" | ForEach-Object { "  shp part: " + $_.FullName.Substring($DataDir.Length) }
Get-ChildItem $DataDir | ForEach-Object { "{0,-40} {1,10:N1} MB" -f $_.Name, ($_.Length / 1MB) }

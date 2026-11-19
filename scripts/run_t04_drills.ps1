# ==============================================================================
# SCRIPT THỰC THI TASK T04 (1-CLICK EXECUTION): DIỄN TẬP RERUN & PHỤC HỒI SỰ CỐ
# Người thực hiện: Phạm Lê Thanh Tú (MSSV: 23120388 - TTU - Nhóm trưởng Nhóm 09)
# Đề tài: Hệ thống BI Phân tích lộ trình NYC Yellow Taxi (CSC12107 - FIT HCMUS)
# ==============================================================================

$ErrorActionPreference = 'Continue'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

# Tạo thư mục logs nếu chưa có
$logDir = Join-Path $root 'logs'
if (!(Test-Path $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}
$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = Join-Path $logDir "t04_drills_$timestamp.log"

function Write-Log {
    param([string]$Message, [string]$Color = "White")
    $logMsg = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    $logMsg | Out-File -FilePath $logFile -Append -Encoding utf8
    Write-Host $Message -ForegroundColor $Color
}

Write-Log "======================================================================" "Cyan"
Write-Log "   KHỞI CHẠY TASK T04: DIỄN TẬP RERUN, BẤT BIẾN & PHỤC HỒI SỰ CỐ     " "Cyan"
Write-Log "   (Thực thi trên kho dữ liệu thật 108,870,632 dòng của Nhóm 09)      " "Cyan"
Write-Log "======================================================================" "Cyan"
Write-Log "Thư mục làm việc: $root" "Yellow"
Write-Log "Tập tin nhật ký (Log): $logFile" "Yellow"

# 1. Kiểm tra Docker
docker version --format "Server={{.Server.Version}}" *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Log "[LỖI] Docker Desktop chưa khởi động. Vui lòng bật Docker Desktop và thử lại!" "Red"
    exit 1
}

# 2. Đảm bảo containers lab-postgres và lab-mage đang hoạt động
Write-Log "`n[BƯỚC 1] Kiểm tra trạng thái containers lab-postgres và lab-mage..." "Yellow"
docker compose ps 2>&1 | ForEach-Object { Write-Log "$_" "Gray" }

# 3. Kích hoạt diễn tập qua test_t04_drills.py
Write-Log "`n[BƯỚC 2] Bắt đầu thực thi 5 kịch bản diễn tập trong container lab-mage..." "Yellow"
$cmd = "cd /home/src && python3 scripts/test_t04_drills.py"
docker compose exec -T mage sh -c $cmd 2>&1 | ForEach-Object {
    if ($_ -match "\[KET QUA.*\]: PASS") {
        Write-Log "$_" "Green"
    } elseif ($_ -match "CRITICAL ERROR|FAILED|AssertionError") {
        Write-Log "$_" "Red"
    } elseif ($_ -match "KICH BAN") {
        Write-Log "$_" "Cyan"
    } else {
        Write-Log "$_" "White"
    }
}

$exitCode = $LASTEXITCODE

Write-Log "`n======================================================================" "Cyan"
if ($exitCode -eq 0) {
    Write-Log ">>> [HOÀN TẤT THÀNH CÔNG] TẤT CẢ CÁC KỊCH BẢN DIỄN TẬP T04 ĐỀU PASS! <<<" "Green"
    Write-Log "Bằng chứng số liệu đã được ghi nhận vào: $logFile" "Green"
} else {
    Write-Log ">>> [CẢNH BÁO / LỖI] Quá trình diễn tập T04 gặp sự cố (Exit Code: $exitCode). Xem chi tiết tại log!" "Red"
}
Write-Log "======================================================================" "Cyan"

exit $exitCode

# Nghiệm thu tích hợp T01 / S01 / B01 / B02 / S02

## 1. Phạm vi và quyết định

Ngày thực hiện: 17/11/2026. Working tree nhánh `ttu`; chưa commit/push.
Tích hợp mã B02 từ `origin/Binh_02` (`5c4ed7c`) và S02 từ
`origin/feature/stinh-task-s02` (`0889063`), không tạo merge commit.

- Hai seed `seed_vendor_mapping` và `seed_rate_code_mapping` là nguồn mapping chung.
- Macro `mapping_is_active`: chỉ `VERIFIED` trong dữ liệu thật; thêm `FIXTURE` khi `b01_use_mock=true`.
- RateCode 99: mock -> FK 99; thật -> `UNMAPPED_RATE_CODE`, FK 0. Không đổi các cờ usable chỉ vì mã chưa xác minh.
- DimRateCode: mock 8 dòng, thật 7 dòng; DimVendor 3 dòng, gồm khóa 0.
- Fact giữ nguyên 40 cột: 1 PK + 11 FK + 3 retained + 13 measures + 7 DQ + 5 lineage.

## 2. Cách chạy lại

Mở PowerShell tại root repo, Docker và hai service phải đang chạy. Dữ liệu nguồn được mount từ `../../Data`.
Dùng **tên database mới** cho mỗi lần chuẩn bị; script từ chối ghi đè database có sẵn.

```powershell
$db = 'taxi_acceptance_20261117_rerun'
docker compose exec -T mage python -u scripts/prepare_acceptance.py $db
if ($LASTEXITCODE -ne 0) { throw 'Prepare failed' }
docker compose exec -T mage python -u scripts/run_integration_acceptance.py $db mock
if ($LASTEXITCODE -ne 0) { throw 'Mock failed' }
docker compose exec -T mage python -u scripts/run_integration_acceptance.py $db sample
if ($LASTEXITCODE -ne 0) { throw 'Sample failed' }
```

Các script chỉ nhận tên `taxi_acceptance_*`. Chuẩn bị tạo DB mới, DDL, nạp lookup/shapefile và đúng hai file 2021-01, 2023-02.
Mỗi phase chạy seed, staging, dimension, projection nghiệm thu rồi toàn bộ dbt tests; dừng ngay khi lệnh lỗi.
Không cần `force_reload` hay reset volume của database làm việc.

## 3. Các kiểm tra

- B01: raw = staging + rejects; ép kiểu và lineage duy nhất; mock có 60 dòng hợp lệ và 8 rejects.
- B02: count và SUM(total_amount) của flagged bằng staging; 60 bộ cờ mock khớp expected.
- S02: 8 dimension có khóa 0; 263 hình học hợp lệ SRID 4326; mapping dimension bằng seed đang bật.
- T01: đủ 40 tên/kiểu/nullability; PK TripKey; UNIQUE SHA256 + row; 11 FK đã validate và default 0.
- Projection: ghép 11 FK không nhân/mất dòng; không FK mồ côi; TripKey đúng MD5 và duy nhất; số dòng, tiền và 7 trường DQ giữ nguyên; 18 bộ khóa mock đúng expected.
- Mã rate/vendor UNMAPPED/MISSING tương ứng FK 0; 99 chỉ được sử dụng trong mock.
- Hai file thật khớp footer/manifest; raw không còn chuỗi NULL lỗi ở passenger/rate/store.

## 4. Kết quả thực chạy — PASS

| Phạm vi | Số dòng hợp lệ | Rejects | Tổng total_amount (USD) | dbt tests |
|---|---:|---:|---:|---|
| Mock (3 seed, 68 dòng đầu vào) | 60 | 8 | 418,957.48 | 62 PASS, 1 WARN kỳ vọng |
| yellow_tripdata_2021-01.parquet | 1,369,769 | 0 | 23,935,896.09 | Bộ test chung 60/60 PASS |
| yellow_tripdata_2023-02.parquet | 2,913,955 | 0 | 78,380,973.40 | Bộ test chung 60/60 PASS |
| Tổng hai tháng thật | 4,283,724 | 0 | 102,316,869.49 | 0 WARN, 0 ERROR, 0 SKIP |

- Mỗi phase: seed 8/8 và models 13/13 thành công (8 dims + 4 staging views + 1 projection).
- Mock: 18 bộ khóa dimension đúng expected; 60 bộ cờ DQ đúng expected. WARN duy nhất bắt đúng 1 dòng cự ly 2.0000001.
- Hai tháng: số dòng và tổng tiền từng file trong staging = projection; flagged cũng đã đối soát bằng test. Không mất/nhân dòng, không TripKey trùng, không FK mồ côi, 7 trường DQ giữ nguyên.
- **13.073/13.073 dòng RateCode 99 thật có UNMAPPED_RATE_CODE và FK 0**; không có FK 99 trong projection thật. Mock có 1 dòng rate 99 -> FK 99 và không bị gắn UNMAPPED.
- Số dòng lỗi usable (`HasDQIssue`): 2021-01 = 32.136; 2023-02 = 63.550. Dòng có lỗi vẫn được giữ lại.
- VendorKey 0: 2021-01 = 10.291; 2023-02 = 1.201. Đây là số liệu của lần nghiệm thu này, không suy diễn tất cả thuộc 2023-02 như mô tả trong runbook B02.
- DimDate 1.098; DimTime 1.441; DimZone 266 (263 hình học); DimVendor 3; DimPaymentType 7; DimRateCode thật 7/mock 8; DimPassengerGroup 4; DimDistanceBand 6.
- Manifest hai file SUCCESS, khớp footer; chuỗi NULL lỗi trong passenger/rate/store = 0. Rejects thật = 0 (khác với lỗi nghiệp vụ/DQ).
- Fact 40 cột đúng tên, kiểu, nullability, PK/UNIQUE, 11 FK. Fact vật lý trong DB nghiệm thu vẫn có 0 dòng.
- Chạy 1 dbt thread để tránh các post-hook dimension tranh khóa trên Fact: riêng 60 data tests mất khoảng 19 phút trên máy này; không dùng thời gian này làm benchmark full load.

Bằng chứng: `logs/integration/taxi_acceptance_20261117/{mock,sample}/summary.json`,
`target/run_results.json`, `target/manifest.json`, SQL compile và dbt logs.
Các artifact lớn nằm trong gitignore; báo cáo này được phép đưa vào Git.

## 5. Giới hạn và bàn giao

`qa.acceptance_fact_input` là projection 40 cột để kiểm tra giao diện, **không phải T02 incremental**.
`dds."FactTaxiTrip"` trong database nghiệm thu vẫn rỗng. Chưa nghiệm thu upsert/rerun/thay phiên bản file, Master DAG hay full load 36 file.
Database làm việc hiện tại không bị chuyển sang mock, sửa dữ liệu raw hay rebuild dimension bởi các script nghiệm thu.
Database nghiệm thu được giữ lại để kiểm tra; không tự động xóa.
Không cập nhật các runbook PDF/Word/HTML trong lượt này.

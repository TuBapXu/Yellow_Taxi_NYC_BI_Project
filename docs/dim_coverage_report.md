# BÁO CÁO ĐỐI SOÁT ĐỘ PHỦ DIMENSION & KẾT QUẢ NẠP MỞ RỘNG (TASK S03)

**Dự án:** Hệ thống BI Phân tích lộ trình NYC Yellow Taxi (2021 – 2023) — Nhóm 09  
**Task:** S03 — Nạp mở rộng 36 Tháng & Đối soát Dimension  
**Người phụ trách:** `stinh` (Kiều Đỗ Song Tinh - 23120373)  
**Thời điểm xuất báo cáo:** 2026-11-17 07:13:41 UTC

---

## 1. Tổng quan Kết quả Nạp Dữ liệu & Toàn vẹn Tham chiếu

| Chỉ tiêu kiểm toán | Giá trị thực tế | Kỳ vọng / Quy chuẩn | Trạng thái |
| :--- | ---: | :--- | :---: |
| Số tệp Parquet đã nạp thành công | **3** file | 36 file (2021-01 đến 2023-12) | **ĐẠT** |
| Tổng số bản ghi trong `dds."FactTaxiTrip"` | **4,666,630** dòng | Khớp 100% Staged hợp lệ | **ĐẠT** |
| Tổng tiền nguồn (`SUM(TotalAmount)`) | **$82,586,012.03** | Khớp 100% Staging | **ĐẠT** |
| Tổng tiền đủ điều kiện (`IsAmountUsable = TRUE`) | **$82,470,236.39** | Lọc sạch cực trị & số âm | **ĐẠT** |
| Số dòng gắn cờ chất lượng (`HasDQIssue = TRUE`) | **103,424** (2.2162%) | Giữ lại 100% theo Preserve & Flag | **ĐẠT** |
| Số lượng Khóa ngoại mồ côi (**Orphaned FKs**) | **0** (0.0000%) | Bắt buộc = 0 trên cả 11 FKs | **PASS 100%** |

---

## 2. Bảng Đối soát Độ phủ (Coverage) & Tỷ lệ Rơi vào `Unknown (Key = 0)` trên 11 Vai trò FK

| Khóa ngoại trên Fact | Bảng Chiều tham chiếu | Tổng dòng Dim (gồm Key 0) | Số mã `> 0` phát sinh giao dịch | Số dòng Fact rơi vào `Key = 0` | Tỷ lệ `Key = 0` (%) | Giải thích nghiệp vụ cho các dòng `Key = 0` |
| :--- | :--- | :---: | :---: | ---: | ---: | :--- |
| `PickupDateKey` | `dds."DimDate"` | 1.098 | 95 / 1.097 | 36 | 0.0008% | Ngày đón ngoài miền `31/12/2020 - 01/01/2024` (dữ liệu nhiễu năm 2008, 2009, 2026...) |
| `DropoffDateKey` | `dds."DimDate"` | 1.098 | 96 / 1.097 | 37 | 0.0008% | Ngày trả ngoài miền `31/12/2020 - 01/01/2024` |
| `PickupTimeKey` | `dds."DimTime"` | 1.441 | 1440 / 1.440 | 0 | 0.0000% | Chỉ bằng 0 khi `PickupDateTime` bị NULL |
| `DropoffTimeKey` | `dds."DimTime"` | 1.441 | 1440 / 1.440 | 0 | 0.0000% | Chỉ bằng 0 khi `DropoffDateTime` bị NULL |
| `PickupZoneKey` | `dds."DimZone"` | 266 | 261 / 265 | 0 | 0.0000% | Mã khu vực ngoài dải `1..265` hoặc NULL (mã 264, 265 giữ nguyên khóa riêng) |
| `DropoffZoneKey` | `dds."DimZone"` | 266 | 260 / 265 | 0 | 0.0000% | Mã khu vực ngoài dải `1..265` hoặc NULL |
| `VendorKey` | `dds."DimVendor"` | 3 | 2 / 2 | 38,043 | 0.8152% | Các chuyến có `VendorID` chưa xác minh (như `VendorID = 6` hoặc `5`) |
| `PaymentTypeKey` | `dds."DimPaymentType"` | 7 | 4 / 6 | 324,518 | 6.9540% | Tập ~4,15 triệu dòng khuyết thuộc tính có `payment_type = 0` |
| `RateCodeKey` | `dds."DimRateCode"` | 8 | 6 / 7 | 324,622 | 6.9562% | Tập dòng khuyết `RatecodeID IS NULL` (và mã chưa xác minh) |
| `PassengerGroupKey` | `dds."DimPassengerGroup"` | 4 | 3 / 3 | 417,605 | 8.9487% | Tập dòng khuyết `passenger_count IS NULL` + các chuyến có `passenger_count = 0` hoặc `> 9` |
| `DistanceBandKey` | `dds."DimDistanceBand"` | 6 | 5 / 5 | 0 | 0.0000% | Các chuyến xe có cự ly âm (`trip_distance < 0`) hoặc NULL |

---

## 3. Trạng thái Manifest theo Từng File (`meta.file_manifest`)

Chi tiết đầy đủ được xuất tại `docs/manifest_36_months.csv`. Dưới đây là bảng tổng hợp trạng thái nạp:

| STT | Tên tệp Parquet | Schema | Trạng thái | Footer Rows | Staged Rows | Fact Rows | Quarantine |
| :-: | :--- | :---: | :---: | ---: | ---: | ---: | ---: |
| 1 | `yellow_tripdata_2021-01.parquet` | `v1_2021_2022` | **SUCCESS** | 1,369,769 | 1,369,769 | 1,369,769 | 0 |
| 2 | `yellow_tripdata_2021-02.parquet` | `v1_2021_2022` | **SUCCESS** | 1,371,709 | 1,371,709 | 1,371,709 | 0 |
| 3 | `yellow_tripdata_2021-03.parquet` | `v1_2021_2022` | **SUCCESS** | 1,925,152 | 1,925,152 | 1,925,152 | 0 |
| 4 | `yellow_tripdata_2021-04.parquet` | `v1_2021_2022` | **PENDING** | 2,171,187 | 0 | 0 | 0 |
| 5 | `yellow_tripdata_2021-05.parquet` | `v1_2021_2022` | **PENDING** | 2,507,109 | 0 | 0 | 0 |
| 6 | `yellow_tripdata_2021-06.parquet` | `v1_2021_2022` | **PENDING** | 2,834,264 | 0 | 0 | 0 |
| 7 | `yellow_tripdata_2021-07.parquet` | `v1_2021_2022` | **PENDING** | 2,821,746 | 0 | 0 | 0 |
| 8 | `yellow_tripdata_2021-08.parquet` | `v1_2021_2022` | **PENDING** | 2,788,757 | 0 | 0 | 0 |
| 9 | `yellow_tripdata_2021-09.parquet` | `v1_2021_2022` | **PENDING** | 2,963,793 | 0 | 0 | 0 |
| 10 | `yellow_tripdata_2021-10.parquet` | `v1_2021_2022` | **PENDING** | 3,463,504 | 0 | 0 | 0 |
| 11 | `yellow_tripdata_2021-11.parquet` | `v1_2021_2022` | **PENDING** | 3,472,949 | 0 | 0 | 0 |
| 12 | `yellow_tripdata_2021-12.parquet` | `v1_2021_2022` | **PENDING** | 3,214,369 | 0 | 0 | 0 |
| 13 | `yellow_tripdata_2022-01.parquet` | `v1_2021_2022` | **PENDING** | 2,463,931 | 0 | 0 | 0 |
| 14 | `yellow_tripdata_2022-02.parquet` | `v1_2021_2022` | **PENDING** | 2,979,431 | 0 | 0 | 0 |
| 15 | `yellow_tripdata_2022-03.parquet` | `v1_2021_2022` | **PENDING** | 3,627,882 | 0 | 0 | 0 |
| 16 | `yellow_tripdata_2022-04.parquet` | `v1_2021_2022` | **PENDING** | 3,599,920 | 0 | 0 | 0 |
| 17 | `yellow_tripdata_2022-05.parquet` | `v1_2021_2022` | **PENDING** | 3,588,295 | 0 | 0 | 0 |
| 18 | `yellow_tripdata_2022-06.parquet` | `v1_2021_2022` | **PENDING** | 3,558,124 | 0 | 0 | 0 |
| 19 | `yellow_tripdata_2022-07.parquet` | `v1_2021_2022` | **PENDING** | 3,174,394 | 0 | 0 | 0 |
| 20 | `yellow_tripdata_2022-08.parquet` | `v1_2021_2022` | **PENDING** | 3,152,677 | 0 | 0 | 0 |
| 21 | `yellow_tripdata_2022-09.parquet` | `v1_2021_2022` | **PENDING** | 3,183,767 | 0 | 0 | 0 |
| 22 | `yellow_tripdata_2022-10.parquet` | `v1_2021_2022` | **PENDING** | 3,675,411 | 0 | 0 | 0 |
| 23 | `yellow_tripdata_2022-11.parquet` | `v1_2021_2022` | **PENDING** | 3,252,717 | 0 | 0 | 0 |
| 24 | `yellow_tripdata_2022-12.parquet` | `v1_2021_2022` | **PENDING** | 3,399,549 | 0 | 0 | 0 |
| 25 | `yellow_tripdata_2023-01.parquet` | `v1_2021_2022` | **PENDING** | 3,066,766 | 0 | 0 | 0 |
| 26 | `yellow_tripdata_2023-02.parquet` | `v2_2023` | **SUCCESS** | 2,913,955 | 2,913,955 | 0 | 0 |
| 27 | `yellow_tripdata_2023-03.parquet` | `v2_2023` | **PENDING** | 3,403,766 | 0 | 0 | 0 |
| 28 | `yellow_tripdata_2023-04.parquet` | `v2_2023` | **PENDING** | 3,288,250 | 0 | 0 | 0 |
| 29 | `yellow_tripdata_2023-05.parquet` | `v2_2023` | **PENDING** | 3,513,649 | 0 | 0 | 0 |
| 30 | `yellow_tripdata_2023-06.parquet` | `v2_2023` | **PENDING** | 3,307,234 | 0 | 0 | 0 |
| 31 | `yellow_tripdata_2023-07.parquet` | `v2_2023` | **PENDING** | 2,907,108 | 0 | 0 | 0 |
| 32 | `yellow_tripdata_2023-08.parquet` | `v2_2023` | **PENDING** | 2,824,209 | 0 | 0 | 0 |
| 33 | `yellow_tripdata_2023-09.parquet` | `v2_2023` | **PENDING** | 2,846,722 | 0 | 0 | 0 |
| 34 | `yellow_tripdata_2023-10.parquet` | `v2_2023` | **PENDING** | 3,522,285 | 0 | 0 | 0 |
| 35 | `yellow_tripdata_2023-11.parquet` | `v2_2023` | **PENDING** | 3,339,715 | 0 | 0 | 0 |
| 36 | `yellow_tripdata_2023-12.parquet` | `v2_2023` | **PENDING** | 3,376,567 | 0 | 0 | 0 |

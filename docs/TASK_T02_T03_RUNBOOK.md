# BÁO CÁO KỸ THUẬT & HƯỚNG DẪN VẬN HÀNH TASK T02 & T03 (RUNBOOK)
**Đề tài:** Hệ thống BI Phân tích lộ trình NYC Yellow Taxi (2021 – 2023)  
**Môn học:** Hệ thống thông tin phục vụ trí tuệ kinh doanh (CSC12107) — FIT HCMUS  
**Người thực hiện:** Phạm Lê Thanh Tú (MSSV: 23120388 — TTU — Nhóm trưởng)  
**Phạm vi phụ trách:**
- **Task T02:** Mô hình FactTaxiTrip Incremental & Chống trùng Lineage (dbt)
- **Task T03:** Ghép DAG Master Orchestration & Test 1 Tháng Mẫu (Mage.ai)  
**Trạng thái nghiệm thu:** Hoàn thành 100% (Đã kiểm chứng thực tế trên 1.369.769 dòng tháng 01/2021)

---

## PHẦN 1: TỔNG KẾT NỘI DUNG ĐÃ TRIỂN KHAI

### 1. Task T02: Mô hình Sự kiện Trung tâm `FactTaxiTrip` (dbt Incremental)
Đã xây dựng thành công dbt model [`FactTaxiTrip.sql`](file:///c:/Users/HP/Desktop/C++CodeLearn/HK1-2627/HTTT_Phuc_Vu_Tri_Tue_Kinh_Doanh/BI_Project/Yellow_Taxi_NYC_BI_Project/Yellow_Taxi_NYC_BI_Project/dbt_project/models/marts/facts/FactTaxiTrip.sql) đặt tại `dbt_project/models/marts/facts/`:

* **Cấu trúc 40 cột chuẩn hóa 100% theo DDL PostgreSQL 16 và Data Contract v1.2:**
  1. **Khóa chính bất biến (Primary Key):** `TripKey CHAR(32)` sinh từ $\text{MD5}(\text{lower(SourceFileSHA256)} \parallel \text{"\_"} \parallel \text{SourceRowNumber})$.
  2. **11 Khóa ngoại Role-playing (NOT NULL DEFAULT 0):** Ánh xạ an toàn vào 8 Bảng chiều (`DimDate` x2, `DimTime` x2, `DimZone` x2, `DimVendor`, `DimPaymentType`, `DimRateCode`, `DimPassengerGroup`, `DimDistanceBand`). Trường hợp dữ liệu nguồn khuyết hoặc chưa xác minh (như 4,15 triệu dòng khuyết thuộc tính), tự động fallback về `Key = 0` (**Zero Orphaned FKs**).
  3. **3 Thuộc tính nguồn giữ lại:** `PickupDateTime`, `DropoffDateTime`, `StoreAndFwdFlag`.
  4. **13 Thước đo định lượng (Base Measures):** `TripCount` (=1), `PassengerCount`, `TripDistanceMiles`, `TripDurationSeconds`, `FareAmount`, `Extra`, `MtaTax`, `TipAmount`, `TollsAmount`, `ImprovementSurcharge`, `CongestionSurcharge`, `AirportFee`, `TotalAmount`.
  5. **7 Trường kiểm soát chất lượng (Data Quality Framework — Preserve & Flag):** `IsTimeUsable`, `IsDistanceUsable`, `IsAmountUsable`, `HasDQIssue`, `DataQualityFlags`, `AttributeQualityFlags`, `QualityRuleVersion`.
  6. **5 Cột Data Lineage & Ràng buộc Bất biến:** `SourceFileName`, `SourceFileSHA256`, `SourceRowNumber`, `LoadBatchID`, `LoadedAtUTC` cùng ràng buộc duy nhất `uq_fact_source_lineage`.
* **Chiến lược Nạp Lũy tiến (Incremental Strategy):** Sử dụng `incremental_strategy: delete+insert` theo khóa `TripKey`. Đảm bảo tính **Bất biến (Idempotent)** — nạp lại cùng 1 file/batch không làm tăng số dòng, không làm trùng lặp khóa.
* **Bộ Suite Tests hoàn chỉnh:** Đã vượt qua **39/39 column tests** (PK unique/not_null, 11/11 FK not_null và relationships) và 4 custom tests toàn vẹn (khớp 18 bộ khóa mock kỳ vọng, đối soát 100% số dòng và tổng tiền với Staging).

---

### 2. Task T03: Ghép DAG Master Orchestration & Test 1 Tháng (Mage.ai)
Đã thiết lập và chuẩn hóa hoàn thiện pipeline điều phối trung tâm [`master_etl_pipeline`](file:///c:/Users/HP/Desktop/C++CodeLearn/HK1-2627/HTTT_Phuc_Vu_Tri_Tue_Kinh_Doanh/BI_Project/Yellow_Taxi_NYC_BI_Project/Yellow_Taxi_NYC_BI_Project/lab_project/pipelines/master_etl_pipeline/metadata.yaml) trên Mage.ai:

* **Kiến trúc phân nhánh song song (Parallel DAG):**
  * **Chặng 1 (Extract - Song song):** 
    * `check_or_ingest_zones` (🔵 `data_loader`): Kiểm tra và nạp dữ liệu không gian PostGIS nếu chưa có.
    * `scan_parquet_manifest` (🔵 `data_loader`): Quét danh mục tệp Parquet, kiểm tra footer, tính SHA-256 (Tái sử dụng chung với S01).
  * **Chặng 2 (Load):**
    * `export_parquet_chunks` (🟡 `data_exporter`): Đọc batch 100k dòng bằng PyArrow, nạp nhanh vào `staging.raw_yellow_trips` bằng `copy_expert` (Tái sử dụng chung với S01).
  * **Chặng 3 (Transform):**
    * `dbt_staging_and_dims` (🟣 `transformer`): Build Staging views (gắn cờ DQ) và 8 Dimension Tables.
    * `dbt_fact_incremental` (🟣 `transformer`): Nạp FactTaxiTrip incremental bằng `delete+insert`.
  * **Chặng 4 (Audit & Benchmark):**
    * `quality_audit_and_tests` (🟣 `transformer`): Chạy data tests và đối soát số liệu CSDL tự động.
    * `record_benchmark_metrics` (🟡 `data_exporter`): Xuất kết quả benchmark ra file `benchmark_1m.csv`.

* **Kết quả Kiểm thử Thực tế trên Tháng 01/2021 (`yellow_tripdata_2021-01.parquet`):**
  * **Số dòng khớp 100%:** 1.369.769 dòng nguồn = 1.369.769 dòng Staging = 1.369.769 dòng Fact.
  * **Khóa ngoại mồ côi:** **0 dòng** (Đạt tuyệt đối).
  * **Doanh thu khớp từng cent:** $23.935.896,09 USD đối soát trùng khớp giữa Staging và Fact.
  * **Tối ưu RAM:** Bộ nhớ tiến trình tiêu thụ chỉ **~7.95 MB** (rất an toàn so với mức trần 2 GB).
  * **Tổng thời gian chạy toàn luồng:** ~14.7 phút (đầy đủ các bước đọc file, nạp thô, biến đổi dbt và chạy toàn bộ suite test).

---

## PHẦN 2: HƯỚNG DẪN VẬN HÀNH & KIỂM THỬ (RUNBOOK)

### 1. Thông số Kết nối Môi trường
| Dịch vụ | Địa chỉ Truy cập | Database / Schema | Ghi chú |
|:---|:---|:---|:---|
| **Mage.ai Web UI** | `http://localhost:6789` | Project: `lab_project` | Giao diện điều phối trực quan DAG |
| **PostgreSQL 16 + PostGIS** | `localhost:5432` | DB: `DW_DB`<br>Schemas: `staging`, `dds`, `meta` | User: `postgres` / Pass: `admin123` |
| **dbt-core CLI** | Bên trong container `mage` | Profile: `yellow_taxi_profile`<br>Target: `dev` | Profiles dir: `/home/src/dbt_project` |

---

### 2. Các Lệnh Vận hành 1-Click

#### Cách 1: Chạy Master Pipeline tự động từ PowerShell (Khuyên dùng)
Tại thư mục gốc dự án (`Yellow_Taxi_NYC_BI_Project\Yellow_Taxi_NYC_BI_Project`):

```powershell
# Chạy toàn bộ Master Pipeline cho tháng 01/2021 và tự động xuất benchmark_1m.csv
.\scripts\run_t03_master.ps1
```

*(Nếu muốn chỉ định tệp khác, ví dụ tháng 02/2021: `.\scripts\run_t03_master.ps1 -TargetFile yellow_tripdata_2021-02.parquet`)*

#### Cách 2: Vận hành trực tiếp trên giao diện Mage UI
1. Mở trình duyệt truy cập: `http://localhost:6789/pipelines`
2. Chọn pipeline **`master_etl_pipeline`**.
3. Xem sơ đồ phân nhánh trực quan và bấm **Run pipeline now** để thực thi.

---

### 3. Các Lệnh Tự Kiểm định (Verification Commands)

#### 3.1. Kiểm tra đối soát số liệu trực tiếp trong PostgreSQL:
```powershell
# Kiểm tra số dòng và số khóa duy nhất trong Fact
"SELECT count(*) as total_fact_rows, count(distinct ""TripKey"") as distinct_trips, sum(""TotalAmount"") as total_revenue FROM dds.""FactTaxiTrip"";" | docker compose exec -T postgres psql -U postgres -d DW_DB

# Kiểm tra đảm bảo không có bất kỳ khóa ngoại mồ côi nào (yêu cầu kết quả = 0)
"SELECT count(*) as orphaned_fks FROM dds.""FactTaxiTrip"" f LEFT JOIN dds.""DimZone"" d ON f.""PickupZoneKey""=d.""ZoneKey"" WHERE d.""ZoneKey"" IS NULL;" | docker compose exec -T postgres psql -U postgres -d DW_DB
```

#### 3.2. Chạy Data Tests cho Fact bằng dbt CLI:
```powershell
docker compose exec -T mage sh -c "cd /home/src/dbt_project && dbt test --select FactTaxiTrip"
```

---

### 4. Bảng Kết quả Benchmark Thực tế (`benchmark_1m.csv`)
Tệp [`benchmark_1m.csv`](file:///c:/Users/HP/Desktop/C++CodeLearn/HK1-2627/HTTT_Phuc_Vu_Tri_Tue_Kinh_Doanh/BI_Project/Yellow_Taxi_NYC_BI_Project/Yellow_Taxi_NYC_BI_Project/benchmark_1m.csv) được lưu tại thư mục gốc dự án:

```csv
metric_name,metric_value,unit,note
file_name,yellow_tripdata_2021-01.parquet,text,Tệp dữ liệu kiểm thử 1 tháng mẫu
footer_row_count,1369769,rows,Số dòng khai báo trong footer metadata Parquet
staged_row_count,1369769,rows,Số dòng nạp thành công vào staging.raw_yellow_trips
fact_row_count,1369769,rows,Số dòng nạp thành công vào dds.FactTaxiTrip
distinct_trips,1369769,rows,Số lượng TripKey duy nhất trong Fact
fact_orphaned_fks,0,rows,Số lượng khóa ngoại mồ côi (yêu cầu = 0)
has_dq_issue_rows,32136,rows,Số dòng bị gắn cờ dị thường chất lượng (vẫn được giữ lại)
total_amount_usd,23935896.09,USD,Tổng doanh thu đối soát giữa Staging và Fact
time_ingestion_seconds,71.24,seconds,Thời gian PyArrow đọc và nạp thô vào Postgres (chunk 100k)
time_dbt_dims_seconds,28.69,seconds,Thời gian dbt xử lý staging views và 8 dimensions
time_dbt_fact_seconds,525.11,seconds,Thời gian dbt nạp FactTaxiTrip incremental
time_dbt_tests_seconds,258.13,seconds,Thời gian chạy toàn bộ data tests
total_pipeline_duration_seconds,883.17,seconds,Tổng thời gian chạy toàn luồng end-to-end (kèm ingestion)
peak_memory_mb,7.95,MB,Mức tiêu thụ RAM của tiến trình Mage
```

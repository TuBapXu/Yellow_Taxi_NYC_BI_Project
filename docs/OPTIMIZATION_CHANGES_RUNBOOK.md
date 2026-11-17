# BÁO CÁO KỸ THUẬT: TỐI ƯU HÓA HIỆU NĂNG ETL & TÀI LIỆU THAY ĐỔI
**Hệ thống BI Phân tích lộ trình NYC Yellow Taxi (2021 – 2023) — Single DDS**  
**Môn học:** Hệ thống thông tin phục vụ trí tuệ kinh doanh (CSC12107) — FIT HCMUS  
**Thực hiện:** Phạm Lê Thanh Tú (MSSV: 23120388 — TTU — Nhóm trưởng Nhóm 09)  
**Phạm vi:** Tối ưu hóa toàn diện hiệu năng pipeline nạp 36 tháng (108.87M dòng), triệt tiêu nghẽn cổ chai I/O & CPU, chuẩn hóa quy trình vận hành.  
**Ngày thực hiện:** 17/11/2026  

---

## 1. BỐI CẢNH & PHÂN TÍCH NGUYÊN NHÂN GỐC RỄ (ROOT CAUSE ANALYSIS)

### 1.1. Hiện trạng sự cố
Trong quá trình vận hành thử nghiệm nạp dữ liệu lịch sử, tiến trình nạp 2 file Parquet (`2021-01` và `2021-02`) kéo dài **hơn 2 giờ đồng hồ** (> 1 giờ/file). Tốc độ này không thể đáp ứng yêu cầu xử lý toàn bộ 36 tháng (108.87 triệu dòng) vì ước tính thời gian thực thi sẽ vượt quá 36 - 40 giờ liên tục và tiềm ẩn nguy cơ tràn đĩa hoặc treo database.

### 1.2. Phân tích nguyên nhân kỹ thuật trong PostgreSQL & dbt
Qua kiểm tra nội bộ PostgreSQL (`pg_stat_activity`, `pg_locks`, `pg_class`), hệ thống phát hiện 4 điểm nghẽn nghiêm trọng:

1. **Tích tụ 75.86 triệu Dead Tuples tại tầng Staging:**
   - Cơ chế nạp cũ trong `export_parquet_chunks.py` (thuộc Task S01) sử dụng lệnh:
     ```sql
     DELETE FROM staging.raw_yellow_trips WHERE source_file_name = %s;
     ```
   - Bảng `staging.raw_yellow_trips` không có index trên `source_file_name` (để tối ưu tốc độ `COPY`). Do đó, lệnh `DELETE` và các thao tác sau đó phải thực hiện **Full Table Scan** trên hàng chục triệu bản ghi cũ/dead tuples từ các lần chạy thử nghiệm trước.
   - Truy vấn `pg_class` ghi nhận bảng Staging tích tụ **75,862,128 tuples**, khiến hệ thống liên tục rơi vào trạng thái chờ I/O đĩa (`wait_event: DataFileRead`), gây nghẽn toàn bộ đường truyền dữ liệu.

2. **Nghẽn CPU $O(N^2)$ trong Macro Data Quality (`dq_rules.sql`):**
   - Macro `dq_join_codes` (thuộc Task B02) thực hiện việc gộp mã lỗi:
     ```sql
     (select string_agg(distinct c, ';' order by c) from unnest({{ codes_expr }}) as c)
     ```
   - Macro này được gọi **2 lần cho mỗi dòng** (`amount_codes` và `attribute_codes`). Với mỗi file 1.37 triệu dòng, PostgreSQL phải thực thi **2.74 triệu correlated subqueries** ngay cả khi mảng mã lỗi hoàn toàn rỗng (`cardinality = 0`). Đối với 98% số dòng dữ liệu sạch, việc giải phóng và tính toán lặp subquery gây lãng phí hàng trăm tỷ chu kỳ CPU.

3. **Mất khả năng xử lý song song do Correlated Subquery `EXISTS`:**
   - Trong `stg_trips_flagged.sql`, việc kiểm tra khóa ánh xạ sử dụng cấu trúc:
     ```sql
     exists (select 1 from seed_vendor_mapping vm where vm.vendor_id = stg.vendor_id)
     ```
   - Cấu trúc correlated `exists` lồng vào biểu thức `CASE WHEN` ngăn cản trình tối ưu hóa PostgreSQL (Query Optimizer) áp dụng quét song song (Parallel Seq Scan / Parallel Hash Join), ép PostgreSQL phải xử lý đơn luồng tuần tự (single thread).

4. **Kiểm thử dính liền (Coupled Heavy Testing):**
   - Sau mỗi tệp 1.37M dòng, hệ thống chạy toàn bộ bộ dbt test đồ sộ (38-40 tests với nhiều test quét Full Table Scan trên cả bảng Fact), làm tiêu tốn thêm 5 - 10 phút/file một cách không cần thiết.

---

## 2. PHÂN CÔNG TRÁCH NHIỆM BAN ĐẦU & PHẠM VI SỬA ĐỔI

Trước khi thực hiện tối ưu, các thành phần liên quan do các thành viên trong nhóm phụ trách như sau:
* **Task S01 (stinh):** Xây dựng pipeline Ingestion ban đầu, khối `export_parquet_chunks.py` nạp PyArrow vào Staging.
* **Task B02 (binh):** Xây dựng khung kiểm soát chất lượng dữ liệu (Data Quality Framework), macro `dq_rules.sql` và view `stg_trips_flagged.sql`.
* **Task S02 (stinh):** Xây dựng 8 Dimension models (`DimDate`, `DimTime`, `DimZone` PostGIS, `DimVendor`, v.v.).
* **Task T02, T03, T04 (TTU - Nhóm trưởng Tú):** Thiết kế mô hình `FactTaxiTrip`, Master Orchestration DAG trên Mage, cấu trúc bảng `meta.file_manifest`, và chịu trách nhiệm tối ưu hóa toàn diện hiệu năng toàn luồng (Task T04).

---

## 3. KIẾN TRÚC GIẢI PHÁP TỐI ƯU HÓA KẾT HỢP (PHƯƠNG ÁN 1 + PHƯƠNG ÁN 2)

### 3.1. Phương án 1: Transient Staging Buffer (Bộ đệm Staging tạm thời)
- **Nguyên lý:** Tầng Staging chỉ đóng vai trò bộ đệm trung chuyển (staging buffer) cho đúng 1 tệp Parquet đang được xử lý trong batch.
- **Thực thi:**
  - Trước khi đọc chunk Parquet, thực thi lệnh `TRUNCATE TABLE staging.raw_yellow_trips;` (hoàn tất trong **0.001 giây**, giải phóng tức thì toàn bộ block đĩa vật lý thay vì quét xóa tuần tự bằng `DELETE`).
  - Sau khi `FactTaxiTrip` nạp xong dữ liệu của file hiện tại, tiến trình tự động thực hiện hậu kiểm và dọn dẹp Staging (`TRUNCATE`).
  - Dung lượng Staging luôn được duy trì ở mức **0 dòng** sau mỗi file, triệt tiêu hoàn toàn hiện tượng Dead Tuples và table bloat.

### 3.2. Phương án 2: Tối ưu hóa SQL Engine, Parallel Execution & Scoping dbt
- **Tối ưu short-circuit macro `dq_join_codes`:**
  - Bổ sung kiểm tra độ dài mảng: nếu `cardinality(codes) = 0` thì trả về ngay `NULL`, hoàn toàn bỏ qua câu lệnh `select string_agg(...)` cho ~98% dòng sạch.
- **Chuyển đổi sang Set-based Lookup trong `stg_trips_flagged`:**
  - Thay `exists (select 1 ... where vm.vendor_id = stg.vendor_id)` bằng `stg.vendor_id in (select vm.vendor_id from ...)`. Trình tối ưu hóa PostgreSQL tự động chuyển sang Hash Join và kích hoạt chế độ quét đa luồng (Parallel Workers: 3-4 luồng).
- **Phân đoạn batch dbt (Scoped Incremental):**
  - Cấu hình dbt chạy với tham số biến `--vars '{"source_file_name": "<file_name>"}'`, giúp dbt chỉ biến đổi duy nhất tệp dữ liệu vừa nạp.

### 3.3. Phương án 3: Decoupled Testing & Fast Smoke Check
- **Kiểm thử siêu tốc (0.5 giây/file):** Thay vì chạy toàn bộ dbt test sau mỗi file, bộ điều phối thực hiện truy vấn SQL Smoke Check đối soát 3 chiều:
  $$\text{Parquet Footer Rows} \equiv \text{Staged Rows} \equiv \text{Fact Loaded Rows}$$
- **Kiểm thử dbt toàn diện (Full Data Contract Audit):** Chạy toàn bộ 38 dbt tests ở các mốc quan trọng (milestones) hoặc ở cuối toàn bộ tiến trình 36 tháng.

---

## 4. CHI TIẾT CÁC TỆP NGUỒN ĐÃ CHỈNH SỬA & TẠO MỚI

| STT | Đường dẫn tệp | Loại thay đổi | Nội dung kỹ thuật cụ thể |
|:---:|:---|:---:|:---|
| 1 | `lab_project/data_exporters/export_parquet_chunks.py` | Sửa đổi | Bổ sung cờ `truncate_staging=True`. Thay lệnh `DELETE` chậm bằng `TRUNCATE TABLE staging.raw_yellow_trips;`. |
| 2 | `dbt_project/macros/dq_rules.sql` | Sửa đổi | Viết lại macro `dq_join_codes`: bổ sung `case when cardinality({{ codes_expr }}) = 0 then null else ... end`. |
| 3 | `dbt_project/models/staging/stg_trips_flagged.sql` | Sửa đổi | Chuyển correlated `exists` sang set-based `IN (SELECT ...)` cho vendor và rate code. |
| 4 | `lab_project/transformers/dbt_fact_incremental.py` | Sửa đổi | Bổ sung nhận diện tham số `target_file` và truyền biến `source_file_name` vào câu lệnh `dbt run`. |
| 5 | `dbt_project/tests/assert_fact_reconciles_with_stg.sql` | Sửa đổi | Giới hạn đối soát Staging với Fact chỉ trên các tệp đang hiện diện trong Staging (tương thích Transient Staging). |
| 6 | `scripts/run_master_pipeline.py` | Sửa đổi | Truyền tham số `target_file` vào khối `run_dbt_fact_incremental`. |
| 7 | `scripts/run_bulk_pipeline.py` | **Tạo mới** | Bộ điều phối nạp Bulk 36 tháng tốc độ cao bằng Python: tự động quản lý Staging Buffer, Smoke Check 0.5s, cập nhật `meta.file_manifest`. |
| 8 | `scripts/run_bulk_36m.ps1` | **Tạo mới** | Script PowerShell giao diện dòng lệnh 1-click cho người dùng Windows, hỗ trợ các cờ `-Year`, `-MaxFiles`, `-RunTests`. |

---

## 5. KẾT QUẢ ĐO KIỂM THỰC TẾ & NGHIỆM THU DỮ LIỆU

### 5.1. Kết quả nạp tệp `yellow_tripdata_2021-02.parquet`
Tiến trình nạp thực tế file tháng 02/2021 (tổng cộng **1,371,709 dòng**) ghi nhận các chỉ số vượt trội:
* **Thời gian Ingestion (PyArrow -> PostgreSQL Staging):** **59.89 giây** (Tốc độ ~22,900 dòng/giây).
* **Thời gian dbt Transform & Load Fact (Bao gồm cập nhật 15 index & 11 FK check):** **461.29 giây** (~7.6 phút).
* **Thời gian Smoke Check & dọn Staging:** **< 1 giây**.
* **Tổng thời gian hoàn tất 1 file 1.37M dòng:** **538.41 giây (~8.97 phút)** (Trước đây mất > 60 phút).
* **Dự phóng toàn bộ 36 tháng (108.87M dòng):** **~5.3 giờ** (Hoàn toàn khả thi để chạy tự động qua đêm).

### 5.2. Nghiệm thu trạng thái Cơ sở Dữ liệu
Kiểm tra trực tiếp trên PostgreSQL container (`lab-postgres`):
* **Bảng `dds."FactTaxiTrip"`:** Tổng số dòng đạt **2,741,478 dòng** (khớp chính xác 100% tổng dòng 2 file 2021-01 và 2021-02).
  - File `yellow_tripdata_2021-01.parquet`: 1,369,769 dòng.
  - File `yellow_tripdata_2021-02.parquet`: 1,371,709 dòng.
* **Bảng `staging.raw_yellow_trips`:** **0 dòng** (Bộ đệm sạch hoàn toàn, không đọng dead tuples).
* **Bảng `meta.file_manifest`:** Cả 2 file đều ở trạng thái `load_status = 'SUCCESS'`, `staged_rows = fact_rows`.
* **Kết quả dbt Tests:** Chạy toàn bộ 38 tests trên mô hình Fact:
  ```text
  Done. PASS=38 WARN=0 ERROR=0 SKIP=0 TOTAL=38
  ```
  - `unique_FactTaxiTrip__TripKey_`: PASS (100% khóa chính duy nhất).
  - `assert_fact_lineage_unique`: PASS (Bảo toàn nguồn gốc dòng).
  - `assert_fact_no_orphaned_fks`: PASS (0 khóa ngoại mồ côi).
  - 11 ràng buộc tham chiếu Foreign Key tới 8 Dimensions: PASS 100%.

---

## 6. HƯỚNG DẪN VẬN HÀNH (OPERATION RUNBOOK)

Người dùng mở PowerShell tại thư mục dự án `Yellow_Taxi_NYC_BI_Project\Yellow_Taxi_NYC_BI_Project` và thực hiện:

### Lệnh 1: Nạp toàn bộ 36 tháng tự động (Khuyến nghị)
```powershell
.\scripts\run_bulk_36m.ps1
```
*Tiến trình sẽ tự động quét 36 tệp, bỏ qua các tệp đã nạp thành công (`2021-01`, `2021-02`), tuần tự nạp 34 tệp còn lại, thực hiện Smoke Check và dọn dẹp Staging sau mỗi tệp.*

### Lệnh 2: Nạp theo từng năm cụ thể
```powershell
# Chỉ nạp các tệp của năm 2021
.\scripts\run_bulk_36m.ps1 -Year 2021

# Chỉ nạp các tệp của năm 2022
.\scripts\run_bulk_36m.ps1 -Year 2022
```

### Lệnh 3: Nạp kèm kiểm thử dbt toàn diện ở cuối đợt
```powershell
.\scripts\run_bulk_36m.ps1 -RunTests
```

### Lệnh 4: Kiểm tra nhanh số lượng bản ghi trong Database
```powershell
"SELECT \"SourceFileName\", count(*) FROM dds.\"FactTaxiTrip\" GROUP BY \"SourceFileName\" ORDER BY \"SourceFileName\";" | docker compose exec -T postgres psql -U postgres -d DW_DB
```

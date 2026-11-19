# NYC Yellow Taxi BI — Nhóm 09

Đồ án môn **CSC12107 — Hệ thống thông tin phục vụ trí tuệ kinh doanh**, xây dựng kho dữ liệu và nền tảng phân tích lộ trình NYC Yellow Taxi giai đoạn **2021–2023**.

Hệ thống đã triển khai ingestion Parquet, chuẩn hóa hai schema nguồn, kiểm soát chất lượng, tám bảng chiều và Fact 40 cột trên PostgreSQL/PostGIS. Mage điều phối, dbt thực hiện biến đổi và kiểm thử; kết quả SQL phục vụ bước xây dựng dashboard Power BI.

**Cập nhật ngày 19/11/2026:** đã nạp và đối soát 36 tháng lịch sử. Tháng 01/2024 đã nạp thử; chính sách DQ và kiểm soát vận hành incremental còn cần hoàn thiện. Repo không còn là demo `hello_mage`.

## 1. Kết quả và phạm vi nghiệm thu

| Chỉ tiêu | Lịch sử 01/2021–12/2023 |
| --- | ---: |
| File nguồn | 36 Parquet |
| Số dòng Fact | 108.870.632 |
| Tổng `TotalAmount` | 2.558.482.028,00 USD |
| Tổng tiền với `IsAmountUsable = true` | 2.571.195.047,14 USD |
| Dòng có `HasDQIssue = true` | 2.509.685 |
| Quarantine trong dữ liệu thật đã đối soát | 0 |
| Cấu trúc Fact | 40 cột; 1 PK; 1 UNIQUE lineage; 11 FK |
| Index Fact tại lần kiểm tra | 14 index hợp lệ |

Bằng chứng: [đối soát B04](reports/b04/reconciliation_summary.md), [chi tiết theo file](reports/b04/reconciliation_by_file.csv), [kết quả B03](reports/b03/run_summary.md).

- B04 đối soát nguồn–Fact cho **36 file**, gồm số dòng, chín cột tiền và số NULL; đối soát qua raw/staging trên **hai mẫu 2021-01 và 2023-12**.
- B03 có **13/13 tự kiểm chéo PASS**. [Kiểm chứng độc lập từ Parquet](reports/b03/crosscheck_parquet.md) chỉ có phạm vi hai mẫu, không phải mọi KPI trên toàn lịch sử.
- [Nghiệm thu tích hợp 17/11](docs/INTEGRATION_ACCEPTANCE.md): mock 68 dòng → 60 hợp lệ + 8 rejects, 62 PASS + 1 WARN kỳ vọng; hai tháng mẫu có 4.283.724 dòng hợp lệ, bộ test chung 60/60 PASS. Đầu ra là projection `qa.acceptance_fact_input`, **chưa phải nghiệm thu rerun/publish T02 hoặc toàn Master**.
- Lượt kiểm tra 19/11 ghi nhận thêm **2.964.624 dòng 01/2024**, tổng Fact **111.835.256 dòng**. Artifact dbt gần nhất tại thời điểm kiểm tra có **47/47 PASS**; không thay thế nghiệm thu nghiệp vụ tháng mới. Artifact/runtime cục bộ không được đóng gói đầy đủ trong Git.

Quarantine bằng 0 không có nghĩa không còn lỗi nghiệp vụ. Tổng tiền nguồn và tổng tiền đủ điều kiện có bộ lọc khác nhau; số liệu lịch sử và toàn kho phải ghi riêng.

## 2. Kiến trúc dữ liệu và contract

```text
Parquet + taxi zone lookup/shapefile
          │
          ├── SHA256, footer, schema → meta.file_manifest / meta.batch_log
          ▼
staging.raw_yellow_trips             19 trường nguồn dạng chuỗi + 5 lineage
          ▼
stg_yellow_trips_typed              Ép kiểu an toàn
          ├── stg_yellow_trips_rejects → quarantine qua bước nạp tương ứng
          ▼
stg_yellow_trips → stg_trips_flagged Chuẩn hóa + DQ
          ▼
8 dimensions → dds.FactTaxiTrip     1 dòng / bản ghi chuyến nguồn
          ▼
dbt tests + B03/B04 → đầu vào Power BI
```

- **meta:** manifest và nhật ký batch. **staging:** raw, views biến đổi và quarantine. Raw là vùng đệm theo file, không giữ đồng thời 36 tháng.
- **dds:** `DimDate`, `DimTime`, `DimZone`, `DimVendor`, `DimPaymentType`, `DimRateCode`, `DimPassengerGroup`, `DimDistanceBand`, `FactTaxiTrip`.
- **Fact:** 1 PK + 11 FK + 3 thuộc tính giữ lại + 13 measures + 7 trường DQ + 5 lineage. `TripKey CHAR(32)` sinh từ MD5 của SHA256 file và số dòng nguồn; UNIQUE trên `(SourceFileSHA256, SourceRowNumber)`.
- **GIS:** 265 mã lookup + Unknown = 266 dòng DimZone; 263 hình học, SRID 4326, index GiST.

[Data Contract v1.2](docs/DATA_CONTRACT_AND_MOCK_GUIDE.md) quy định schema, mapping, khóa và DQ:

- Loader hợp nhất `airport_fee` (01/2021–01/2023) và `Airport_fee` (02/2023–12/2023) về một cột raw. B01 ép kiểu; B02 giữ dòng hợp lệ về kiểu và gắn cờ nghiệp vụ.
- Bảy trường DQ gồm ba cờ usable, `HasDQIssue`, `DataQualityFlags`, `AttributeQualityFlags`, `QualityRuleVersion`. Cảnh báo thuộc tính không tự thay đổi ba cờ usable.
- Seed vendor/rate dùng chung cho DQ và dimension. Dữ liệu thật dùng `VERIFIED`; mock thêm `FIXTURE`. RateCode 99 thật → FK 0 và `UNMAPPED_RATE_CODE`; fixture → FK 99.
- DimDate hiện phủ 31/12/2020–31/12/2030, có 3.654 dòng kể cả Unknown. **Miền DQ lịch sử vẫn là 31/12/2020–01/01/2024**; mở rộng dimension không tự mở rộng miền DQ.

## 3. Luồng thực thi và việc còn lại

| Luồng | Vai trò và trạng thái |
| --- | --- |
| [Script S03](scripts/run_s03_batch_36m.py) | Đã dùng nạp lịch sử theo file; Fact LOGGED, raw UNLOGGED; tạm gỡ và khôi phục constraints/indexes |
| [Master Mage](lab_project/pipelines/master_etl_pipeline/metadata.yaml) | 7 block; đã chạy thử ingestion → staging/dimensions → Fact → audit → benchmark |
| [Initial Backfill Mage](lab_project/pipelines/initial_backfill_pipeline/metadata.yaml) | Khung 5 block; nhánh cần nạp trong backfill_execute_bulk chưa gọi vòng nạp thật |

Master có hai nhánh đầu vào `check_or_ingest_zones` và `scan_parquet_manifest → export_parquet_chunks`, hội tụ tại `dbt_staging_and_dims`, sau đó chạy `dbt_fact_incremental → quality_audit_and_tests → record_benchmark_metrics`.

Fact mặc định dùng **delete+insert theo TripKey**, không thay toàn bộ tháng theo ngày đón. S03 bật `s03_fast_append` riêng.

### Các giới hạn đã xác định

1. **DQ 01/2024:** tại lần kiểm tra, 2.884.020 dòng có `DATE_OUT_OF_DOMAIN` do dùng miền ngày lịch sử. Chưa coi tháng này đạt nghiệm thu DQ nghiệp vụ.
2. **Manifest:** ingestion có thể ghi SUCCESS trước khi Fact/audit hoàn tất; 01/2024 có `fact_rows=0` trong manifest dù đã có dữ liệu trong Fact.
3. **Nhiều file mới:** loader TRUNCATE raw trước mỗi file, trong khi Master chạy Fact sau ingestion. Cần xử lý trọn chu trình theo file; chưa dùng Master quét nhiều file mới như luồng đã nghiệm thu.
4. **Hard gate:** block audit ghi kết quả dbt nhưng chưa raise khi tests thất bại. Trạng thái xanh trên Mage không đủ chứng minh kiểm toán đạt.
5. **Phục hồi:** T04 có năm mô phỏng PASS, chưa chứng minh đầy đủ kill/restart ETL, tự phục hồi và đọc Fact đồng thời. S03 skip theo số dòng không thay thế đối soát nội dung/hash; DELETE file cũ và nạp lại không nằm trong một transaction publish duy nhất.
6. **Lập lịch:** đã có cấu hình @once/@monthly; chưa nghiệm thu monthly nhiều kỳ. Mô tả “ngày 5” phải đối chiếu lịch và múi giờ thực tế.

Hoàn thiện trên database thử nghiệm trước, giữ nguyên baseline 36 tháng. Một số runbook cũ có kết luận rộng hơn phạm vi kiểm thử: đọc cùng thời điểm, selector, log và các giới hạn ở đây.

## 4. Cấu trúc repo

```text
compose.yaml / Dockerfile      PostgreSQL/PostGIS, Mage, dbt và thư viện Python
.env.example                  Mẫu biến môi trường
postgres/                     DDL khởi tạo và SQL kiểm tra
lab_project/                  Blocks và DAG Mage
dbt_project/                  Models, macros, seeds, tests, profiles
data/mock/                    Fixture nhỏ theo contract
scripts/                      Orchestration, acceptance và audit
sql/                          Truy vấn B03/B04 và benchmark spatial
docs/                         Contract, runbook, nghiệm thu, dbt docs
reports/b03/ / reports/b04/    Kết quả SQL và đối soát đã lưu
logs/ / mage_data/             Runtime cục bộ, bỏ qua bởi Git
```

## 5. Chuẩn bị môi trường

Yêu cầu Docker Desktop/Compose, dung lượng cho dữ liệu và index, cùng Parquet/lookup/shapefile nguồn. Repo không chứa database hoặc 36 Parquet để khôi phục toàn kho chỉ bằng clone.

- PostgreSQL/PostGIS: image `postgis/postgis:16-3.4`.
- Mage: image nền `mageai/mageai:latest` — chưa khóa phiên bản Mage.
- Dockerfile pin dbt-core/dbt-postgres 1.8.0, PyArrow 16.1.0 và các thư viện liên quan.
- Bind mount mặc định: **../../Data → /home/src/external_data:ro**. Kiểm tra đường dẫn theo vị trí clone. Scanner hỗ trợ quét thư mục con; shapefile cần đủ các file thành phần đi kèm.

Tại thư mục chứa README:

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
```

Điền thông tin kết nối và thay mật khẩu mẫu trong `.env`; không commit file này. Trước khi khởi động môi trường có dữ liệu, kiểm tra triggers: Mage có thể kích hoạt lịch đang bật.

```powershell
docker compose config --quiet
docker compose up -d --build
docker compose ps
docker compose port postgres 5432
docker compose exec -T mage ls -lh /home/src/external_data
```

- Mage UI mặc định: [localhost:6789](http://localhost:6789).
- Trong Compose, Mage dùng **postgres:5432**. Công cụ host dùng localhost và cổng xuất bởi `docker compose port postgres 5432`; cổng host có thể khác 5432 theo cấu hình máy.
- Volume database tên `yellow_taxi_pgdata`. SQL trong `postgres/init` chỉ tự chạy khi khởi tạo dữ liệu PostgreSQL mới; sửa DDL hoặc `.env` không tự cập nhật database đã tồn tại.
- `docker compose down` giữ volume. Không dùng `down -v` khi cần giữ kho dữ liệu.

## 6. Kiểm thử và vận hành

Ưu tiên đọc [nghiệm thu tích hợp](docs/INTEGRATION_ACCEPTANCE.md), [B03](reports/b03/README.md), [B04](docs/B04_BAO_CAO_STAGING_DQ.md) trước khi chạy lại. Dùng database nghiệm thu riêng với tên `taxi_acceptance_*` chưa tồn tại và xem giới hạn projection trong hướng dẫn. Không chạy mock/full-refresh trên database phục vụ báo cáo.

| Script | Phạm vi |
| --- | --- |
| `scripts/run_t03_master.ps1 -TargetFile <file.parquet>` | Master cho một file; ghi vào database cấu hình, cần đọc giới hạn incremental |
| `scripts/run_s03_36months.ps1` | Bulk lịch sử; có DDL, TRUNCATE, DELETE và nạp Fact, không chạy chỉ để xem trạng thái |
| `scripts/prepare_acceptance.py` / `run_integration_acceptance.py` | Chuẩn bị DB riêng, kiểm thử projection/mock/sample; xem phạm vi và kết quả lịch sử trong runbook |
| `scripts/run_b03.ps1` | Truy vấn nghiệp vụ/audit và xuất kết quả |
| `scripts/b04_reconciliation.py` / `b04_dq_review.py` | Đối soát, rà soát DQ; kiểm tra phạm vi file và tác động I/O trước khi chạy |
| `scripts/run_t04_drills.ps1` | Diễn tập có chèn/dọn dữ liệu giả; không phải status chỉ đọc hoặc auto-recovery production |

Không chạy đồng thời ingestion/bulk/master trên cùng raw và Fact. Đối chiếu log, manifest, Fact và artifact tests sau mỗi lượt; không chỉ dựa vào UI. `dbt_project/target/run_results.json` có thể bị lượt sau ghi đè: lưu bằng chứng theo database/run/thời điểm.

### Quy ước KPI cho bước BI

- **Sản lượng:** COUNT hoặc SUM(TripCount), không loại chuyến chỉ vì DQ.
- **Tổng tiền nguồn:** SUM(TotalAmount); **tổng tiền đủ điều kiện:** lọc IsAmountUsable.
- **Vận tốc tổng hợp:** `3600 × SUM(TripDistanceMiles) / SUM(TripDurationSeconds)`, lọc time/distance usable và bảo vệ mẫu số 0.
- **Tỷ lệ tip:** tỷ số tổng tip/tổng fare trên thanh toán thẻ, fare > 0 và amount usable; ghi rõ bộ lọc.
- Tách tháng file nguồn khỏi tháng của ngày đón. Dashboard Power BI là công việc tiếp theo.

## 7. Tài liệu và bằng chứng

| Nội dung | Tài liệu |
| --- | --- |
| Contract dùng trong triển khai | [Data Contract v1.2](docs/DATA_CONTRACT_AND_MOCK_GUIDE.md) |
| Mock và hai file mẫu | [Integration acceptance](docs/INTEGRATION_ACCEPTANCE.md) |
| Fact và Master | [T02–T03 runbook](docs/TASK_T02_T03_RUNBOOK.md) |
| Benchmark 01/2021 | [Benchmark 1 tháng](docs/BENCHMARK_1M.md) |
| Manifest 36 tháng | [Manifest](docs/manifest_36_months.csv) |
| SQL nghiệp vụ và kiểm chứng độc lập | [B03 run summary](reports/b03/run_summary.md), [Parquet crosscheck](reports/b03/crosscheck_parquet.md) |
| DQ và tài chính | [B04 report](docs/B04_BAO_CAO_STAGING_DQ.md), [reconciliation](reports/b04/reconciliation_summary.md) |
| Diễn tập vận hành | [T04 runbook](docs/TASK_T04_RUNBOOK.md) — đọc kèm giới hạn mô phỏng |
| Truy vấn spatial | [S04 benchmark SQL](sql/s04_spatial_benchmark.sql) |

Logs, dbt targets, dữ liệu thật và runtime Mage bị `.gitignore` loại. Báo cáo kết quả có trong Git không có nghĩa mọi artifact lớn cũng được cung cấp; đường dẫn trong tài liệu cũ có thể thuộc workspace gốc.

## 8. Phân công và đóng góp

| Thành viên | Phạm vi |
| --- | --- |
| Phạm Lê Thanh Tú — TTU | T01 hạ tầng/DDL; T02 Fact; T03 Master; T04 vận hành/diễn tập |
| Nguyễn Ngọc Bình — NBINH | B01 staging/quarantine; B02 DQ/mapping; B03 SQL/audit; B04 đối soát/báo cáo |
| Kiều Đỗ Song Tinh — STINH | S01 ingestion/manifest; S02 dimensions; S03 bulk lịch sử; S04 GIS/dbt docs |

Commit mã, cấu hình mẫu, tài liệu và fixture nhỏ. Không commit mật khẩu thật, `.env`, database, Parquet hoặc runtime. Khi đổi contract/rule, ghi phiên bản, phạm vi ảnh hưởng và bằng chứng kiểm thử; không đồng nhất mapping fixture với danh mục sản xuất đã xác minh.

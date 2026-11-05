# NYC Yellow Taxi BI — Nhóm 09

Kho mã phục vụ đồ án phân tích NYC Yellow Taxi 2021–2023 bằng Mage, dbt và PostgreSQL/PostGIS. Hiện repo là **khung khởi tạo và demo kết nối**, chưa phải pipeline ETL Taxi hoàn chỉnh.

## Tài liệu và dữ liệu mẫu

- [Data Contract v1.2](docs/DATA_CONTRACT_AND_MOCK_GUIDE.md): schema nguồn, miền ngày, khóa dimension, DQ và nghiệm thu; trạng thái chờ nhóm xác nhận được giữ theo bản nguồn.
- [Mock 18 dòng](data/mock/mock_yellow_trips.csv): dữ liệu giả lập dùng để phát triển và kiểm thử.
- Contract được sao chép nguyên nội dung từ workspace BI_Project. Những đường dẫn nguồn cũ trong contract thuộc workspace đó; trong repo này dùng đường dẫn mock ở trên. Kế hoạch `KE_HOACH_ETL_GIAI_DOAN_2_NHOM_09.pdf` và dữ liệu thật được quản lý ngoài repo.
- Bộ 36 Parquet có baseline 108.870.632 dòng. Dữ liệu thật không được đóng gói trong Git; chưa có mount/loader Taxi trong cấu hình hiện tại.

## Cấu trúc

```text
compose.yaml                 PostgreSQL và Mage
Dockerfile                   Mage cùng dbt-postgres, psycopg2-binary
.env.example                 Mẫu cấu hình kết nối
postgres/init/01_init.sql     Schema demo hiện tại
lab_project/                 Mã và pipeline Mage
docs/                       Data Contract
data/mock/                  Fixture nhỏ được đưa vào Git
mage_data/                  Trạng thái chạy cục bộ, được Git bỏ qua
```

## Chạy môi trường demo

Yêu cầu Docker Desktop đang chạy và Docker Compose. Thực hiện lệnh tại thư mục chứa README này.

1. Tạo `.env` từ mẫu nếu chưa có. Giữ cấu hình đang dùng nếu máy đã có database:

   ```powershell
   if (-not (Test-Path .env)) { Copy-Item .env.example .env }
   ```

2. Điền mật khẩu cục bộ trong `.env`. Mage kết nối `postgres:5432` qua mạng Compose. Công cụ chạy trực tiếp trên máy dùng `localhost:5432`.

3. Kiểm tra cấu hình và khởi động:

   ```powershell
   docker compose config --quiet
   docker compose up -d --build
   docker compose ps
   ```

4. Mở Mage tại http://localhost:6789, chạy pipeline `hello_mage`. Pipeline tạo ba dòng Alice/Bob/Carol và ghi đè bảng `stage.demo`; đây là demo kết nối, không nạp fixture Taxi.

   Có thể kiểm tra kết quả từ service PostgreSQL:

   ```powershell
   docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT * FROM stage.demo;"'
   ```

5. Xem log hoặc dừng môi trường:

   ```powershell
   docker compose logs --tail 100 mage postgres
   docker compose down
   ```

Database được giữ trong named volume `pgdata`. Biến khởi tạo và script `postgres/init` áp dụng khi tạo database mới; thay `.env` hoặc SQL không tự cập nhật database đã tồn tại. Không dùng tùy chọn xóa volume khi cần giữ dữ liệu.

Các lệnh trên là hướng dẫn chạy; repo chưa có bằng chứng kiểm thử end-to-end Taxi. Cổng 5432/6789 và tên container `lab-postgres`/`lab-mage` cần còn khả dụng.

## Trạng thái và việc tiếp theo

| Hiện tại | Mục tiêu triển khai |
| --- | --- |
| PostgreSQL 17, chưa cấu hình PostGIS | Thống nhất PostgreSQL 16 + PostGIS theo kế hoạch, hoặc ghi nhận quyết định đổi phiên bản |
| Schema demo `stage`, `dds`, `control` | Schema chính thức `staging`, `dds`, `meta` |
| Mage dùng `latest`, thư viện chưa khóa phiên bản | Chốt bộ phiên bản tương thích |
| dbt adapter đã được cài trong image Mage | Bổ sung dbt project, models và tests; hiện chưa có service dbt riêng |
| Loader/validator mẫu cho transactions | Ingestion Parquet/zone, chuẩn hóa, quarantine và DQ theo contract |
| Mock và contract có sẵn | Bổ sung expected, seed mapping, tám dimensions và Fact |

Trong container, fixture nằm tại `/home/src/data/mock/mock_yellow_trips.csv` nhờ bind mount repo. Loader mẫu `load_csv.py` đọc mặc định `/home/src/data/batch_01.csv`; file đó không được cung cấp và validator transactions không dùng trực tiếp cho fixture Taxi.

## Phân công

| Thành viên | Chủ trì |
| --- | --- |
| ttu — Phạm Lê Thanh Tú | Hạ tầng, Fact, điều phối và phục hồi |
| nbinh — Nguyễn Ngọc Bình | Contract, staging, validator/quarantine, DQ và audit |
| stinh — Kiều Đỗ Song Tinh | Ingestion, manifest, tám dimensions và GIS |

## Quy ước đưa mã lên GitHub

Commit mã, cấu hình mẫu, tài liệu và fixture nhỏ. `.gitignore` loại `.env`, runtime Mage, cache, cấu hình SSH, dữ liệu lớn và database cục bộ; `.dockerignore` cũng loại cấu hình riêng khỏi build context.

Trước khi commit, kiểm tra danh sách file và diff để bảo đảm chỉ có nội dung dự kiến. Không đưa mật khẩu thật vào `.env.example`, README hoặc mã nguồn. Khi sửa contract, đồng bộ bản dùng chung và ghi rõ version; không xem fixture mapping là danh mục sản xuất đã xác minh.

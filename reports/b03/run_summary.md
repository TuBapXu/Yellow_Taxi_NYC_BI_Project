# B03 - Kết quả chạy truy vấn kiểm chứng và audit DQ

- Thời điểm chạy (UTC): 2026-11-17 20:28:26
- Phạm vi: **108,870,632 dòng Fact** từ 36 file: `yellow_tripdata_2021-01.parquet`, `yellow_tripdata_2021-02.parquet`, `yellow_tripdata_2021-03.parquet`, `yellow_tripdata_2021-04.parquet`, `yellow_tripdata_2021-05.parquet`, `yellow_tripdata_2021-06.parquet`, `yellow_tripdata_2021-07.parquet`, `yellow_tripdata_2021-08.parquet`, `yellow_tripdata_2021-09.parquet`, `yellow_tripdata_2021-10.parquet`, `yellow_tripdata_2021-11.parquet`, `yellow_tripdata_2021-12.parquet`, `yellow_tripdata_2022-01.parquet`, `yellow_tripdata_2022-02.parquet`, `yellow_tripdata_2022-03.parquet`, `yellow_tripdata_2022-04.parquet`, `yellow_tripdata_2022-05.parquet`, `yellow_tripdata_2022-06.parquet`, `yellow_tripdata_2022-07.parquet`, `yellow_tripdata_2022-08.parquet`, `yellow_tripdata_2022-09.parquet`, `yellow_tripdata_2022-10.parquet`, `yellow_tripdata_2022-11.parquet`, `yellow_tripdata_2022-12.parquet`, `yellow_tripdata_2023-01.parquet`, `yellow_tripdata_2023-02.parquet`, `yellow_tripdata_2023-03.parquet`, `yellow_tripdata_2023-04.parquet`, `yellow_tripdata_2023-05.parquet`, `yellow_tripdata_2023-06.parquet`, `yellow_tripdata_2023-07.parquet`, `yellow_tripdata_2023-08.parquet`, `yellow_tripdata_2023-09.parquet`, `yellow_tripdata_2023-10.parquet`, `yellow_tripdata_2023-11.parquet`, `yellow_tripdata_2023-12.parquet`
- Tổng thời gian: 2173.9 giây

| Mã | Nội dung | Số dòng kết quả | Thời gian (s) |
|---|---|---:|---:|
| CB01 | Lưu lượng chuyến và hành khách theo ngày, tháng, năm | 1,151 | 72.14 |
| CB02 | Doanh thu theo Quận và Vendor tại từng thời điểm: năm, tháng, ngày, giờ | 16,717 | 219.97 |
| CB03 | Số chuyến theo phương thức thanh toán, quận, tháng | 1,276 | 65.2 |
| CB04 | Nhịp độ đón và trả khách theo khung giờ, thứ, tháng | 4,080 | 85.91 |
| CB05 | Hành vi đi theo nhóm khách, biểu phí và dải cự ly | 126 | 72.13 |
| CB06 | Tổng hợp hoạt động vận tải theo khu vực (nền cho dashboard) | 266 | 111.59 |
| CB07 | Top 10 tuyến lộ trình phổ biến nhất (đón -> trả) | 10 | 62.95 |
| CB08 | Tổng hợp doanh thu toàn hệ thống: GỘP và THUẦN (đủ điều kiện) | 9 | 108.49 |
| MR01 | Tương quan biểu phí với tỷ lệ tiền boa và vận tốc | 7 | 74.64 |
| MR02 | Biến động doanh thu Tháng 5 và Tháng 6 (từng năm 2021–2023) | 6 | 32.33 |
| MR03 | Phân tích tăng doanh thu đơn vị (doanh thu / chuyến) theo tháng | 41 | 37.73 |
| MR04 | Tăng trưởng cùng kỳ: MoM và YoY cho đủ 36 tháng 2021–2023 | 36 | 35.77 |
| MR05 | MR5a - Bản đồ GIS: lượng chuyến ĐÓN và TRẢ khách theo khu vực và khung giờ | 1,044 | 91.33 |
| MR05B | MR5b - Bảng doanh thu theo từng Quận qua các tháng trong năm | 264 | 39.79 |
| MR05B_GIO | MR5b - Xu hướng chuyến và doanh thu theo khung giờ trong ngày, từng quận | 593 | 34.77 |
| DQ01 | Tỷ lệ cờ chất lượng theo tháng của file nguồn | 37 | 130.19 |
| DQ02 | Tần suất từng mã lỗi / cảnh báo theo năm của file nguồn | 51 | 177.84 |
| DQ03 | Khối khuyết đồng loạt so với baseline khảo sát (4.156.354 dòng = 3,8177%) | 4 | 206.23 |
| DQ04 | Kiểm toán doanh thu gộp và thuần theo năm của file, tách phần bị loại theo mã lỗi tiền | 4 | 262.88 |
| DQ05 | Các ca cực trị (top 10 mỗi nhóm) kèm lineage để truy về file gốc | 50 | 200.5 |
| DQ06 | Đối soát số dòng theo file: Footer Parquet = Fact + Quarantine; tiền staging = Fact (file còn trong staging) | 36 | 51.49 |

## Tự kiểm chéo

| Kiểm tra | Kết quả | Chi tiết |
|---|---|---|
| CB01 tổng chuyến = COUNT(Fact) | PASS | 108,870,632 vs 108,870,632 |
| CB03 tổng chuyến = COUNT(Fact) | PASS | 108,870,632 |
| CB04 mỗi vai trò = COUNT(Fact) | PASS | {'DROPOFF': 108870632, 'PICKUP': 108870632} |
| CB06 tổng đón = tổng trả = COUNT(Fact) | PASS | 108,870,632 / 108,870,632 |
| CB02 mỗi cấp năm/tháng/ngày/giờ: tổng doanh thu thuần = CB08 | PASS | GIO 2,571,195,047.14, NAM 2,571,195,047.14, NGAY 2,571,195,047.14, THANG 2,571,195,047.14 vs 2,571,195,047.14 |
| MR05B tổng doanh thu thuần = CB08 | PASS | 2,571,195,047.14 |
| MR05B_GIO tổng doanh thu thuần = CB08 | PASS | 2,571,195,047.14 |
| MR05 (5a) tổng đón / trả = CB06 trên các zone có đa giác | PASS | đón 107,548,824 vs 107,548,824, trả 107,597,006 vs 107,597,006 |
| CB05 tổng doanh thu thuần = CB08 | PASS | 2,571,195,047.14 |
| DQ04 gộp/thuần = CB08 (cùng Fact, khác cách nhóm) | PASS | gộp 2,558,482,028.00, thuần 2,571,195,047.14 |
| DQ06 footer = Fact + Quarantine (mọi file); tiền staging = Fact (file còn trong staging) | PASS | 36 file, 0 file lệch dòng; 1 file đối soát tiền |
| MR04 đủ 36 tháng lịch | PASS | 36 tháng, 36 tháng có dữ liệu |
| CB07 không chứa zone 0/264/265 | PASS | 10 tuyến |

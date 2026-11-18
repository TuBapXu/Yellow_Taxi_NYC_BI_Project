# B03 — SQL kiểm chứng nghiệp vụ & Audit DQ (nbinh)

**Trạng thái (18/11): HOÀN THÀNH trên đủ 36 file = 108.870.632 dòng Fact.** 21 câu chạy xong, **13/13 tự kiểm PASS**
(xem `run_summary.md`). Tiêu chí nghiệm thu DQ03: khối khuyết đồng loạt = **4.156.354 dòng = 3,8177%**, khớp tuyệt đối baseline khảo sát.
Số liệu mục 5 bên dưới là bản chạy thử 2 file (giữ lại để đối chiếu); số chính thức 3 năm nằm trong `results/*.csv` và `dq_audit_3y.xlsx`.

## 1. Sản phẩm

| File | Nội dung |
|---|---|
| `sql/acceptance_queries.sql` | 15 truy vấn CB01–CB08, MR01–MR05, MR05B, MR05B_GIO (mỗi câu có comment: yêu cầu, cờ lọc, cách tự kiểm) |
| `sql/dq_audit_queries.sql` | 6 truy vấn audit DQ01–DQ06 (tỷ lệ cờ, tần suất mã, baseline 3,81%, kiểm toán doanh thu, cực trị, đối soát) |
| `scripts/run_b03.ps1` | Chạy tất cả, xuất kết quả. Thêm `-CrossCheck` để kiểm chứng độc lập từ Parquet |
| `scripts/b03_export.py` | Chạy SQL, xuất CSV, Excel, GeoJSON và 10 phép tự kiểm chéo giữa các câu |
| `scripts/b03_crosscheck.py` | Tính lại chỉ số chính bằng pandas trực tiếp từ Parquet gốc rồi so với SQL |
| `reports/b03/results/*.csv` | Kết quả mẫu 19 câu (CSV UTF-8 mở được bằng Excel) |
| `reports/b03/results/MR05_zones.geojson` | 263 đa giác zone (EPSG:4326) cho bản đồ MR05 |
| `reports/b03/dq_audit_3y.xlsx` | Bảng thống kê DQ + kiểm toán doanh thu + CB08 + MR01 + kết quả tự kiểm |
| `reports/b03/run_summary.md` | Thời gian chạy, số dòng mỗi câu, tự kiểm |
| `reports/b03/crosscheck_parquet.md` | Kết quả kiểm chứng độc lập (41/41 PASS) |

### Đối chiếu đề `cq_BI_PROJECT_LAB_2627.pdf`

| Đề | Câu | Ghi chú |
|---|---|---|
| CB 1–8 | CB01–CB08 | CB02 có 4 cấp thời gian (`cap_thoi_gian` = NAM / THANG / NGAY / GIO) theo yêu cầu "tại một thời điểm cụ thể (ngày, giờ, tháng, năm)" |
| MR 1–4 | MR01–MR04 | Phần giải thích nguyên nhân (MR2, MR3) viết trong báo cáo B04 dựa trên số liệu các câu này |
| MR 5a – bản đồ đón/trả | MR05 + `MR05_zones.geojson` | Có cả `so_chuyen_don` và `so_chuyen_tra`, mật độ theo km² và chênh lệch trả − đón |
| MR 5b – doanh thu quận theo tháng | MR05B | Dạng dài quận × năm × tháng, kèm tỷ trọng quận trong tháng (pivot thành ma trận trong Power BI) |
| MR 5b – xu hướng theo khung giờ | MR05B_GIO | Quận × năm × giờ 0–23 (kèm TimeBand 4 khung) |

## 2. Cách chạy

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_b03.ps1              # chạy 19 câu + xuất kết quả (~1 phút với 2 file)
powershell -ExecutionPolicy Bypass -File scripts\run_b03.ps1 -CrossCheck  # thêm kiểm chứng độc lập (chỉ khi Fact có ít file)
```

Cần Docker đang chạy và Fact đã nạp (T03). Script chỉ **đọc** database. Log: `logs\b03_*.log`.

## 3. Định nghĩa đã chốt

| Khái niệm | Định nghĩa | Căn cứ |
|---|---|---|
| Doanh thu **gộp** | `SUM(TotalAmount)` mọi dòng Fact | Kế hoạch GĐ2, bảng 2.5, CB08 ("tổng tiền nguồn") |
| Doanh thu **thuần (đủ điều kiện)** | `SUM(TotalAmount) WHERE IsAmountUsable` | CB08 ("tổng tiền đủ điều kiện"); Contract v1.2 mục 4 dòng 13 |
| Vận tốc TB | `3600 × SUM(dặm) / SUM(giây)` trên dòng time + distance usable | Kế hoạch 2.4 (không AVG đơn giản) |
| Tỷ lệ tip | `100 × SUM(tip) / SUM(fare)`, chuyến trả thẻ, fare > 0, amount usable | Kế hoạch 2.4 |
| Khối khuyết đồng loạt | passenger, rate, store flag, congestion đều NULL **và** payment = 0 | Kế hoạch 2.4 (baseline 4.156.354 dòng = 3,8177%) |
| Ngày/khu vực | Theo vai trò **đón khách**, trừ CB04 (tách đón/trả) và CB06 (thêm số chuyến trả) | Quy ước B03 |

## 4. Kiểm chứng

1. **Tự kiểm chéo (10/10 PASS):** tổng chuyến CB01 = CB03 = CB04 (mỗi vai trò) = CB06 = COUNT(Fact); doanh thu thuần CB02 = CB05 = CB08 = DQ04; staging = Fact (DQ06); MR04 đủ 36 tháng; CB07 không chứa zone 0/264/265.
2. **Kiểm chứng độc lập từ Parquet gốc (41/41 PASS):** cài lại rule Contract v1.2 bằng pandas, tính trực tiếp từ 2 file Parquet (không qua staging/dbt/SQL). Khớp **đến từng chuyến và từng cent**: số chuyến theo tháng, doanh thu gộp/thuần, tỷ lệ tip và vận tốc của 7 biểu phí, top 10 tuyến, khối khuyết, số dòng lỗi time/cự ly/tiền.

## 5. Kết quả trên 2 file thử (chưa phải số nghiệm thu 3 năm)

| Chỉ số | Giá trị |
|---|---|
| Doanh thu gộp / thuần | $102.316.869,49 / $102.992.854,62 |
| Số chuyến bị loại khỏi doanh thu thuần | 32.703 (0,76%) |
| Khối khuyết đồng loạt | 175.169 dòng = **4,09%** (2021-01: 7,18%; 2023-02: 2,64%) — baseline 3 năm 3,81% |
| Dòng có lỗi usable (HasDQIssue) | 95.686 |
| Vận tốc TB (biểu phí chuẩn) | 11,08 mph (AVG đơn giản sẽ ra 12,38 — sai) |
| Tỷ lệ tip (biểu phí chuẩn) | 23,63% |
| Tuyến phổ biến nhất | 237 → 236 (Upper East Side South → North), 31.613 chuyến |

## 6. Phát hiện cần đưa vào B04

- **Doanh thu thuần lớn hơn doanh thu gộp** ($676 nghìn). Gộp cộng cả giao dịch âm (hoàn/hủy, total âm khoảng −$686 nghìn); thuần loại chúng ra nên tổng tăng. Đây là điểm cần giải thích rõ trong báo cáo, không phải lỗi.
- **Cấu thành cước:** 1.188.260 dòng có tổng 8 thành phần (fare + extra + mta + tip + tolls + improvement + congestion + airport) lệch `TotalAmount` hơn 1 cent, tổng lệch −$2,51 triệu (thành phần > total). Rule DQ chỉ kiểm cận dưới `total + 0,01 ≥ fare + tip + tolls` nên các dòng này vẫn usable. Cần khảo sát thêm trong B04 (có thể do cách TLC ghi `extra`/phụ phí theo năm).
- **MR01 biểu phí 6 (Group ride):** chỉ 17 chuyến, tỷ lệ tip 242,71% (tip > fare). Mẫu quá nhỏ, không kết luận được.
- **MR01 so sánh vận tốc:** AVG đơn giản cho kết quả sai rất xa với nhóm ít chuyến (Negotiated: 93,54 so với 24,76 mph đúng) — minh họa vì sao phải dùng trọng số.
- **Ca cực trị (DQ05):** total lớn nhất $7.661,28 (2021-01 dòng 108759); cự ly 335.004 dặm (2023-02 dòng 2892310); thời lượng 1.729.062 giây ≈ 20 ngày (2021-01 dòng 302617). Đều đã bị cờ loại khỏi chỉ số tương ứng.
- **RateCode 99:** đang về key 0 (FIXTURE chưa xác minh), nên nhóm "Unknown / Unmapped Rate" có 188.242 chuyến.

## 7. Việc còn lại sau S03

1. Chạy `scripts\run_b03.ps1` trên đủ 36 file, cập nhật `dq_audit_3y.xlsx`.
2. Đối chiếu DQ03 với tiêu chí ~3,81%.
3. MR02 (tháng 5/6) và MR04 (MoM/YoY) mới có số thật khi đủ tháng — hiện tại chỉ 5/36 tháng có dữ liệu.
4. Nếu CB04/DQ chạy chậm trên 108 triệu dòng: tối ưu bằng index/bảng tổng hợp (không đổi logic).

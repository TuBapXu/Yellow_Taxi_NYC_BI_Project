-- =============================================================================
-- B03 (nbinh) - 15 TRUY VẤN KIỂM CHỨNG NGHIỆP VỤ (CB01–CB08, MR01–MR05, MR05B, MR05B_GIO)
--   Bám đủ đề cq_BI_PROJECT_LAB_2627: CB02 có đủ 4 mốc năm/tháng/ngày/giờ; MR05 (5a) có cả đón và trả;
--   MR05B + MR05B_GIO là yêu cầu 5b (doanh thu quận theo tháng, xu hướng theo khung giờ).
-- Nguồn: dds."FactTaxiTrip" + 8 Dimension (Single DDS). Không đọc staging.
--
-- QUY ƯỚC CHUNG (áp dụng cho mọi câu, ghi lại để không phải nhắc lại):
--   * Ngày / giờ / khu vực mặc định theo vai trò ĐÓN KHÁCH (Pickup*Key), trừ CB04
--     (tách riêng đón và trả) và CB06 (có thêm số chuyến trả).
--   * Đếm chuyến: KHÔNG lọc cờ (Preserve & Flag - giữ đủ 100% sản lượng).
--   * Doanh thu: chỉ dòng "IsAmountUsable".
--       - Doanh thu GỘP  = SUM("TotalAmount") trên mọi dòng (tổng tiền nguồn).
--       - Doanh thu THUẦN (đủ điều kiện) = SUM("TotalAmount") WHERE "IsAmountUsable".
--       Định nghĩa theo Kế hoạch GĐ2 bảng 2.5 (CB08) và Data Contract v1.2 mục 4.
--   * Vận tốc / cự ly / thời lượng: chỉ dòng "IsTimeUsable" AND "IsDistanceUsable".
--   * Vận tốc trung bình = 3600 * SUM(dặm) / SUM(giây) (có trọng số), KHÔNG dùng AVG(vận tốc).
--   * Tỷ lệ tip = 100 * SUM(tip) / SUM(fare) trên chuyến trả thẻ ("IsElectronicTipObservable"),
--     "FareAmount" > 0 và "IsAmountUsable" (loại cả ca tip cực trị 133 triệu USD).
--   * Giữ nhóm Unknown (key 0) trong kết quả để tổng luôn khớp với Fact.
--   * Không ghi cứng năm/tháng: chạy lại sau S03 (đủ 36 file) không cần sửa câu nào.
--
-- ĐỊNH DẠNG: mỗi câu bắt đầu bằng dòng  "-- @query <MÃ> | <tiêu đề>"  để scripts/b03_export.py
-- tách và chạy từng câu. Có thể copy riêng từng câu chạy trong psql / DBeaver.
-- =============================================================================


-- @query CB01 | Lưu lượng chuyến và hành khách theo ngày, tháng, năm
-- ROLLUP sinh đủ 4 cấp tổng hợp; DateKey = 0 (ngoài miền ngày / thiếu ngày) hiện thành năm 1900.
-- Tự kiểm: dòng cap_tong_hop = 'TONG' phải bằng COUNT(*) của Fact.
select
    case
        when grouping(d."YearNumber") = 1 then 'TONG'
        when grouping(d."MonthNumber") = 1 then 'NAM'
        when grouping(d."FullDate") = 1 then 'THANG'
        else 'NGAY'
    end                                                       as cap_tong_hop,
    d."YearNumber"                                            as nam,
    d."MonthNumber"                                           as thang,
    d."FullDate"                                              as ngay,
    sum(f."TripCount")                                        as so_chuyen,
    sum(f."PassengerCount")                                   as so_khach,
    count(*) filter (where f."PassengerCount" is null)        as so_chuyen_thieu_so_khach
from dds."FactTaxiTrip" f
join dds."DimDate" d on d."DateKey" = f."PickupDateKey"
group by rollup (d."YearNumber", d."MonthNumber", d."FullDate")
order by nam nulls last, thang nulls last, ngay nulls last;


-- @query CB02 | Doanh thu theo Quận và Vendor tại từng thời điểm: năm, tháng, ngày, giờ
-- Đề: "doanh thu của mỗi quận theo từng nhà cung cấp tại một thời điểm cụ thể (ngày, giờ, tháng, năm)".
-- GROUPING SETS sinh 4 cấp thời gian độc lập, phân biệt bằng cột cap_thoi_gian:
--   NAM   (năm đón)  | THANG (năm-tháng) | NGAY (ngày cụ thể) | GIO (giờ trong ngày 0–23, gộp mọi ngày).
--   Muốn xem một giờ của một ngày cụ thể: lọc Fact theo PickupDateKey + DimTime.HourNumber (Power BI drill-down).
-- Doanh thu thuần = chỉ dòng IsAmountUsable; cột tien_bi_loai để đối chiếu phần bị loại.
-- Năm 1900 / giờ -1 = key 0 (thiếu hoặc ngoài miền ngày/giờ): vẫn tính để tổng khớp Fact.
-- Tự kiểm: SUM(doanh_thu_thuan) của MỖI cấp = doanh_thu_thuan dòng TONG của CB08.
select
    case
        when grouping(d."YearNumber") = 0 then 'NAM'
        when grouping(d."YearMonth")  = 0 then 'THANG'
        when grouping(d."FullDate")   = 0 then 'NGAY'
        else 'GIO'
    end                                                                   as cap_thoi_gian,
    d."YearNumber"                                                        as nam,
    d."YearMonth"                                                         as nam_thang,
    d."FullDate"                                                          as ngay,
    t."HourNumber"                                                        as gio,
    z."BoroughName"                                                       as quan,
    v."VendorName"                                                        as vendor,
    sum(f."TripCount")                                                    as so_chuyen,
    count(*) filter (where f."IsAmountUsable")                            as so_chuyen_tien_hop_le,
    sum(f."TotalAmount") filter (where f."IsAmountUsable")                as doanh_thu_thuan,
    sum(f."FareAmount") filter (where f."IsAmountUsable")                 as cuoc_dong_ho_thuan,
    round(sum(f."TotalAmount") filter (where f."IsAmountUsable")
          / nullif(count(*) filter (where f."IsAmountUsable"), 0), 2)     as doanh_thu_tb_chuyen,
    count(*) filter (where not f."IsAmountUsable")                        as so_chuyen_bi_loai,
    sum(f."TotalAmount") filter (where not f."IsAmountUsable")            as tien_bi_loai
from dds."FactTaxiTrip" f
join dds."DimDate"   d on d."DateKey"   = f."PickupDateKey"
join dds."DimTime"   t on t."TimeKey"   = f."PickupTimeKey"
join dds."DimZone"   z on z."ZoneKey"   = f."PickupZoneKey"
join dds."DimVendor" v on v."VendorKey" = f."VendorKey"
group by grouping sets (
    (d."YearNumber", z."BoroughName", v."VendorName"),
    (d."YearMonth",  z."BoroughName", v."VendorName"),
    (d."FullDate",   z."BoroughName", v."VendorName"),
    (t."HourNumber", z."BoroughName", v."VendorName")
)
order by 1, nam, nam_thang, ngay, gio, quan, vendor;


-- @query CB03 | Số chuyến theo phương thức thanh toán, quận, tháng
-- Giữ nhóm "Unknown / Missing Payment" (payment_type = 0, chủ yếu thuộc khối 4,15 triệu dòng khuyết).
-- ty_le_trong_quan_thang: tỷ trọng của phương thức trong cùng quận + tháng.
select
    d."YearMonth"                                                         as nam_thang,
    z."BoroughName"                                                       as quan,
    p."PaymentTypeName"                                                   as phuong_thuc,
    sum(f."TripCount")                                                    as so_chuyen,
    round(100.0 * sum(f."TripCount")
          / sum(sum(f."TripCount")) over (partition by d."YearMonth", z."BoroughName"), 2)
                                                                          as ty_le_trong_quan_thang_pct
from dds."FactTaxiTrip" f
join dds."DimDate"        d on d."DateKey"        = f."PickupDateKey"
join dds."DimZone"        z on z."ZoneKey"        = f."PickupZoneKey"
join dds."DimPaymentType" p on p."PaymentTypeKey" = f."PaymentTypeKey"
group by 1, 2, 3
order by 1, 2, so_chuyen desc;


-- @query CB04 | Nhịp độ đón và trả khách theo khung giờ, thứ, tháng
-- Role-playing: cùng DimDate/DimTime nhưng 2 luồng độc lập (PICKUP, DROPOFF) gộp bằng UNION ALL.
-- Tự kiểm: tổng so_chuyen của mỗi vai_tro = COUNT(*) của Fact.
with su_kien as (
    select 'PICKUP'::text as vai_tro, f."PickupDateKey" as date_key, f."PickupTimeKey" as time_key
    from dds."FactTaxiTrip" f
    union all
    select 'DROPOFF', f."DropoffDateKey", f."DropoffTimeKey"
    from dds."FactTaxiTrip" f
)
select
    s.vai_tro,
    d."MonthNumber"   as thang,
    d."DayOfWeek"     as thu_iso,
    d."DayName"       as ten_thu,
    d."IsWeekend"     as cuoi_tuan,
    t."HourNumber"    as gio,
    t."TimeBand"      as khung_gio,
    count(*)          as so_chuyen
from su_kien s
join dds."DimDate" d on d."DateKey" = s.date_key
join dds."DimTime" t on t."TimeKey" = s.time_key
group by 1, 2, 3, 4, 5, 6, 7
order by 1, 2, 3, 6;


-- @query CB05 | Hành vi đi theo nhóm khách, biểu phí và dải cự ly
-- Cự ly TB chỉ trên dòng distance usable; doanh thu chỉ trên dòng amount usable.
select
    pg."PassengerGroupName"                                               as nhom_khach,
    r."RateCodeName"                                                      as bieu_phi,
    db."DistanceBandName"                                                 as dai_cu_ly,
    sum(f."TripCount")                                                    as so_chuyen,
    sum(f."PassengerCount")                                               as so_khach,
    sum(f."TotalAmount") filter (where f."IsAmountUsable")                as doanh_thu_thuan,
    round(sum(f."TotalAmount") filter (where f."IsAmountUsable")
          / nullif(count(*) filter (where f."IsAmountUsable"), 0), 2)     as doanh_thu_tb_chuyen,
    round(sum(f."TripDistanceMiles") filter (where f."IsDistanceUsable")
          / nullif(count(*) filter (where f."IsDistanceUsable"), 0), 3)   as cu_ly_tb_dam
from dds."FactTaxiTrip" f
join dds."DimPassengerGroup" pg on pg."PassengerGroupKey" = f."PassengerGroupKey"
join dds."DimRateCode"       r  on r."RateCodeKey"        = f."RateCodeKey"
join dds."DimDistanceBand"   db on db."DistanceBandKey"   = f."DistanceBandKey"
group by pg."SortOrder", 1, r."RateCodeKey", 2, db."SortOrder", 3
order by pg."SortOrder", r."RateCodeKey", db."SortOrder";


-- @query CB06 | Tổng hợp hoạt động vận tải theo khu vực (nền cho dashboard)
-- Mỗi zone một dòng (kể cả zone không có chuyến, kể cả 264/265 và key 0).
-- Tự kiểm: SUM(so_chuyen_don) = SUM(so_chuyen_tra) = COUNT(*) của Fact.
with don as (
    select f."PickupZoneKey" as zone_key,
           count(*)                                                as so_chuyen_don,
           sum(f."PassengerCount")                                 as so_khach,
           count(*) filter (where f."IsAmountUsable")              as so_chuyen_tien_hop_le,
           sum(f."TotalAmount") filter (where f."IsAmountUsable")  as doanh_thu_thuan
    from dds."FactTaxiTrip" f
    group by 1
),
tra as (
    select f."DropoffZoneKey" as zone_key, count(*) as so_chuyen_tra
    from dds."FactTaxiTrip" f
    group by 1
)
select
    z."ZoneKey"                                     as zone_key,
    z."BoroughName"                                 as quan,
    z."ZoneName"                                    as khu_vuc,
    z."ServiceZone"                                 as vung_dich_vu,
    z."HasGeometry"                                 as co_ban_do,
    coalesce(don.so_chuyen_don, 0)                  as so_chuyen_don,
    coalesce(tra.so_chuyen_tra, 0)                  as so_chuyen_tra,
    don.so_khach,
    don.doanh_thu_thuan,
    round(don.doanh_thu_thuan / nullif(don.so_chuyen_tien_hop_le, 0), 2) as doanh_thu_tb_chuyen
from dds."DimZone" z
left join don on don.zone_key = z."ZoneKey"
left join tra on tra.zone_key = z."ZoneKey"
order by so_chuyen_don desc, z."ZoneKey";


-- @query CB07 | Top 10 tuyến lộ trình phổ biến nhất (đón -> trả)
-- Loại 264 (Unknown), 265 (Outside NYC) và key 0 ở cả hai đầu. DENSE_RANK: tuyến bằng số chuyến cùng hạng.
-- Đổi số 10 ở điều kiện cuối nếu cần Top N khác.
with tuyen as (
    select
        f."PickupZoneKey"  as pu_key,
        f."DropoffZoneKey" as do_key,
        count(*)                                                                  as so_chuyen,
        sum(f."TotalAmount") filter (where f."IsAmountUsable")                    as doanh_thu_thuan,
        round(3600 * sum(f."TripDistanceMiles") filter (where f."IsTimeUsable" and f."IsDistanceUsable")
              / nullif(sum(f."TripDurationSeconds") filter (where f."IsTimeUsable" and f."IsDistanceUsable"), 0), 2)
                                                                                  as van_toc_mph
    from dds."FactTaxiTrip" f
    where f."PickupZoneKey"  not in (0, 264, 265)
      and f."DropoffZoneKey" not in (0, 264, 265)
    group by 1, 2
),
xep_hang as (
    select t.*, dense_rank() over (order by t.so_chuyen desc) as hang
    from tuyen t
)
select
    x.hang,
    x.pu_key as zone_don, x.do_key as zone_tra,
    pz."BoroughName" as quan_don, pz."ZoneName" as khu_don,
    dz."BoroughName" as quan_tra, dz."ZoneName" as khu_tra,
    x.so_chuyen, x.doanh_thu_thuan, x.van_toc_mph
from xep_hang x
join dds."DimZone" pz on pz."ZoneKey" = x.pu_key
join dds."DimZone" dz on dz."ZoneKey" = x.do_key
where x.hang <= 10
order by x.hang, x.so_chuyen desc, pz."ZoneName", dz."ZoneName";


-- @query CB08 | Tổng hợp doanh thu toàn hệ thống: GỘP và THUẦN (đủ điều kiện)
-- Gộp = mọi dòng; Thuần = IsAmountUsable. Theo năm đón khách + dòng TONG.
-- Năm 1900 = DateKey 0 (ngoài miền ngày / thiếu ngày): vẫn tính, không bỏ.
select
    case when grouping(d."YearNumber") = 1 then 'TONG' else d."YearNumber"::text end   as nam,
    sum(f."TripCount")                                                                  as so_chuyen,
    sum(f."TotalAmount")                                                                as doanh_thu_gop,
    sum(f."TotalAmount") filter (where f."IsAmountUsable")                              as doanh_thu_thuan,
    sum(f."TotalAmount") - coalesce(sum(f."TotalAmount") filter (where f."IsAmountUsable"), 0)
                                                                                        as chenh_lech,
    round(100.0 * sum(f."TotalAmount") filter (where f."IsAmountUsable")
          / nullif(sum(f."TotalAmount"), 0), 4)                                         as thuan_tren_gop_pct,
    count(*) filter (where not f."IsAmountUsable")                                      as so_chuyen_bi_loai,
    round(100.0 * count(*) filter (where not f."IsAmountUsable") / count(*), 4)         as ty_le_chuyen_bi_loai_pct
from dds."FactTaxiTrip" f
join dds."DimDate" d on d."DateKey" = f."PickupDateKey"
group by rollup (d."YearNumber")
order by grouping(d."YearNumber"), d."YearNumber";


-- @query MR01 | Tương quan biểu phí với tỷ lệ tiền boa và vận tốc
-- Tỷ lệ tip: chỉ chuyến trả thẻ, fare > 0, amount usable. Vận tốc có trọng số thời gian.
-- Cột van_toc_avg_don_gian_KHONG_DUNG chỉ để minh họa vì sao không dùng AVG: KHÔNG dùng cho báo cáo.
select
    r."RateCodeKey"                                                       as rate_code_key,
    r."RateCodeName"                                                      as bieu_phi,
    r."RateGroup"                                                         as nhom_bieu_phi,
    sum(f."TripCount")                                                    as so_chuyen,
    count(*) filter (where p."IsElectronicTipObservable" and f."FareAmount" > 0 and f."IsAmountUsable")
                                                                          as so_chuyen_tinh_tip,
    round(100.0 * sum(f."TipAmount")  filter (where p."IsElectronicTipObservable" and f."FareAmount" > 0 and f."IsAmountUsable")
          / nullif(sum(f."FareAmount") filter (where p."IsElectronicTipObservable" and f."FareAmount" > 0 and f."IsAmountUsable"), 0), 2)
                                                                          as ty_le_tip_pct,
    count(*) filter (where f."IsTimeUsable" and f."IsDistanceUsable")     as so_chuyen_tinh_van_toc,
    round(3600 * sum(f."TripDistanceMiles") filter (where f."IsTimeUsable" and f."IsDistanceUsable")
          / nullif(sum(f."TripDurationSeconds") filter (where f."IsTimeUsable" and f."IsDistanceUsable"), 0), 2)
                                                                          as van_toc_mph,
    round(avg(3600 * f."TripDistanceMiles" / f."TripDurationSeconds")
          filter (where f."IsTimeUsable" and f."IsDistanceUsable"), 2)    as van_toc_avg_don_gian_KHONG_DUNG
from dds."FactTaxiTrip" f
join dds."DimRateCode"    r on r."RateCodeKey"    = f."RateCodeKey"
join dds."DimPaymentType" p on p."PaymentTypeKey" = f."PaymentTypeKey"
group by 1, 2, 3
order by 1;


-- @query MR02 | Biến động doanh thu Tháng 5 và Tháng 6 (từng năm 2021–2023)
-- Lịch lấy từ DimDate nên tháng chưa có dữ liệu vẫn hiện (co_du_lieu = false).
-- Cơ cấu phụ phí = tỷ trọng từng thành phần trên doanh thu thuần. so_sanh_*: tháng 6 so với tháng 5 cùng năm.
with lich as (
    select "YearNumber" as nam, "MonthNumber" as thang,
           count(*)                            as so_ngay,
           count(*) filter (where "IsWeekend") as so_ngay_cuoi_tuan
    from dds."DimDate"
    -- DimDate đã mở rộng đến 2030 (RFC-002 v2): giới hạn đúng phạm vi dữ liệu 2021–2023 như MR04
    where "DateKey" <> 0 and "MonthNumber" in (5, 6) and "YearNumber" between 2021 and 2023
    group by 1, 2
),
thang as (
    select d."YearNumber" as nam, d."MonthNumber" as thang,
           count(*)                                                                  as so_chuyen,
           sum(f."TotalAmount")           filter (where f."IsAmountUsable")          as doanh_thu_thuan,
           sum(f."FareAmount")            filter (where f."IsAmountUsable")          as fare,
           sum(f."Extra")                 filter (where f."IsAmountUsable")          as extra,
           sum(f."MtaTax")                filter (where f."IsAmountUsable")          as mta_tax,
           sum(f."ImprovementSurcharge")  filter (where f."IsAmountUsable")          as improvement,
           sum(f."CongestionSurcharge")   filter (where f."IsAmountUsable")          as congestion,
           sum(f."AirportFee")            filter (where f."IsAmountUsable")          as airport,
           sum(f."TollsAmount")           filter (where f."IsAmountUsable")          as tolls,
           sum(f."TipAmount")             filter (where f."IsAmountUsable")          as tip
    from dds."FactTaxiTrip" f
    join dds."DimDate" d on d."DateKey" = f."PickupDateKey"
    where d."MonthNumber" in (5, 6)
    group by 1, 2
)
select
    l.nam, l.thang, l.so_ngay, l.so_ngay_cuoi_tuan,
    (t.so_chuyen is not null)                                         as co_du_lieu,
    coalesce(t.so_chuyen, 0)                                          as so_chuyen,
    t.doanh_thu_thuan,
    round(t.doanh_thu_thuan / l.so_ngay, 2)                           as doanh_thu_tb_ngay,
    round(100.0 * t.fare        / nullif(t.doanh_thu_thuan, 0), 2)    as ty_trong_fare_pct,
    round(100.0 * t.extra       / nullif(t.doanh_thu_thuan, 0), 2)    as ty_trong_extra_pct,
    round(100.0 * t.mta_tax     / nullif(t.doanh_thu_thuan, 0), 2)    as ty_trong_mta_pct,
    round(100.0 * t.improvement / nullif(t.doanh_thu_thuan, 0), 2)    as ty_trong_improvement_pct,
    round(100.0 * t.congestion  / nullif(t.doanh_thu_thuan, 0), 2)    as ty_trong_congestion_pct,
    round(100.0 * t.airport     / nullif(t.doanh_thu_thuan, 0), 2)    as ty_trong_airport_pct,
    round(100.0 * t.tolls       / nullif(t.doanh_thu_thuan, 0), 2)    as ty_trong_tolls_pct,
    round(100.0 * t.tip         / nullif(t.doanh_thu_thuan, 0), 2)    as ty_trong_tip_pct,
    round(100.0 * (t.doanh_thu_thuan - lag(t.doanh_thu_thuan) over w)
          / nullif(lag(t.doanh_thu_thuan) over w, 0), 2)              as so_sanh_doanh_thu_pct,
    round(100.0 * (t.so_chuyen - lag(t.so_chuyen) over w)
          / nullif(lag(t.so_chuyen) over w, 0), 2)                    as so_sanh_so_chuyen_pct
from lich l
left join thang t on t.nam = l.nam and t.thang = l.thang
window w as (partition by l.nam order by l.thang)
order by l.nam, l.thang;


-- @query MR03 | Phân tích tăng doanh thu đơn vị (doanh thu / chuyến) theo tháng
-- Tách doanh thu TB mỗi chuyến thành từng thành phần (NULL phụ phí tính 0 trong trung bình),
-- kèm cự ly TB, thời lượng TB và cước theo dặm để giải thích vì sao giá mỗi chuyến tăng.
select
    d."YearMonth"                                                                         as nam_thang,
    count(*) filter (where f."IsAmountUsable")                                            as so_chuyen_tien_hop_le,
    round(avg(f."TotalAmount")                         filter (where f."IsAmountUsable"), 2) as doanh_thu_tb_chuyen,
    round(avg(f."FareAmount")                          filter (where f."IsAmountUsable"), 2) as fare_tb,
    round(avg(coalesce(f."Extra", 0))                  filter (where f."IsAmountUsable"), 2) as extra_tb,
    round(avg(coalesce(f."MtaTax", 0) + coalesce(f."ImprovementSurcharge", 0))
                                                       filter (where f."IsAmountUsable"), 2) as thue_mta_improvement_tb,
    round(avg(coalesce(f."CongestionSurcharge", 0))    filter (where f."IsAmountUsable"), 2) as congestion_tb,
    round(avg(coalesce(f."AirportFee", 0))             filter (where f."IsAmountUsable"), 2) as airport_tb,
    round(avg(coalesce(f."TollsAmount", 0))            filter (where f."IsAmountUsable"), 2) as tolls_tb,
    round(avg(f."TipAmount")                           filter (where f."IsAmountUsable"), 2) as tip_tb,
    round(avg(f."TripDistanceMiles")   filter (where f."IsTimeUsable" and f."IsDistanceUsable"), 3) as cu_ly_tb_dam,
    round(avg(f."TripDurationSeconds") filter (where f."IsTimeUsable" and f."IsDistanceUsable") / 60, 2)
                                                                                          as thoi_luong_tb_phut,
    round(sum(f."FareAmount") filter (where f."IsAmountUsable" and f."IsDistanceUsable")
          / nullif(sum(f."TripDistanceMiles") filter (where f."IsAmountUsable" and f."IsDistanceUsable"), 0), 3)
                                                                                          as fare_tren_dam
from dds."FactTaxiTrip" f
join dds."DimDate" d on d."DateKey" = f."PickupDateKey"
group by 1
order by 1;


-- @query MR04 | Tăng trưởng cùng kỳ: MoM và YoY cho đủ 36 tháng 2021–2023
-- Lịch 36 tháng lấy từ DimDate (LEFT JOIN) nên LAG(1) luôn là tháng trước, LAG(12) luôn là cùng kỳ năm trước,
-- kể cả khi có tháng chưa nạp dữ liệu (co_du_lieu = false -> các tỷ lệ = NULL thay vì so sai tháng).
with lich as (
    select distinct "YearNumber" as nam, "MonthNumber" as thang, "YearMonth" as nam_thang
    from dds."DimDate"
    where "DateKey" <> 0 and "YearNumber" between 2021 and 2023
),
thang as (
    select d."YearNumber" as nam, d."MonthNumber" as thang,
           count(*)                                               as so_chuyen,
           sum(f."TotalAmount") filter (where f."IsAmountUsable") as doanh_thu_thuan
    from dds."FactTaxiTrip" f
    join dds."DimDate" d on d."DateKey" = f."PickupDateKey"
    group by 1, 2
),
chuoi as (
    select l.nam, l.thang, l.nam_thang,
           (t.so_chuyen is not null) as co_du_lieu,
           t.so_chuyen, t.doanh_thu_thuan
    from lich l
    left join thang t on t.nam = l.nam and t.thang = l.thang
)
select
    nam_thang, co_du_lieu, so_chuyen, doanh_thu_thuan,
    lag(so_chuyen, 1)  over w                                                     as so_chuyen_thang_truoc,
    round(100.0 * (so_chuyen - lag(so_chuyen, 1) over w) / nullif(lag(so_chuyen, 1) over w, 0), 2)
                                                                                  as mom_so_chuyen_pct,
    round(100.0 * (doanh_thu_thuan - lag(doanh_thu_thuan, 1) over w) / nullif(lag(doanh_thu_thuan, 1) over w, 0), 2)
                                                                                  as mom_doanh_thu_pct,
    lag(so_chuyen, 12) over w                                                     as so_chuyen_cung_ky,
    round(100.0 * (so_chuyen - lag(so_chuyen, 12) over w) / nullif(lag(so_chuyen, 12) over w, 0), 2)
                                                                                  as yoy_so_chuyen_pct,
    round(100.0 * (doanh_thu_thuan - lag(doanh_thu_thuan, 12) over w) / nullif(lag(doanh_thu_thuan, 12) over w, 0), 2)
                                                                                  as yoy_doanh_thu_pct
from chuoi
window w as (order by nam, thang)
order by nam, thang;


-- @query MR05 | MR5a - Bản đồ GIS: lượng chuyến ĐÓN và TRẢ khách theo khu vực và khung giờ
-- Đề: "hiển thị lượng chuyến đón/trả khách theo từng khu vực trên bản đồ không gian".
-- Role-playing: đón theo PickupZoneKey + PickupTimeKey, trả theo DropoffZoneKey + DropoffTimeKey (hai luồng độc lập).
-- Chỉ zone có đa giác (HasGeometry). Diện tích tính trên geography (m2 -> km2).
-- Tâm khu (kinh/vĩ độ) để vẽ điểm; đa giác GeoJSON xuất riêng ra file MR05_zones.geojson (xem b03_export.py).
-- chenh_lech_tra_don > 0: khu vực "hút" khách (nhiều điểm đến hơn điểm đi) trong khung giờ đó.
with don as (
    select f."PickupZoneKey" as zone_key, t."TimeBand" as khung_gio, count(*) as so_chuyen_don
    from dds."FactTaxiTrip" f
    join dds."DimTime" t on t."TimeKey" = f."PickupTimeKey"
    group by 1, 2
),
tra as (
    select f."DropoffZoneKey" as zone_key, t."TimeBand" as khung_gio, count(*) as so_chuyen_tra
    from dds."FactTaxiTrip" f
    join dds."DimTime" t on t."TimeKey" = f."DropoffTimeKey"
    group by 1, 2
),
gop as (
    select coalesce(don.zone_key, tra.zone_key)       as zone_key,
           coalesce(don.khung_gio, tra.khung_gio)     as khung_gio,
           coalesce(don.so_chuyen_don, 0)             as so_chuyen_don,
           coalesce(tra.so_chuyen_tra, 0)             as so_chuyen_tra
    from don
    full join tra on tra.zone_key = don.zone_key and tra.khung_gio = don.khung_gio
)
select
    z."ZoneKey"                                                            as zone_key,
    z."BoroughName"                                                        as quan,
    z."ZoneName"                                                           as khu_vuc,
    gop.khung_gio,
    gop.so_chuyen_don,
    gop.so_chuyen_tra,
    gop.so_chuyen_tra - gop.so_chuyen_don                                  as chenh_lech_tra_don,
    round((st_area(z."ZoneGeometry"::geography) / 1e6)::numeric, 3)        as dien_tich_km2,
    round(gop.so_chuyen_don / nullif((st_area(z."ZoneGeometry"::geography) / 1e6)::numeric, 0), 1)
                                                                           as mat_do_don_km2,
    round(gop.so_chuyen_tra / nullif((st_area(z."ZoneGeometry"::geography) / 1e6)::numeric, 0), 1)
                                                                           as mat_do_tra_km2,
    round(st_x(st_pointonsurface(z."ZoneGeometry"))::numeric, 6)           as kinh_do,
    round(st_y(st_pointonsurface(z."ZoneGeometry"))::numeric, 6)           as vi_do
from gop
join dds."DimZone" z on z."ZoneKey" = gop.zone_key
where z."HasGeometry"
order by gop.khung_gio, mat_do_don_km2 desc;


-- @query MR05B | MR5b - Bảng doanh thu theo từng Quận qua các tháng trong năm
-- Đề: "lập bảng hiển thị doanh thu theo từng quận qua các tháng trong năm".
-- Quận theo điểm ĐÓN; doanh thu thuần = IsAmountUsable. Dạng dài (quận × năm × tháng) để Power BI pivot thành ma trận.
-- ty_trong_quan_trong_thang: tỷ trọng doanh thu của quận trong tổng doanh thu tháng đó.
-- Tự kiểm: SUM(doanh_thu_thuan) = doanh_thu_thuan dòng TONG của CB08.
select
    d."YearNumber"                                                        as nam,
    d."MonthNumber"                                                       as thang,
    z."BoroughName"                                                       as quan,
    sum(f."TripCount")                                                    as so_chuyen,
    sum(f."TotalAmount") filter (where f."IsAmountUsable")                as doanh_thu_thuan,
    round(sum(f."TotalAmount") filter (where f."IsAmountUsable")
          / nullif(count(*) filter (where f."IsAmountUsable"), 0), 2)     as doanh_thu_tb_chuyen,
    round(100.0 * sum(f."TotalAmount") filter (where f."IsAmountUsable")
          / nullif(sum(sum(f."TotalAmount") filter (where f."IsAmountUsable"))
                   over (partition by d."YearNumber", d."MonthNumber"), 0), 2)
                                                                          as ty_trong_quan_trong_thang
from dds."FactTaxiTrip" f
join dds."DimDate" d on d."DateKey" = f."PickupDateKey"
join dds."DimZone" z on z."ZoneKey" = f."PickupZoneKey"
group by 1, 2, 3
order by 1, 2, 3;


-- @query MR05B_GIO | MR5b - Xu hướng chuyến và doanh thu theo khung giờ trong ngày, từng quận
-- Đề: "vẽ đồ thị phân tích xu hướng theo các khung giờ trong ngày".
-- Giờ 0–23 theo giờ ĐÓN (DimTime.HourNumber; -1 = key 0), kèm TimeBand 4 khung để vẽ gộp. Tách theo năm để so xu hướng.
-- Tự kiểm: SUM(doanh_thu_thuan) = doanh_thu_thuan dòng TONG của CB08.
select
    d."YearNumber"                                                        as nam,
    z."BoroughName"                                                       as quan,
    t."HourNumber"                                                        as gio,
    t."TimeBand"                                                          as khung_gio,
    sum(f."TripCount")                                                    as so_chuyen,
    sum(f."TotalAmount") filter (where f."IsAmountUsable")                as doanh_thu_thuan,
    round(sum(f."TotalAmount") filter (where f."IsAmountUsable")
          / nullif(count(*) filter (where f."IsAmountUsable"), 0), 2)     as doanh_thu_tb_chuyen
from dds."FactTaxiTrip" f
join dds."DimDate" d on d."DateKey" = f."PickupDateKey"
join dds."DimTime" t on t."TimeKey" = f."PickupTimeKey"
join dds."DimZone" z on z."ZoneKey" = f."PickupZoneKey"
group by 1, 2, 3, 4
order by 1, 2, 3;

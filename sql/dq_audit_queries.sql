-- =============================================================================
-- B03 (nbinh) - AUDIT CHẤT LƯỢNG DỮ LIỆU & KIỂM TOÁN DOANH THU (đầu vào dq_audit_3y.xlsx và B04)
-- Nguồn: dds."FactTaxiTrip" (+ staging.stg_trips_flagged cho đối soát DQ06).
-- Gom theo THÁNG CỦA FILE NGUỒN (lấy từ SourceFileName) vì audit là theo lô nạp;
-- dòng có ngày đón lệch tháng/ngoài miền vẫn thuộc file của nó.
-- Không ghi cứng năm/tháng: sau S03 (36 file) chạy lại là ra số 3 năm.
-- Định dạng: "-- @query <MÃ> | <tiêu đề>" như sql/acceptance_queries.sql.
-- =============================================================================


-- @query DQ01 | Tỷ lệ cờ chất lượng theo tháng của file nguồn
select
    substring(f."SourceFileName" from '(\d{4}-\d{2})')                       as thang_file,
    count(*)                                                                  as so_dong,
    count(*) filter (where f."HasDQIssue")                                    as co_loi_usable,
    round(100.0 * count(*) filter (where f."HasDQIssue") / count(*), 4)       as co_loi_usable_pct,
    count(*) filter (where not f."IsTimeUsable")                              as time_khong_dung_duoc,
    round(100.0 * count(*) filter (where not f."IsTimeUsable") / count(*), 4) as time_khong_dung_duoc_pct,
    count(*) filter (where not f."IsDistanceUsable")                          as cu_ly_khong_dung_duoc,
    round(100.0 * count(*) filter (where not f."IsDistanceUsable") / count(*), 4) as cu_ly_khong_dung_duoc_pct,
    count(*) filter (where not f."IsAmountUsable")                            as tien_khong_dung_duoc,
    round(100.0 * count(*) filter (where not f."IsAmountUsable") / count(*), 4) as tien_khong_dung_duoc_pct,
    count(*) filter (where f."AttributeQualityFlags" is not null)             as co_canh_bao_thuoc_tinh,
    max(f."QualityRuleVersion")                                               as rule_version
from dds."FactTaxiTrip" f
group by rollup (substring(f."SourceFileName" from '(\d{4}-\d{2})'))
order by 1 nulls last;


-- @query DQ02 | Tần suất từng mã lỗi / cảnh báo theo năm của file nguồn
-- Một dòng có thể mang nhiều mã nên tổng các mã > số dòng có lỗi.
with ma as (
    select substring(f."SourceFileName" from '(\d{4})-\d{2}') as nam_file,
           'DATA'::text as loai, unnest(string_to_array(f."DataQualityFlags", ';')) as ma_loi
    from dds."FactTaxiTrip" f
    where f."DataQualityFlags" is not null
    union all
    select substring(f."SourceFileName" from '(\d{4})-\d{2}'),
           'ATTRIBUTE', unnest(string_to_array(f."AttributeQualityFlags", ';'))
    from dds."FactTaxiTrip" f
    where f."AttributeQualityFlags" is not null
),
tong as (
    select substring(f."SourceFileName" from '(\d{4})-\d{2}') as nam_file, count(*) as so_dong
    from dds."FactTaxiTrip" f
    group by 1
)
select ma.nam_file, ma.loai, ma.ma_loi,
       count(*)                                       as so_dong_co_ma,
       round(100.0 * count(*) / max(tong.so_dong), 4) as ty_le_pct
from ma
join tong on tong.nam_file = ma.nam_file
group by 1, 2, 3
order by 1, 2, so_dong_co_ma desc;


-- @query DQ03 | Khối khuyết đồng loạt so với baseline khảo sát (4.156.354 dòng = 3,8177%)
-- Định nghĩa khối (Kế hoạch 2.4): passenger, RatecodeID, store_and_fwd_flag, congestion_surcharge đều NULL
-- VÀ payment_type = 0. Dùng mã MISSING_* của B02 để biết là NULL NGUỒN (không phải mã chưa ánh xạ).
-- Tiêu chí nghiệm thu B03: tỷ lệ ~3,81% khi đã nạp đủ 36 file (S03). Với ít file, chỉ mang tính tham khảo.
with khoi as (
    select
        substring(f."SourceFileName" from '(\d{4})-\d{2}') as nam_file,
        (f."AttributeQualityFlags" ~ '(^|;)MISSING_PASSENGER_COUNT(;|$)'
         and f."AttributeQualityFlags" ~ '(^|;)MISSING_RATE_CODE(;|$)'
         and f."AttributeQualityFlags" ~ '(^|;)MISSING_STORE_AND_FWD_FLAG(;|$)'
         and f."CongestionSurcharge" is null
         and f."PaymentTypeKey" = 0)                       as thuoc_khoi_khuyet,
        f."PassengerCount" is null                         as thieu_passenger,
        f."TotalAmount", f."IsAmountUsable"
    from dds."FactTaxiTrip" f
)
select
    coalesce(nam_file, 'TONG')                                                    as nam_file,
    count(*)                                                                      as so_dong,
    count(*) filter (where thuoc_khoi_khuyet)                                     as dong_khuyet_dong_loat,
    round(100.0 * count(*) filter (where thuoc_khoi_khuyet) / count(*), 4)        as ty_le_khuyet_pct,
    count(*) filter (where thieu_passenger)                                       as dong_thieu_passenger,
    sum("TotalAmount") filter (where thuoc_khoi_khuyet and "IsAmountUsable")      as doanh_thu_thuan_cua_khoi_khuyet,
    4156354                                                                       as baseline_dong_3_nam,
    3.8177                                                                        as baseline_pct,
    round(100.0 * count(*) filter (where thuoc_khoi_khuyet) / count(*) - 3.8177, 4) as lech_baseline_diem_pct
from khoi
group by rollup (nam_file)
order by grouping(nam_file), nam_file;


-- @query DQ04 | Kiểm toán doanh thu gộp và thuần theo năm của file, tách phần bị loại theo mã lỗi tiền
-- Các cột loai_* có thể chồng lấn (một dòng nhiều mã); chenh_lech = gộp - thuần là con số chính xác.
-- lech_cau_thanh: số dòng mà tổng 8 thành phần khác TotalAmount quá 1 cent (chỉ để theo dõi, không phải lỗi usable).
with d as (
    select
        substring(f."SourceFileName" from '(\d{4})-\d{2}') as nam_file,
        f.*,
        coalesce(f."FareAmount",0) + coalesce(f."Extra",0) + coalesce(f."MtaTax",0) + coalesce(f."TipAmount",0)
          + coalesce(f."TollsAmount",0) + coalesce(f."ImprovementSurcharge",0)
          + coalesce(f."CongestionSurcharge",0) + coalesce(f."AirportFee",0)          as tong_thanh_phan
    from dds."FactTaxiTrip" f
)
select
    coalesce(nam_file, 'TONG')                                                       as nam_file,
    count(*)                                                                         as so_chuyen,
    sum("TotalAmount")                                                               as doanh_thu_gop,
    sum("TotalAmount") filter (where "IsAmountUsable")                               as doanh_thu_thuan,
    sum("TotalAmount") - coalesce(sum("TotalAmount") filter (where "IsAmountUsable"), 0) as chenh_lech,
    sum("TotalAmount") filter (where "DataQualityFlags" ~ '(^|;)NEGATIVE_TOTAL_AMOUNT(;|$)') as loai_tong_am,
    sum("TotalAmount") filter (where "DataQualityFlags" ~ '(^|;)AMOUNT_OVER_5000(;|$)')      as loai_tren_5000,
    sum("TotalAmount") filter (where "DataQualityFlags" ~ '(^|;)INCONSISTENT_TOTAL(;|$)')    as loai_lech_cau_thanh,
    sum("TotalAmount") filter (where "DataQualityFlags" ~ '(^|;)(NEGATIVE_FARE|NEGATIVE_TIP)(;|$)') as loai_fare_tip_am,
    sum("FareAmount")  filter (where "IsAmountUsable")                               as fare_thuan,
    sum("TipAmount")   filter (where "IsAmountUsable")                               as tip_thuan,
    sum(coalesce("Extra",0) + coalesce("MtaTax",0) + coalesce("ImprovementSurcharge",0)
        + coalesce("CongestionSurcharge",0) + coalesce("AirportFee",0))
                       filter (where "IsAmountUsable")                               as phu_phi_thue_thuan,
    sum("TollsAmount") filter (where "IsAmountUsable")                               as tolls_thuan,
    count(*) filter (where abs("TotalAmount" - tong_thanh_phan) > 0.01)              as dong_lech_cau_thanh,
    sum("TotalAmount" - tong_thanh_phan)                                             as tong_lech_cau_thanh
from d
group by rollup (nam_file)
order by grouping(nam_file), nam_file;


-- @query DQ05 | Các ca cực trị (top 10 mỗi nhóm) kèm lineage để truy về file gốc
(select 'TOTAL_LON_NHAT' as nhom, f."TripKey", f."SourceFileName", f."SourceRowNumber",
        f."TotalAmount", f."FareAmount", f."TipAmount", f."TripDistanceMiles", f."TripDurationSeconds",
        f."DataQualityFlags"
 from dds."FactTaxiTrip" f order by f."TotalAmount" desc nulls last limit 10)
union all
(select 'FARE_AM_NHAT', f."TripKey", f."SourceFileName", f."SourceRowNumber",
        f."TotalAmount", f."FareAmount", f."TipAmount", f."TripDistanceMiles", f."TripDurationSeconds",
        f."DataQualityFlags"
 from dds."FactTaxiTrip" f order by f."FareAmount" asc nulls last limit 10)
union all
(select 'TIP_LON_NHAT', f."TripKey", f."SourceFileName", f."SourceRowNumber",
        f."TotalAmount", f."FareAmount", f."TipAmount", f."TripDistanceMiles", f."TripDurationSeconds",
        f."DataQualityFlags"
 from dds."FactTaxiTrip" f order by f."TipAmount" desc nulls last limit 10)
union all
(select 'CU_LY_LON_NHAT', f."TripKey", f."SourceFileName", f."SourceRowNumber",
        f."TotalAmount", f."FareAmount", f."TipAmount", f."TripDistanceMiles", f."TripDurationSeconds",
        f."DataQualityFlags"
 from dds."FactTaxiTrip" f order by f."TripDistanceMiles" desc nulls last limit 10)
union all
(select 'THOI_LUONG_LON_NHAT', f."TripKey", f."SourceFileName", f."SourceRowNumber",
        f."TotalAmount", f."FareAmount", f."TipAmount", f."TripDistanceMiles", f."TripDurationSeconds",
        f."DataQualityFlags"
 from dds."FactTaxiTrip" f order by f."TripDurationSeconds" desc nulls last limit 10);


-- @query DQ06 | Đối soát số dòng theo file: Footer Parquet = Fact + Quarantine; tiền staging = Fact (file còn trong staging)
-- Staging là vùng đệm TẠM (S03 / RFC-002 v2: TRUNCATE mỗi file, chỉ giữ file cuối), nên:
--   * Số dòng đối soát với footer Parquet ghi ở meta.file_manifest (S01) cho ĐỦ 36 file:
--       lech_so_dong = footer - (Fact + Quarantine), kỳ vọng 0 mọi file.
--   * Tổng tiền staging = Fact chỉ đối soát được cho file còn nằm trong staging (co_trong_staging = true),
--       kỳ vọng lech_tong_tien = 0; file khác để NULL (không phải lỗi).
with mf as (
    select file_name as source_file_name, total_rows as so_dong_footer
    from meta.file_manifest
    where file_name like 'yellow_tripdata_%'
),
fct as (
    select "SourceFileName" as source_file_name, count(*) as so_dong, sum("TotalAmount") as tong_tien
    from dds."FactTaxiTrip"
    group by 1
),
q as (
    select source_file_name, count(*) as so_dong
    from staging.quarantine
    group by 1
),
stg as (
    select source_file_name, count(*) as so_dong, sum(total_amount) as tong_tien
    from staging.stg_trips_flagged
    group by 1
)
select
    coalesce(mf.source_file_name, f.source_file_name)                            as file_nguon,
    mf.so_dong_footer,
    f.so_dong                                                                    as so_dong_fact,
    coalesce(q.so_dong, 0)                                                       as so_dong_quarantine,
    coalesce(mf.so_dong_footer, 0) - coalesce(f.so_dong, 0) - coalesce(q.so_dong, 0)
                                                                                 as lech_so_dong,
    s.source_file_name is not null                                               as co_trong_staging,
    s.tong_tien                                                                  as tong_tien_staging,
    f.tong_tien                                                                  as tong_tien_fact,
    case when s.source_file_name is not null
         then coalesce(s.tong_tien, 0) - coalesce(f.tong_tien, 0) end            as lech_tong_tien
from mf
full outer join fct f on f.source_file_name = mf.source_file_name
left join q   on q.source_file_name = coalesce(mf.source_file_name, f.source_file_name)
left join stg s on s.source_file_name = coalesce(mf.source_file_name, f.source_file_name)
order by 1;

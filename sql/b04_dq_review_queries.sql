-- =============================================================================
-- B04 (nbinh) - RÀ SOÁT CA CỰC TRỊ & KIỂM CHỨNG BỘ CỜ CHẤT LƯỢNG trên dds."FactTaxiTrip" (108.870.632 dòng)
-- Kế hoạch GĐ2 - B04: "Rà soát các ca cực trị (fare âm, tip thẻ)" + "Báo cáo mô tả chi tiết, minh bạch các quy tắc gắn cờ".
-- Chỉ ĐỌC dữ liệu. Định dạng "-- @query <MÃ> | <tiêu đề>" để scripts/b04_dq_review.py tách và chạy từng câu.
-- Năm = năm của FILE nguồn (SourceFileName), không phải năm đón khách, để khớp với bảng đối soát reconciliation.csv.
-- =============================================================================


-- @query X01 | Fare âm theo năm và hình thức thanh toán: quy mô, số tiền và mức độ bị cờ loại
-- Kỳ vọng: 100% chuyến FareAmount < 0 có IsAmountUsable = false (so_chuyen_van_usable = 0).
select
    substring(f."SourceFileName" from '(\d{4})-\d{2}')                     as nam_file,
    p."PaymentTypeName"                                                    as hinh_thuc_tt,
    count(*)                                                               as so_chuyen_fare_am,
    sum(f."FareAmount")                                                    as tong_fare_am,
    sum(f."TotalAmount")                                                   as tong_total,
    min(f."FareAmount")                                                    as fare_nho_nhat,
    count(*) filter (where f."TotalAmount" < 0)                            as so_chuyen_total_cung_am,
    count(*) filter (where f."IsAmountUsable")                             as so_chuyen_van_usable
from dds."FactTaxiTrip" f
join dds."DimPaymentType" p on p."PaymentTypeKey" = f."PaymentTypeKey"
where f."FareAmount" < 0
group by rollup (1, 2)
order by 1 nulls last, 3 desc;


-- @query X02 | Tip trên chuyến trả thẻ: tip âm, tip > fare, tip > 100 USD, tip lớn nhất (theo năm)
-- Chỉ thẻ tín dụng ghi nhận được tip điện tử (DimPaymentType.IsElectronicTipObservable).
-- tip_gt_fare: tip > fare với fare > 0 - không phải lỗi (khách boa hào phóng / fare nhỏ), chỉ để theo dõi.
select
    substring(f."SourceFileName" from '(\d{4})-\d{2}')                     as nam_file,
    count(*)                                                               as so_chuyen_the,
    count(*) filter (where f."TipAmount" < 0)                              as tip_am,
    count(*) filter (where f."TipAmount" < 0 and f."IsAmountUsable")       as tip_am_van_usable,
    count(*) filter (where f."FareAmount" > 0 and f."TipAmount" > f."FareAmount")
                                                                           as tip_lon_hon_fare,
    count(*) filter (where f."TipAmount" > 100)                            as tip_tren_100usd,
    count(*) filter (where f."TipAmount" > 100 and f."IsAmountUsable")     as tip_tren_100usd_usable,
    max(f."TipAmount")                                                     as tip_lon_nhat,
    max(f."TipAmount") filter (where f."IsAmountUsable")                   as tip_lon_nhat_usable,
    round(100.0 * sum(f."TipAmount") filter (where f."IsAmountUsable" and f."FareAmount" > 0)
          / nullif(sum(f."FareAmount") filter (where f."IsAmountUsable" and f."FareAmount" > 0), 0), 2)
                                                                           as ty_le_tip_pct_usable
from dds."FactTaxiTrip" f
join dds."DimPaymentType" p on p."PaymentTypeKey" = f."PaymentTypeKey"
where p."IsElectronicTipObservable"
group by rollup (1)
order by 1 nulls last;


-- @query X03 | Tip ghi nhận trên chuyến KHÔNG trả thẻ (tiền mặt, no charge, dispute...)
-- Tiền mặt không ghi nhận tip điện tử: tip > 0 ở đây là bất thường nguồn cần nêu trong báo cáo.
select
    p."PaymentTypeName"                                                    as hinh_thuc_tt,
    count(*)                                                               as so_chuyen,
    count(*) filter (where f."TipAmount" > 0)                              as so_chuyen_co_tip,
    round(100.0 * count(*) filter (where f."TipAmount" > 0) / count(*), 4) as ty_le_co_tip_pct,
    sum(f."TipAmount") filter (where f."TipAmount" > 0)                    as tong_tip,
    max(f."TipAmount")                                                     as tip_lon_nhat
from dds."FactTaxiTrip" f
join dds."DimPaymentType" p on p."PaymentTypeKey" = f."PaymentTypeKey"
where not p."IsElectronicTipObservable"
group by 1
order by 2 desc;


-- @query X04 | Danh sách ca cực trị tiền kèm lineage và cờ (top 10 mỗi nhóm)
-- Truy được về file + số dòng gốc trong Parquet (SourceFileName, SourceRowNumber).
(select 'TOTAL_LON_NHAT' as nhom, f."SourceFileName", f."SourceRowNumber", p."PaymentTypeName" as hinh_thuc_tt,
        f."FareAmount", f."TipAmount", f."TollsAmount", f."TotalAmount", f."IsAmountUsable", f."DataQualityFlags"
 from dds."FactTaxiTrip" f join dds."DimPaymentType" p using ("PaymentTypeKey")
 order by f."TotalAmount" desc nulls last limit 10)
union all
(select 'TOTAL_AM_NHAT', f."SourceFileName", f."SourceRowNumber", p."PaymentTypeName",
        f."FareAmount", f."TipAmount", f."TollsAmount", f."TotalAmount", f."IsAmountUsable", f."DataQualityFlags"
 from dds."FactTaxiTrip" f join dds."DimPaymentType" p using ("PaymentTypeKey")
 order by f."TotalAmount" asc nulls last limit 10)
union all
(select 'FARE_AM_NHAT', f."SourceFileName", f."SourceRowNumber", p."PaymentTypeName",
        f."FareAmount", f."TipAmount", f."TollsAmount", f."TotalAmount", f."IsAmountUsable", f."DataQualityFlags"
 from dds."FactTaxiTrip" f join dds."DimPaymentType" p using ("PaymentTypeKey")
 order by f."FareAmount" asc nulls last limit 10)
union all
(select 'TIP_THE_LON_NHAT', f."SourceFileName", f."SourceRowNumber", p."PaymentTypeName",
        f."FareAmount", f."TipAmount", f."TollsAmount", f."TotalAmount", f."IsAmountUsable", f."DataQualityFlags"
 from dds."FactTaxiTrip" f join dds."DimPaymentType" p using ("PaymentTypeKey")
 where p."IsElectronicTipObservable"
 order by f."TipAmount" desc nulls last limit 10)
union all
(select 'TIP_AM_NHAT', f."SourceFileName", f."SourceRowNumber", p."PaymentTypeName",
        f."FareAmount", f."TipAmount", f."TollsAmount", f."TotalAmount", f."IsAmountUsable", f."DataQualityFlags"
 from dds."FactTaxiTrip" f join dds."DimPaymentType" p using ("PaymentTypeKey")
 order by f."TipAmount" asc nulls last limit 10);


-- @query X05 | Kiểm chứng bộ cờ: tính lại từng điều kiện của Contract v1.2 trực tiếp từ measure và so với cờ đã lưu
-- Mỗi dòng = một điều kiện; so_dong_vi_pham_ma_van_usable phải = 0 (không có ca lọt cờ).
-- so_dong_usable_ma_vi_pham phải = 0 với cờ usable (nếu dòng được đánh usable thì không vi phạm điều kiện nào).
with d as (
    select f.*,
        (f."PickupDateTime" is null or f."DropoffDateTime" is null
         or f."DropoffDateTime" <= f."PickupDateTime" or f."TripDurationSeconds" > 86400
         or f."PickupDateTime"::date  not between date '2020-12-31' and date '2024-01-01'
         or f."DropoffDateTime"::date not between date '2020-12-31' and date '2024-01-01')       as vp_time,
        (f."TripDistanceMiles" is null or f."TripDistanceMiles" <= 0 or f."TripDistanceMiles" > 100) as vp_dist,
        (f."TotalAmount" is null or f."FareAmount" is null or f."TipAmount" is null
         or f."TotalAmount" <= 0 or f."FareAmount" < 0 or f."TipAmount" < 0
         or f."TotalAmount" > 5000
         or f."TotalAmount" + 0.01 < f."FareAmount" + f."TipAmount" + coalesce(f."TollsAmount", 0)) as vp_amount
    from dds."FactTaxiTrip" f
)
select 'IsTimeUsable' as co, count(*) filter (where vp_time) as so_dong_vi_pham,
       count(*) filter (where vp_time and "IsTimeUsable") as so_dong_vi_pham_ma_van_usable,
       count(*) filter (where not vp_time and not "IsTimeUsable") as so_dong_khong_vi_pham_ma_bi_loai
from d
union all
select 'IsDistanceUsable', count(*) filter (where vp_dist),
       count(*) filter (where vp_dist and "IsDistanceUsable"),
       count(*) filter (where not vp_dist and not "IsDistanceUsable")
from d
union all
select 'IsAmountUsable', count(*) filter (where vp_amount),
       count(*) filter (where vp_amount and "IsAmountUsable"),
       count(*) filter (where not vp_amount and not "IsAmountUsable")
from d
union all
select 'HasDQIssue', count(*) filter (where vp_time or vp_dist or vp_amount),
       count(*) filter (where (vp_time or vp_dist or vp_amount) and not "HasDQIssue"),
       count(*) filter (where not (vp_time or vp_dist or vp_amount) and "HasDQIssue")
from d;


-- @query X06 | Đối chiếu số liệu khảo sát trong Kế hoạch GĐ2 (mục 2.4) với số đếm thực tế theo mã lỗi
-- Kế hoạch ghi: 107.100 dòng thời gian âm; 661 dòng treo > 24 giờ; ~1,75 triệu dòng cự ly bằng 0;
-- 4.156.354 dòng khuyết đồng loạt (3,8177%). Mã lỗi đếm từ DataQualityFlags / AttributeQualityFlags.
with ma as (
    select unnest(string_to_array(f."DataQualityFlags", ';')) as ma
    from dds."FactTaxiTrip" f
    where f."DataQualityFlags" is not null
)
select ma as ma_loi, count(*) as so_dong_thuc_te,
       case ma when 'TIME_REVERSED'     then 107100
               when 'DURATION_OVER_24H' then 661
               when 'ZERO_DISTANCE'     then 1750000 end                      as so_lieu_ke_hoach,
       round(100.0 * count(*) / 108870632, 4)                                  as ty_le_pct
from ma
group by 1
order by 2 desc;

{#
  B02 (nbinh) - Bộ rule chất lượng dữ liệu theo Data Contract v1.2 mục 2.2.
  Mỗi macro trả về một mảng text[] các mã lỗi (mảng rỗng = đạt). Mọi rule được kiểm ĐỘC LẬP,
  không dừng ở lỗi đầu tiên. Cờ usable = mảng tương ứng rỗng.
  Tham số `t` là alias của bảng/CTE chứa các cột của stg_yellow_trips.
#}

{# --- Thời gian: is_time_usable ----------------------------------------------------------- #}
{% macro dq_time_codes(t) -%}
    array_remove(array[
        -- Thiếu một trong hai mốc thời gian
        case when {{ t }}.pickup_datetime is null or {{ t }}.dropoff_datetime is null
             then 'MISSING_TIME' end,
        -- Ngày đón hoặc trả nằm ngoài miền (bao gồm hai biên)
        case when ({{ t }}.pickup_datetime is not null
                   and {{ t }}.pickup_datetime::date not between date '{{ var("dq_date_domain_start") }}'
                                                               and date '{{ var("dq_date_domain_end") }}')
               or ({{ t }}.dropoff_datetime is not null
                   and {{ t }}.dropoff_datetime::date not between date '{{ var("dq_date_domain_start") }}'
                                                                and date '{{ var("dq_date_domain_end") }}')
             then 'DATE_OUT_OF_DOMAIN' end,
        -- Trả khách không sau đón khách (bao gồm bằng nhau)
        case when {{ t }}.dropoff_datetime <= {{ t }}.pickup_datetime
             then 'TIME_REVERSED' end,
        -- Thời lượng vượt 24 giờ
        case when {{ t }}.trip_duration_seconds > 86400
             then 'DURATION_OVER_24H' end
    ]::text[], null)
{%- endmacro %}

{# --- Quãng đường: is_distance_usable (0 < d <= 100) ------------------------------------- #}
{% macro dq_distance_codes(t) -%}
    array_remove(array[
        case when {{ t }}.trip_distance is null then 'MISSING_DISTANCE'
             when {{ t }}.trip_distance < 0     then 'NEGATIVE_DISTANCE'
             when {{ t }}.trip_distance = 0     then 'ZERO_DISTANCE'
             when {{ t }}.trip_distance > 100   then 'DISTANCE_OUTLIER'
        end
    ]::text[], null)
{%- endmacro %}

{# --- Tiền: is_amount_usable (3 điều kiện) ------------------------------------------------- #}
{% macro dq_amount_codes(t) -%}
    array_remove(array[
        -- Điều kiện 1: đủ dữ liệu và không âm
        case when {{ t }}.total_amount is null then 'MISSING_TOTAL_AMOUNT' end,
        case when {{ t }}.fare_amount  is null then 'MISSING_FARE_AMOUNT'  end,
        case when {{ t }}.tip_amount   is null then 'MISSING_TIP_AMOUNT'   end,
        case when {{ t }}.total_amount < 0  then 'NEGATIVE_TOTAL_AMOUNT'
             when {{ t }}.total_amount = 0  then 'ZERO_TOTAL_AMOUNT' end,
        case when {{ t }}.fare_amount  < 0  then 'NEGATIVE_FARE' end,
        case when {{ t }}.tip_amount   < 0  then 'NEGATIVE_TIP'  end,
        -- Điều kiện 2: chặn cực trị
        case when {{ t }}.total_amount > 5000 then 'AMOUNT_OVER_5000' end,
        -- Điều kiện 3: cấu thành cước hợp lý (sai số 1 cent; tolls thiếu tính là 0 chỉ trong phép kiểm)
        case when {{ t }}.total_amount + 0.01
                  < {{ t }}.fare_amount + {{ t }}.tip_amount + coalesce({{ t }}.tolls_amount, 0)
             then 'INCONSISTENT_TOTAL' end
    ]::text[], null)
{%- endmacro %}

{# --- Thuộc tính: attribute_quality_flags (không ảnh hưởng 3 cờ usable) ------------------ #}
{# vendor_is_mapped / rate_code_is_mapped: boolean, tính từ seed mapping dùng chung với S02 #}
{% macro dq_attribute_codes(t, vendor_is_mapped, rate_code_is_mapped) -%}
    array_remove(array[
        case when {{ t }}.passenger_count is null then 'MISSING_PASSENGER_COUNT'
             when {{ t }}.passenger_count <= 0 or {{ t }}.passenger_count > 9 then 'INVALID_PASSENGER_COUNT'
        end,
        case when {{ t }}.vendor_id is null then 'MISSING_VENDOR'
             when not {{ vendor_is_mapped }} then 'UNMAPPED_VENDOR'
        end,
        case when {{ t }}.rate_code_id is null then 'MISSING_RATE_CODE'
             when not {{ rate_code_is_mapped }} then 'UNMAPPED_RATE_CODE'
        end,
        -- store flag: NULL nguồn -> MISSING; có giá trị gốc nhưng không phải Y/N -> INVALID
        case when {{ t }}.store_and_fwd_flag is null and {{ t }}.store_and_fwd_flag_raw is null
                  then 'MISSING_STORE_AND_FWD_FLAG'
             when {{ t }}.store_and_fwd_flag is null
                  then 'INVALID_STORE_AND_FWD_FLAG'
        end,
        -- Tiền nguồn có hơn 2 chữ số thập phân, đã làm tròn về cent ở B01
        case when {{ t }}.money_rounded_columns is not null then 'MONEY_ROUNDED' end
    ]::text[], null)
{%- endmacro %}

{# --- Gộp mã: không lặp, sắp alphabet, nối ';'; không có mã -> NULL ------------------------ #}
{% macro dq_join_codes(codes_expr) -%}
    (select string_agg(distinct c, ';' order by c) from unnest({{ codes_expr }}) as c)
{%- endmacro %}

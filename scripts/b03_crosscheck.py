#!/usr/bin/env python3
"""
B03 (nbinh) - KIỂM CHỨNG ĐỘC LẬP: tính lại các chỉ số chính TRỰC TIẾP từ file Parquet gốc bằng pandas
(không qua staging, dbt hay SQL), cài lại rule Contract v1.2 mục 2.2 bằng Python, rồi so với kết quả
CSV mà scripts/b03_export.py đã xuất trong reports/b03/results/.

Nếu hai cách tính độc lập cho cùng con số thì cả pipeline (S01 -> B01 -> B02 -> S02 -> T02) lẫn SQL B03
đều đúng trên các chỉ số này. Chạy SAU b03_export.py, trong container Mage:
    python3 scripts/b03_crosscheck.py
Chỉ đọc các file Parquet có mặt trong Fact (lấy danh sách từ CB/DQ06), nên dùng được cả sau S03 (sẽ chậm hơn).
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / 'reports' / 'b03' / 'results'
DATA = Path('/home/src/external_data')
DOMAIN = (pd.Timestamp('2020-12-31'), pd.Timestamp('2024-01-01'))

files = pd.read_csv(RES / 'DQ06.csv').file_nguon.tolist()
frames = []
for name in files:
    path = next(DATA.rglob(name))
    t = pq.read_table(path).to_pandas()
    t = t.rename(columns={'Airport_fee': 'airport_fee'})
    frames.append(t)
    print(f'Đọc {name}: {len(t):,} dòng', flush=True)
df = pd.concat(frames, ignore_index=True)
del frames

# ---- Cài lại rule bằng pandas (độc lập với SQL) ----
cents = lambda s: np.rint(s * 100)                       # tiền -> số nguyên cent (dữ liệu thật không có >2 số lẻ)
pu, do = df.tpep_pickup_datetime, df.tpep_dropoff_datetime
dur = (do - pu).dt.total_seconds()
in_dom = lambda s: s.notna() & (s.dt.normalize() >= DOMAIN[0]) & (s.dt.normalize() <= DOMAIN[1])
time_ok = in_dom(pu) & in_dom(do) & (do > pu) & (dur <= 86400)
dist = df.trip_distance
dist_ok = (dist > 0) & (dist <= 100)
T, F, Tp, To = cents(df.total_amount), cents(df.fare_amount), cents(df.tip_amount), cents(df.tolls_amount).fillna(0)
amt_ok = (T.notna() & F.notna() & Tp.notna() & (T > 0) & (F >= 0) & (Tp >= 0) & (T <= 500000)
          & (T + 1 >= F + Tp + To))
rate_key = df.RatecodeID.where(df.RatecodeID.isin([1, 2, 3, 4, 5, 6]), 0).fillna(0).astype(int)
zone = lambda s: s.where(s.between(1, 265), 0).fillna(0).astype(int)
pu_zone, do_zone = zone(df.PULocationID), zone(df.DOLocationID)

checks = []
def add(name, expected, actual, tol=0.0):
    ok = abs(float(expected) - float(actual)) <= tol
    checks.append((name, ok, expected, actual))

# 1. CB08: số chuyến, doanh thu gộp, thuần
cb08 = pd.read_csv(RES / 'CB08.csv').set_index('nam')
add('CB08 tổng số chuyến', len(df), cb08.loc['TONG', 'so_chuyen'])
add('CB08 doanh thu gộp (USD)', T.sum() / 100, cb08.loc['TONG', 'doanh_thu_gop'], 0.005)
add('CB08 doanh thu thuần (USD)', T[amt_ok].sum() / 100, cb08.loc['TONG', 'doanh_thu_thuan'], 0.005)
add('CB08 số chuyến bị loại khỏi thuần', (~amt_ok).sum(), cb08.loc['TONG', 'so_chuyen_bi_loai'])

# 2. CB01: số chuyến theo tháng đón khách (trong miền ngày) + số dòng DateKey 0
cb01 = pd.read_csv(RES / 'CB01.csv')
thang = cb01[(cb01.cap_tong_hop == 'THANG') & (cb01.nam != 1900)]
py_thang = pu[in_dom(pu)].dt.to_period('M').value_counts()
for _, r in thang.iterrows():
    add(f'CB01 số chuyến {int(r.nam)}-{int(r.thang):02d}', py_thang.get(pd.Period(f'{int(r.nam)}-{int(r.thang):02d}'), 0), r.so_chuyen)
key0 = cb01[(cb01.cap_tong_hop == 'NAM') & (cb01.nam == 1900)].so_chuyen.sum()
add('CB01 số chuyến DateKey 0 (ngoài miền / thiếu ngày)', (~in_dom(pu)).sum(), key0)

# 3. MR01: tỷ lệ tip (thẻ, fare > 0, amount usable) và vận tốc có trọng số theo RateCodeKey
mr01 = pd.read_csv(RES / 'MR01.csv').set_index('rate_code_key')
tip_m = (df.payment_type == 1) & (F > 0) & amt_ok
td = time_ok & dist_ok
for k in mr01.index:
    m = rate_key == k
    if (tip_m & m).any():
        add(f'MR01 tỷ lệ tip % rate {k}', round(100 * Tp[tip_m & m].sum() / F[tip_m & m].sum(), 2), mr01.loc[k, 'ty_le_tip_pct'], 0.011)
    if (td & m).any():
        add(f'MR01 vận tốc mph rate {k}', round(3600 * dist[td & m].sum() / dur[td & m].sum(), 2), mr01.loc[k, 'van_toc_mph'], 0.011)

# 4. CB07: top 10 tuyến (loại 0/264/265) - so khớp cặp tuyến và số chuyến
cb07 = pd.read_csv(RES / 'CB07.csv')
ok_pair = ~pu_zone.isin([0, 264, 265]) & ~do_zone.isin([0, 264, 265])
pairs = pd.DataFrame({'p': pu_zone[ok_pair], 'd': do_zone[ok_pair]}).value_counts()
for _, r in cb07.iterrows():
    add(f'CB07 hạng {int(r.hang)} tuyến {int(r.zone_don)}->{int(r.zone_tra)}', pairs.get((r.zone_don, r.zone_tra), 0), r.so_chuyen)
add('CB07 số chuyến tuyến hạng 1 = lớn nhất toàn bộ', pairs.max(), cb07.so_chuyen.max())

# 5. DQ03: khối khuyết đồng loạt
dq03 = pd.read_csv(RES / 'DQ03.csv').set_index('nam_file')
khoi = (df.passenger_count.isna() & df.RatecodeID.isna() & df.store_and_fwd_flag.isna()
        & df.congestion_surcharge.isna() & (df.payment_type == 0))
add('DQ03 dòng khuyết đồng loạt', khoi.sum(), dq03.loc['TONG', 'dong_khuyet_dong_loat'])

# 6. DQ01: số dòng có lỗi usable
dq01 = pd.read_csv(RES / 'DQ01.csv')
tong01 = dq01[dq01.thang_file.isna()].iloc[0]
add('DQ01 dòng HasDQIssue', (~(time_ok & dist_ok & amt_ok)).sum(), tong01.co_loi_usable)
add('DQ01 dòng time không dùng được', (~time_ok).sum(), tong01.time_khong_dung_duoc)
add('DQ01 dòng cự ly không dùng được', (~dist_ok).sum(), tong01.cu_ly_khong_dung_duoc)
add('DQ01 dòng tiền không dùng được', (~amt_ok).sum(), tong01.tien_khong_dung_duoc)

# ---- Báo cáo ----
lines = ['# B03 - Kiểm chứng độc lập từ Parquet gốc', '',
         f'Tính lại bằng pandas trực tiếp từ {len(files)} file Parquet ({len(df):,} dòng), không qua SQL/dbt.', '',
         '| Chỉ số | Pandas (Parquet) | SQL B03 | Kết quả |', '|---|---:|---:|---|']
for n, ok, e, a in checks:
    lines.append(f'| {n} | {e:,.2f} | {float(a):,.2f} | {"PASS" if ok else "**FAIL**"} |')
    print(f'  [{"PASS" if ok else "FAIL"}] {n}: pandas={float(e):,.2f}  sql={float(a):,.2f}', flush=True)
(RES.parent / 'crosscheck_parquet.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
failed = [n for n, ok, *_ in checks if not ok]
print(f'\n{len(checks) - len(failed)}/{len(checks)} PASS' + (f' | FAIL: {failed}' if failed else ''), flush=True)
sys.exit(1 if failed else 0)

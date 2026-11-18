#!/usr/bin/env python3
"""
B04 (nbinh) - Đối soát tài chính & sản lượng: NGUỒN (Parquet gốc) ↔ STAGING ↔ FACT, theo file và theo năm.

Vì sao đọc Parquet gốc: theo S03 / RFC-002 v2, staging.raw_yellow_trips là vùng đệm TẠM (TRUNCATE trước mỗi file,
chỉ giữ file cuối). Để đối soát đủ 36 file, script tính lại số dòng và tổng tiền TRỰC TIẾP từ Parquet gốc, áp dụng
đúng quy tắc chuẩn hóa tiền của B01, rồi so với dds."FactTaxiTrip" (+ staging.quarantine). File còn nằm trong staging
được đối soát thêm 3 chiều: Nguồn = Staging (stg_trips_flagged) = Fact.

Quy tắc chuẩn hóa tiền của B01 (tái hiện chính xác):
  S01 ghi float vào staging dạng chuỗi str(x)  ->  B01 ép numeric(18,2)  =  làm tròn NỬA XA SỐ 0 về cent trên chuỗi đó.
  Ở đây: Decimal(str(x)).quantize(0.01, ROUND_HALF_UP) (ROUND_HALF_UP của Decimal = nửa xa số 0, đúng như PostgreSQL).
  Tổng được cộng bằng SỐ NGUYÊN CENT (không cộng float) nên không có sai số làm tròn tích lũy.

Xuất:
  reports/b04/reconciliation.csv          đối soát theo NĂM (sản phẩm bàn giao B04 theo Kế hoạch GĐ2)
  reports/b04/reconciliation_by_file.csv  đối soát chi tiết 36 file
  reports/b04/reconciliation_summary.md   tóm tắt + kết luận PASS/FAIL

Chạy trong container Mage:  python3 scripts/b04_reconciliation.py      (chỉ ĐỌC database)
"""
import glob
import os
import sys
import time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.getenv('B04_DATA_DIR', '/home/src/external_data'))
OUT = ROOT / 'reports' / 'b04'

# (cột Parquet viết thường, cột Fact, cột staging)
MONEY = [
    ('fare_amount', 'FareAmount', 'fare_amount'),
    ('extra', 'Extra', 'extra'),
    ('mta_tax', 'MtaTax', 'mta_tax'),
    ('tip_amount', 'TipAmount', 'tip_amount'),
    ('tolls_amount', 'TollsAmount', 'tolls_amount'),
    ('improvement_surcharge', 'ImprovementSurcharge', 'improvement_surcharge'),
    ('congestion_surcharge', 'CongestionSurcharge', 'congestion_surcharge'),
    ('airport_fee', 'AirportFee', 'airport_fee'),
    ('total_amount', 'TotalAmount', 'total_amount'),
]
CENT = Decimal('0.01')
# File mẫu cho đối soát 3 chiều khi staging trống: 1 file mỗi schema nguồn (Contract v1.2 mục 2.1)
SAMPLE_FILES = ['yellow_tripdata_2021-01.parquet', 'yellow_tripdata_2023-12.parquet']


def connect():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'), port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'), user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123'))


def to_cents(values: np.ndarray):
    """Mảng float64 (NaN = NULL) -> (tổng cent kiểu int, số NULL, số giá trị bị làm tròn) theo đúng quy tắc B01."""
    ok = ~np.isnan(values)
    v = values[ok]
    scaled = v * 100.0
    near = np.rint(scaled)
    # Giá trị đã đúng 2 chữ số thập phân (sai số biểu diễn float rất nhỏ): lấy thẳng số cent
    exact = np.abs(scaled - near) < 1e-6
    total = int(near[exact].astype(np.int64).sum())
    # Giá trị có hơn 2 chữ số thập phân (ca MONEY_ROUNDED, rất ít): làm tròn đúng như PostgreSQL trên chuỗi str(x)
    rest = v[~exact]
    for x in rest:
        total += int(Decimal(str(float(x))).quantize(CENT, rounding=ROUND_HALF_UP) * 100)
    return total, int((~ok).sum()), int(rest.size)


def read_source():
    rows = []
    files = sorted(glob.glob(str(DATA / 'yellow_tripdata_20[0-9][0-9]-[0-9][0-9].parquet')))
    for f in files:
        t0 = time.time()
        pf = pq.ParquetFile(f)
        names = {n.lower(): n for n in pf.schema_arrow.names}
        cols = [names[m[0]] for m in MONEY]
        tbl = pf.read(columns=cols)
        rec = {'file_nguon': Path(f).name, 'so_dong_footer': pf.metadata.num_rows, 'so_dong_doc_duoc': tbl.num_rows}
        for (src, _, _), col in zip(MONEY, cols):
            arr = tbl.column(col).to_numpy(zero_copy_only=False).astype(np.float64)
            cents, nulls, rounded = to_cents(arr)
            rec[f'nguon_{src}'] = cents
            rec[f'null_{src}'] = nulls
            rec[f'lam_tron_{src}'] = rounded
        rows.append(rec)
        print(f'  Nguồn {rec["file_nguon"]}: {rec["so_dong_footer"]:,} dòng ({time.time() - t0:.1f}s)', flush=True)
    return pd.DataFrame(rows)


def read_fact(cur):
    sums = ', '.join(f'(sum("{fc}") * 100)::bigint as "fact_{s}", count(*) - count("{fc}") as "fact_null_{s}"'
                     for s, fc, _ in MONEY)
    cur.execute(f'select "SourceFileName", count(*), {sums} from dds."FactTaxiTrip" group by 1')
    cols = ['file_nguon', 'so_dong_fact'] + [x for s, _, _ in MONEY for x in (f'fact_{s}', f'fact_null_{s}')]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def read_quarantine(cur):
    cur.execute('select source_file_name, count(distinct source_row_number) from staging.quarantine group by 1')
    return pd.DataFrame(cur.fetchall(), columns=['file_nguon', 'so_dong_quarantine'])


def read_staging(cur):
    sums = ', '.join(f'(sum({sc}) * 100)::bigint as "stg_{s}"' for s, _, sc in MONEY)
    cur.execute(f'select source_file_name, count(*), {sums} from staging.stg_trips_flagged group by 1')
    cols = ['file_nguon', 'so_dong_staging'] + [f'stg_{s}' for s, _, _ in MONEY]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def stage_and_rollback(files):
    """Nạp lại từng file mẫu vào staging.raw_yellow_trips BẰNG ĐÚNG hàm định dạng của S01 (format_series_for_staging),
    đọc tổng qua toàn bộ chuỗi view B01/B02 (raw -> stg_yellow_trips -> stg_trips_flagged) rồi ROLLBACK.
    Database không thay đổi gì; dùng khi staging đang trống (vùng đệm tạm, hoặc UNLOGGED bị xóa sau khi Postgres khởi động lại)."""
    sys.path.insert(0, str(ROOT))
    from lab_project.data_exporters.export_parquet_chunks import (  # noqa: E402
        COPY_SQL, PARQUET_SOURCE_ORDER, format_series_for_staging)
    import csv, io  # noqa: E401,E402
    int_like = {'VendorID', 'passenger_count', 'RatecodeID', 'PULocationID', 'DOLocationID', 'payment_type'}
    target = {'VendorID': 'vendor_id', 'RatecodeID': 'rate_code_id', 'PULocationID': 'pu_location_id',
              'DOLocationID': 'do_location_id'}
    out = []
    for name in files:
        t0 = time.time()
        conn = connect()
        cur = conn.cursor()
        cur.execute('select file_sha256 from meta.file_manifest where file_name = %s', (name,))
        sha = cur.fetchone()[0]
        cur.execute('select count(*) from staging.raw_yellow_trips')
        if cur.fetchone()[0]:
            conn.close()
            raise RuntimeError('staging.raw_yellow_trips đang có dữ liệu: không nạp thử để tránh trộn file')
        pf = pq.ParquetFile(DATA / name)
        airport = next(n for n in pf.schema_arrow.names if n.lower() == 'airport_fee')
        row = 0
        for rb in pf.iter_batches(batch_size=250_000, columns=PARQUET_SOURCE_ORDER + [airport]):
            df = rb.to_pandas()
            frame = {'source_file_name': name, 'source_file_sha256': sha,
                     'source_row_number': range(row + 1, row + rb.num_rows + 1),
                     'load_batch_id': 'B04_RECON_ROLLBACK', 'loaded_at_utc': '2000-01-01 00:00:00'}
            for c in PARQUET_SOURCE_ORDER:
                frame[target.get(c, c)] = format_series_for_staging(df[c], is_int_like=c in int_like)
            frame['airport_fee'] = format_series_for_staging(df[airport])
            buf = io.StringIO()
            pd.DataFrame(frame).to_csv(buf, sep='\t', header=False, index=False, na_rep='', quoting=csv.QUOTE_NONE)
            buf.seek(0)
            cur.copy_expert(COPY_SQL, buf)
            row += rb.num_rows
        cur.execute('select count(*) from staging.raw_yellow_trips')
        raw_rows = cur.fetchone()[0]
        cur.execute('select count(*) from staging.stg_yellow_trips_rejects')
        rej = cur.fetchone()[0]
        df_stg = read_staging(cur)
        conn.rollback()   # KHÔNG ghi gì vào database
        conn.close()
        df_stg['so_dong_raw'] = raw_rows
        df_stg['so_dong_reject'] = rej
        out.append(df_stg)
        print(f'  Staging (nạp thử + ROLLBACK) {name}: raw {raw_rows:,}, flagged {int(df_stg.so_dong_staging.sum()):,}, '
              f'reject {rej:,} ({time.time() - t0:.0f}s)', flush=True)
    return pd.concat(out, ignore_index=True)


def money(cents):
    return None if pd.isna(cents) else round(int(cents) / 100, 2)


def build(df_src, df_fact, df_q, df_stg):
    d = df_src.merge(df_fact, on='file_nguon', how='outer').merge(df_q, on='file_nguon', how='left') \
        .merge(df_stg, on='file_nguon', how='left')
    d['so_dong_quarantine'] = d['so_dong_quarantine'].fillna(0).astype('int64')
    d['nam'] = d['file_nguon'].str.extract(r'(\d{4})-\d{2}')[0]
    d['co_trong_staging'] = d['so_dong_staging'].notna()
    return d


def compare(d, key):
    """Gộp theo key (file hoặc năm), tính chênh lệch. Tiền: Fact phải bằng Nguồn vì quarantine = 0 dòng;
    nếu có quarantine, phần chênh tiền được báo riêng để giải thích (dòng quarantine không vào Fact)."""
    num = [c for c in d.columns if c.startswith(('nguon_', 'fact_', 'null_', 'lam_tron_', 'so_dong', 'stg_'))]
    g = d.groupby(key, dropna=False)[num].sum(min_count=1).reset_index()
    g.insert(1, 'so_file', d.groupby(key, dropna=False).size().values)
    out = pd.DataFrame({key: g[key], 'so_file': g['so_file']})
    out['so_dong_footer'] = g['so_dong_footer'].astype('Int64')
    out['so_dong_doc_duoc'] = g['so_dong_doc_duoc'].astype('Int64')
    out['so_dong_fact'] = g['so_dong_fact'].astype('Int64')
    out['so_dong_quarantine'] = g['so_dong_quarantine'].astype('Int64')
    out['lech_so_dong'] = (g['so_dong_footer'] - g['so_dong_fact'] - g['so_dong_quarantine']).astype('Int64')
    for s, _, _ in MONEY:
        out[f'nguon_{s}'] = g[f'nguon_{s}'].map(money)
        out[f'fact_{s}'] = g[f'fact_{s}'].map(money)
        out[f'lech_{s}'] = (g[f'nguon_{s}'] - g[f'fact_{s}']).map(money)
        out[f'null_nguon_{s}'] = g[f'null_{s}'].astype('Int64')
        out[f'null_fact_{s}'] = g[f'fact_null_{s}'].astype('Int64')
        out[f'so_gia_tri_lam_tron_{s}'] = g[f'lam_tron_{s}'].astype('Int64')
    return out


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    print('1) Đọc 36 file Parquet gốc ...', flush=True)
    df_src = read_source()

    print('2) Đọc Fact / Quarantine / Staging (chỉ đọc) ...', flush=True)
    conn = connect()
    conn.set_session(isolation_level='REPEATABLE READ', readonly=True)
    cur = conn.cursor()
    df_fact, df_q, df_stg = read_fact(cur), read_quarantine(cur), read_staging(cur)
    conn.close()
    if df_stg.empty:
        print('3) Staging đang trống -> nạp thử 2 file mẫu (schema v1 & v2) trong transaction rồi ROLLBACK ...',
              flush=True)
        df_stg = stage_and_rollback(SAMPLE_FILES)

    d = build(df_src, df_fact, df_q, df_stg)
    by_file = compare(d, 'file_nguon')
    by_year = compare(d, 'nam')
    tong = compare(d.assign(nam='TONG'), 'nam')
    by_year = pd.concat([by_year, tong], ignore_index=True)

    # Đối soát 3 chiều cho file còn trong staging: Nguồn = Staging = Fact
    staged = d[d['co_trong_staging']]
    three_way = []
    for _, r in staged.iterrows():
        rec = {'file_nguon': r['file_nguon'], 'so_dong_footer': int(r['so_dong_footer']),
               'so_dong_raw': None if pd.isna(r.get('so_dong_raw')) else int(r['so_dong_raw']),
               'so_dong_reject': None if pd.isna(r.get('so_dong_reject')) else int(r['so_dong_reject']),
               'so_dong_staging': int(r['so_dong_staging']), 'so_dong_fact': int(r['so_dong_fact'])}
        for s, _, _ in MONEY:
            rec[f'nguon_{s}'], rec[f'staging_{s}'], rec[f'fact_{s}'] = \
                money(r[f'nguon_{s}']), money(r[f'stg_{s}']), money(r[f'fact_{s}'])
        three_way.append(rec)
    df3 = pd.DataFrame(three_way)

    by_year.to_csv(OUT / 'reconciliation.csv', index=False, encoding='utf-8-sig')
    by_file.to_csv(OUT / 'reconciliation_by_file.csv', index=False, encoding='utf-8-sig')
    df3.to_csv(OUT / 'reconciliation_staging_3way.csv', index=False, encoding='utf-8-sig')

    # ---- Kết luận ----
    lech_cols = [c for c in by_file.columns if c.startswith('lech_')]
    file_lech = by_file[(by_file[lech_cols].fillna(0) != 0).any(axis=1)]
    null_bad = [s for s, _, _ in MONEY if (by_file[f'null_nguon_{s}'] != by_file[f'null_fact_{s}']).any()]
    footer_bad = by_file[by_file['so_dong_footer'] != by_file['so_dong_doc_duoc']]
    three_bad = [] if df3.empty else [
        r['file_nguon'] for _, r in df3.iterrows()
        if not (r['so_dong_footer'] == r['so_dong_staging'] + (r['so_dong_reject'] or 0) == r['so_dong_fact']
                + (r['so_dong_reject'] or 0)
                and (r['so_dong_raw'] is None or r['so_dong_raw'] == r['so_dong_footer'])
                and all(r[f'nguon_{s}'] == r[f'staging_{s}'] == r[f'fact_{s}'] for s, _, _ in MONEY))]
    checks = [
        ('Footer Parquet = số dòng đọc được (36 file)', footer_bad.empty, f'{len(by_file)} file'),
        ('Footer = Fact + Quarantine, tổng tiền 9 cột Nguồn = Fact (từng file, đến từng cent)', file_lech.empty,
         f'{len(file_lech)} file lệch'),
        ('Số giá trị NULL từng cột tiền: Nguồn = Fact', not null_bad, ', '.join(null_bad) or 'khớp 9/9 cột'),
        ('Đối soát 3 chiều Nguồn = Raw = Staging (flagged + reject) = Fact (số dòng + 9 cột tiền)', not three_bad and not df3.empty,
         ', '.join(df3['file_nguon']) if not three_bad else 'lệch: ' + ', '.join(three_bad)),
    ]
    tg = by_year[by_year['nam'] == 'TONG'].iloc[0]
    lines = ['# B04 - Đối soát tài chính & sản lượng: Nguồn (Parquet) ↔ Staging ↔ Fact', '',
             f'- Thời gian chạy: {time.time() - t0:.0f} giây',
             f'- Phạm vi: {len(by_file)} file, {int(tg["so_dong_footer"]):,} dòng theo footer Parquet', '',
             '## Kết quả kiểm tra', '', '| Kiểm tra | Kết quả | Chi tiết |', '|---|---|---|']
    lines += [f'| {n} | {"PASS" if ok else "**FAIL**"} | {dt} |' for n, ok, dt in checks]
    lines += ['', '## Đối soát theo năm', '',
              '| Năm | File | Dòng nguồn (footer) | Dòng Fact | Quarantine | Lệch dòng | TotalAmount nguồn | TotalAmount Fact | Lệch |',
              '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
    for _, r in by_year.iterrows():
        lines.append(f'| {r["nam"]} | {r["so_file"]} | {int(r["so_dong_footer"]):,} | {int(r["so_dong_fact"]):,} | '
                     f'{int(r["so_dong_quarantine"]):,} | {int(r["lech_so_dong"]):,} | {r["nguon_total_amount"]:,.2f} | '
                     f'{r["fact_total_amount"]:,.2f} | {r["lech_total_amount"]:,.2f} |')
    lines += ['', 'Chi tiết 9 cột tiền và số giá trị NULL / bị làm tròn: `reconciliation.csv` (theo năm), '
              '`reconciliation_by_file.csv` (36 file), `reconciliation_staging_3way.csv` (đối soát 3 chiều qua staging: file mẫu schema v1 & v2, nạp thử rồi ROLLBACK).']
    (OUT / 'reconciliation_summary.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')

    for n, ok, dt in checks:
        print(f'  [{"PASS" if ok else "FAIL"}] {n}: {dt}', flush=True)
    failed = [n for n, ok, _ in checks if not ok]
    print('TẤT CẢ ĐỐI SOÁT PASS' if not failed else 'CÓ ĐỐI SOÁT FAIL', flush=True)
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()

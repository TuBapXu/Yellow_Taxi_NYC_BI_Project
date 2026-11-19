#!/usr/bin/env python3
"""
B03 (nbinh) - Chạy 15 truy vấn kiểm chứng (sql/acceptance_queries.sql) và 6 truy vấn audit DQ
(sql/dq_audit_queries.sql) trên dds."FactTaxiTrip", rồi xuất:
  reports/b03/results/<MÃ>.csv        kết quả từng câu (UTF-8 có BOM để Excel đọc đúng tiếng Việt)
  reports/b03/results/MR05_zones.geojson   đa giác 263 zone (EPSG:4326) cho bản đồ MR05
  reports/b03/dq_audit_3y.xlsx        bảng thống kê DQ + kiểm toán doanh thu + tóm tắt CB08/MR01
  reports/b03/run_summary.md          thời gian chạy, số dòng mỗi câu, phạm vi dữ liệu, kết quả tự kiểm

Chạy trong container Mage:  python3 scripts/b03_export.py
Chỉ ĐỌC dữ liệu (SELECT); không ghi gì vào database.
"""
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports' / 'b03'
RES = OUT / 'results'
SQL_FILES = [ROOT / 'sql' / 'acceptance_queries.sql', ROOT / 'sql' / 'dq_audit_queries.sql']
MARKER = re.compile(r'^-- @query (\w+) \| (.+)$', re.M)


def connect():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'), port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'), user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123'))


def split_queries(path):
    """Tách file SQL theo dòng '-- @query MÃ | tiêu đề'."""
    text = path.read_text(encoding='utf-8')
    marks = list(MARKER.finditer(text))
    out = []
    for i, m in enumerate(marks):
        body = text[m.end(): marks[i + 1].start() if i + 1 < len(marks) else len(text)]
        out.append((m.group(1), m.group(2).strip(), body.strip().rstrip(';')))
    return out


def run(cur, sql):
    cur.execute(sql)
    cols = [c.name for c in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    # Decimal -> float để Excel/CSV xử lý như số (giữ đủ độ chính xác cho tiền 2 số lẻ)
    for c in df.columns:
        if df[c].map(lambda v: isinstance(v, Decimal)).any():
            df[c] = df[c].map(lambda v: float(v) if isinstance(v, Decimal) else v)
    return int_like(df)


INT_COLS = ('nam', 'thang', 'gio', 'zone_key', 'hang', 'rate_code_key')


def int_like(df):
    """Cột đếm / năm / tháng / giờ có NULL bị pandas đổi sang float (2021.0, 0.0): trả về kiểu Int64 để CSV ghi 2021, 0."""
    for c in df.columns:
        if not (c.startswith('so_') or c in INT_COLS):
            continue
        s = pd.to_numeric(df[c], errors='coerce') if df[c].dtype == object else df[c]
        if s.dtype.kind == 'f' and s.notna().any() and (s.dropna() % 1 == 0).all():
            df[c] = s.astype('Int64')
    return df


def self_checks(results, fact_rows):
    """Kiểm chéo giữa các câu: tổng phải khớp Fact và khớp lẫn nhau. Trả list (tên, đạt, chi tiết)."""
    checks = []

    def add(name, ok, detail):
        checks.append((name, bool(ok), detail))

    cb01 = results['CB01']
    tong = int(cb01.loc[cb01.cap_tong_hop == 'TONG', 'so_chuyen'].iloc[0])
    add('CB01 tổng chuyến = COUNT(Fact)', tong == fact_rows, f'{tong:,} vs {fact_rows:,}')

    cb03 = int(results['CB03'].so_chuyen.sum())
    add('CB03 tổng chuyến = COUNT(Fact)', cb03 == fact_rows, f'{cb03:,}')

    cb04 = results['CB04'].groupby('vai_tro').so_chuyen.sum().to_dict()
    add('CB04 mỗi vai trò = COUNT(Fact)', all(int(v) == fact_rows for v in cb04.values()), str(cb04))

    cb06 = results['CB06']
    add('CB06 tổng đón = tổng trả = COUNT(Fact)',
        int(cb06.so_chuyen_don.sum()) == fact_rows == int(cb06.so_chuyen_tra.sum()),
        f'{int(cb06.so_chuyen_don.sum()):,} / {int(cb06.so_chuyen_tra.sum()):,}')

    cb08 = results['CB08'].set_index('nam')
    thuan = round(float(cb08.loc['TONG', 'doanh_thu_thuan']), 2)
    gop = round(float(cb08.loc['TONG', 'doanh_thu_gop']), 2)
    cb02 = results['CB02'].groupby('cap_thoi_gian').doanh_thu_thuan.sum().round(2).to_dict()
    add('CB02 mỗi cấp năm/tháng/ngày/giờ: tổng doanh thu thuần = CB08',
        set(cb02) == {'NAM', 'THANG', 'NGAY', 'GIO'} and all(abs(float(v) - thuan) < 0.005 for v in cb02.values()),
        ', '.join(f'{k} {float(v):,.2f}' for k, v in sorted(cb02.items())) + f' vs {thuan:,.2f}')

    for code in ('MR05B', 'MR05B_GIO'):
        s = round(float(results[code].doanh_thu_thuan.fillna(0).sum()), 2)
        add(f'{code} tổng doanh thu thuần = CB08', abs(s - thuan) < 0.005, f'{s:,.2f}')

    mr05 = results['MR05']
    geo = results['CB06'].loc[results['CB06'].co_ban_do.astype(bool)]
    don, tra = int(geo.so_chuyen_don.sum()), int(geo.so_chuyen_tra.sum())
    add('MR05 (5a) tổng đón / trả = CB06 trên các zone có đa giác',
        int(mr05.so_chuyen_don.sum()) == don and int(mr05.so_chuyen_tra.sum()) == tra,
        f'đón {int(mr05.so_chuyen_don.sum()):,} vs {don:,}, trả {int(mr05.so_chuyen_tra.sum()):,} vs {tra:,}')

    cb05 = round(float(results['CB05'].doanh_thu_thuan.fillna(0).sum()), 2)
    add('CB05 tổng doanh thu thuần = CB08', abs(cb05 - thuan) < 0.005, f'{cb05:,.2f}')

    dq04 = results['DQ04'].set_index('nam_file')
    add('DQ04 gộp/thuần = CB08 (cùng Fact, khác cách nhóm)',
        abs(float(dq04.loc['TONG', 'doanh_thu_gop']) - gop) < 0.005
        and abs(float(dq04.loc['TONG', 'doanh_thu_thuan']) - thuan) < 0.005,
        f'gộp {gop:,.2f}, thuần {thuan:,.2f}')

    dq06 = results['DQ06']
    staged = dq06.loc[dq06.co_trong_staging.astype(bool)]
    add('DQ06 footer = Fact + Quarantine (mọi file); tiền staging = Fact (file còn trong staging)',
        (dq06.lech_so_dong == 0).all() and (staged.lech_tong_tien.abs() < 0.005).all(),
        f'{len(dq06)} file, {int((dq06.lech_so_dong != 0).sum())} file lệch dòng; '
        f'{len(staged)} file đối soát tiền')

    mr04 = results['MR04']
    add('MR04 đủ 36 tháng lịch', len(mr04) == 36, f'{len(mr04)} tháng, {int(mr04.co_du_lieu.sum())} tháng có dữ liệu')

    top = results['CB07']
    add('CB07 không chứa zone 0/264/265', not top[['zone_don', 'zone_tra']].isin([0, 264, 265]).any().any(),
        f'{len(top)} tuyến')
    return checks


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    RES.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    conn = connect()
    conn.set_session(readonly=True)
    cur = conn.cursor()

    cur.execute('select count(*) from dds."FactTaxiTrip"')
    fact_rows = cur.fetchone()[0]
    scope = run(cur, '''select "SourceFileName" as file_nguon, count(*) as so_dong
                        from dds."FactTaxiTrip" group by 1 order by 1''')
    print(f'Fact: {fact_rows:,} dòng từ {len(scope)} file', flush=True)

    results, timings, titles = {}, {}, {}
    for path in SQL_FILES:
        for code, title, sql in split_queries(path):
            t0 = time.time()
            df = run(cur, sql)
            timings[code] = round(time.time() - t0, 2)
            results[code], titles[code] = df, title
            df.to_csv(RES / f'{code}.csv', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_MINIMAL)
            print(f'  {code:5s} {len(df):>7,} dòng  {timings[code]:>8.2f}s  {title}', flush=True)

    # Đa giác zone cho bản đồ MR05 (tách khỏi CSV vì GeoJSON rất dài)
    cur.execute('''select json_build_object('type','FeatureCollection','features', json_agg(json_build_object(
                       'type','Feature','geometry', st_asgeojson("ZoneGeometry", 6)::json,
                       'properties', json_build_object('zone_key',"ZoneKey",'borough',"BoroughName",'zone',"ZoneName"))))
                   from dds."DimZone" where "HasGeometry"''')
    (RES / 'MR05_zones.geojson').write_text(json.dumps(cur.fetchone()[0], ensure_ascii=False), encoding='utf-8')
    conn.close()

    checks = self_checks(results, fact_rows)
    for name, ok, detail in checks:
        print(f'  [{"PASS" if ok else "FAIL"}] {name}: {detail}', flush=True)

    # ---------------- Excel: dq_audit_3y.xlsx ----------------
    readme = pd.DataFrame([
        ['Thời điểm chạy (UTC)', started.strftime('%Y-%m-%d %H:%M:%S')],
        ['Phạm vi dữ liệu', f'{fact_rows:,} dòng Fact từ {len(scope)} file: ' + ', '.join(scope.file_nguon)],
        ['Lưu ý', 'Số liệu 3 năm chỉ có ý nghĩa nghiệm thu khi đã nạp đủ 36 file (S03). Chạy lại scripts/run_b03.ps1 sau S03.'],
        ['Doanh thu GỘP', 'SUM(TotalAmount) trên mọi dòng Fact (tổng tiền nguồn)'],
        ['Doanh thu THUẦN (đủ điều kiện)', 'SUM(TotalAmount) WHERE IsAmountUsable (Kế hoạch GĐ2 mục 2.5 CB08; Contract v1.2 mục 4)'],
        ['Vận tốc TB', '3600 * SUM(dặm) / SUM(giây) trên dòng IsTimeUsable AND IsDistanceUsable (không AVG đơn giản)'],
        ['Tỷ lệ tip', '100 * SUM(tip) / SUM(fare) trên chuyến trả thẻ, fare > 0, IsAmountUsable'],
        ['Baseline khuyết đồng loạt', '4.156.354 dòng = 3,8177% của 108.870.632 dòng (khảo sát ban đầu)'],
        ['Nguồn SQL', 'sql/acceptance_queries.sql (CB/MR), sql/dq_audit_queries.sql (DQ)'],
    ], columns=['Mục', 'Nội dung'])
    sheets = [
        ('README', readme),
        ('1_Ty_le_co_theo_thang', results['DQ01']),
        ('2_Tan_suat_ma_loi', results['DQ02']),
        ('3_So_voi_baseline', results['DQ03']),
        ('4_Kiem_toan_doanh_thu', results['DQ04']),
        ('5_Ca_cuc_tri', results['DQ05']),
        ('6_Doi_soat_stg_fact', results['DQ06']),
        ('CB08_Gop_va_thuan', results['CB08']),
        ('MR01_Tip_van_toc', results['MR01']),
        ('Tu_kiem', pd.DataFrame([(n, 'PASS' if ok else 'FAIL', d) for n, ok, d in checks],
                                 columns=['Kiểm tra', 'Kết quả', 'Chi tiết'])),
    ]
    xlsx = OUT / 'dq_audit_3y.xlsx'
    with pd.ExcelWriter(xlsx, engine='openpyxl') as xw:
        for name, df in sheets:
            df.to_excel(xw, sheet_name=name, index=False)
            ws = xw.sheets[name]
            ws.freeze_panes = 'A2'
            for col in ws.columns:
                width = max(len(str(c.value)) if c.value is not None else 0 for c in col)
                ws.column_dimensions[col[0].column_letter].width = min(max(10, width + 2), 80)

    # ---------------- run_summary.md ----------------
    lines = [
        '# B03 - Kết quả chạy truy vấn kiểm chứng và audit DQ', '',
        f'- Thời điểm chạy (UTC): {started:%Y-%m-%d %H:%M:%S}',
        f'- Phạm vi: **{fact_rows:,} dòng Fact** từ {len(scope)} file: ' + ', '.join(f'`{x}`' for x in scope.file_nguon),
        f'- Tổng thời gian: {sum(timings.values()):.1f} giây', '',
        '| Mã | Nội dung | Số dòng kết quả | Thời gian (s) |', '|---|---|---:|---:|',
    ]
    lines += [f'| {c} | {titles[c]} | {len(results[c]):,} | {timings[c]} |' for c in results]
    lines += ['', '## Tự kiểm chéo', '', '| Kiểm tra | Kết quả | Chi tiết |', '|---|---|---|']
    lines += [f'| {n} | {"PASS" if ok else "**FAIL**"} | {d} |' for n, ok, d in checks]
    (OUT / 'run_summary.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')

    failed = [n for n, ok, _ in checks if not ok]
    print(f'\nXuất xong: {xlsx}\n{"TẤT CẢ TỰ KIỂM PASS" if not failed else "CÓ TỰ KIỂM FAIL: " + ", ".join(failed)}', flush=True)
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
B04 (nbinh) - Chạy sql/b04_dq_review_queries.sql (rà soát ca cực trị + kiểm chứng bộ cờ) và xuất CSV vào
reports/b04/dq_review/<MÃ>.csv. Dùng lại hàm tách/chạy câu của scripts/b03_export.py. Chỉ ĐỌC database.

Chạy trong container Mage:  python3 scripts/b04_dq_review.py
"""
import csv
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import b03_export as b  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports' / 'b04' / 'dq_review'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    conn = b.connect()
    conn.set_session(isolation_level='REPEATABLE READ', readonly=True)
    cur = conn.cursor()
    results = {}
    for code, title, sql in b.split_queries(ROOT / 'sql' / 'b04_dq_review_queries.sql'):
        t0 = time.time()
        df = b.run(cur, sql)
        results[code] = df
        df.to_csv(OUT / f'{code}.csv', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_MINIMAL)
        print(f'  {code} {len(df):>6,} dòng {time.time() - t0:8.1f}s  {title}', flush=True)
    conn.close()

    x05 = results['X05']
    ok = (x05.so_dong_vi_pham_ma_van_usable == 0).all() and (x05.so_dong_khong_vi_pham_ma_bi_loai == 0).all()
    x01 = results['X01']
    fare_ok = int(x01.so_chuyen_van_usable.fillna(0).sum()) == 0
    print(f'  [{"PASS" if ok else "FAIL"}] X05 cờ lưu trong Fact khớp 100% điều kiện tính lại', flush=True)
    print(f'  [{"PASS" if fare_ok else "FAIL"}] X01 mọi chuyến fare âm đều bị IsAmountUsable = false', flush=True)
    sys.exit(0 if ok and fare_ok else 1)


if __name__ == '__main__':
    main()

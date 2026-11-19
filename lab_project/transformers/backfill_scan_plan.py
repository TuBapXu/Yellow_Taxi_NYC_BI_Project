import os
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from lab_project.data_loaders.scan_parquet_manifest import scan_and_register_manifest


@transformer
def backfill_scan_plan(data, *args, **kwargs):
    """
    Block 2: Quet toan bo 36 tep Parquet lich su (2021-2023), kiem tra footer,
    tinh SHA-256 va lap ke hoach Backfill dong nhat.
    """
    print("[BACKFILL STAGE 2] Quet danh muc 36 file Parquet lich su...")
    data_dir = '/home/src/external_data'
    df_manifest = scan_and_register_manifest(
        data_dir=data_dir,
        file_pattern='yellow_tripdata_*.parquet'
    )
    total_files = len(df_manifest)
    total_footer_rows = int(df_manifest['total_rows'].sum()) if 'total_rows' in df_manifest.columns else 0

    print(f"-> Ke hoach Backfill: Phat hien {total_files} file voi tong {total_footer_rows:,} dong khai bao.")
    return {
        'total_files': total_files,
        'total_footer_rows': total_footer_rows,
        'manifest_records': df_manifest.to_dict('records')
    }

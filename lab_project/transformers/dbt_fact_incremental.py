import time
import json
import subprocess
from pathlib import Path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def dbt_fact_incremental(*args, **kwargs):
    """
    Task T03: Transformer nạp dbt Incremental vào bảng dds."FactTaxiTrip".
    - 11 Role-playing FKs
    - 13 Measures & 7 DQ Flags
    - Chiến lược delete+insert theo TripKey đảm bảo tính Idempotent
    """
    root = (
        Path(__file__).resolve().parents[2]
        if '__file__' in globals()
        else (Path('/home/src') if Path('/home/src/dbt_project').exists() else Path.cwd())
    )
    dbt_dir = root / 'dbt_project'

    t0 = time.time()
    target_file = kwargs.get('target_file') or kwargs.get('source_file_name')
    print(f"[START] Đang chạy dbt run --select FactTaxiTrip (target_file={target_file})...")
    cmd = ['dbt', 'run', '--select', 'FactTaxiTrip']
    if target_file:
        cmd.extend(['--vars', json.dumps({'source_file_name': str(target_file)})])
    res = subprocess.run(cmd, cwd=dbt_dir, capture_output=True, text=True)
    duration = round(time.time() - t0, 2)

    if res.returncode != 0:
        print("[ERROR] dbt FactTaxiTrip thất bại:")
        print(res.stderr or res.stdout)
        raise RuntimeError(f"dbt run FactTaxiTrip failed with exit code {res.returncode}")

    print(f"[SUCCESS] dbt FactTaxiTrip hoàn thành trong {duration}s.")
    return {
        'status': 'SUCCESS',
        'duration_seconds': duration,
        'dbt_output': res.stdout[-500:] if res.stdout else ''
    }

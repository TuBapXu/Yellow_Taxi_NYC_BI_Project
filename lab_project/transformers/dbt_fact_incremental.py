import time
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
    root = Path(__file__).resolve().parents[2]
    dbt_dir = root / 'dbt_project'

    t0 = time.time()
    print("[START] Đang chạy dbt run --select FactTaxiTrip...")
    cmd = ['dbt', 'run', '--select', 'FactTaxiTrip']
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

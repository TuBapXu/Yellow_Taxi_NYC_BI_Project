import time
import subprocess
from pathlib import Path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def dbt_staging_and_dims(*args, **kwargs):
    """
    Task T03: Transformer thực thi biến đổi dbt cho tầng Staging và 8 Dimension Tables.
    - staging.stg_yellow_trips
    - staging.stg_trips_flagged (áp dụng toàn bộ DQ rules B02)
    - dds.DimDate, DimTime, DimZone, DimVendor, DimPaymentType, DimRateCode, DimPassengerGroup, DimDistanceBand
    """
    root = Path(__file__).resolve().parents[2] if '__file__' in globals() else (Path('/home/src') if Path('/home/src/lab_project').exists() else Path.cwd())
    dbt_dir = root / 'dbt_project'

    t0 = time.time()
    print("[START] Đang chạy dbt cho Staging và 8 Dimensions...")
    cmd = ['dbt', 'run', '--select', 'staging', 'marts.dims', '--threads', '1']
    res = subprocess.run(cmd, cwd=dbt_dir, capture_output=True, text=True, close_fds=True)
    duration = round(time.time() - t0, 2)

    if res.returncode != 0:
        print("[ERROR] dbt staging & dims thất bại:")
        print(res.stderr or res.stdout)
        raise RuntimeError(f"dbt run staging/dims failed with exit code {res.returncode}")

    print(f"[SUCCESS] dbt staging & dims hoàn thành trong {duration}s.")
    return {
        'status': 'SUCCESS',
        'duration_seconds': duration,
        'dbt_output': res.stdout[-500:] if res.stdout else ''
    }

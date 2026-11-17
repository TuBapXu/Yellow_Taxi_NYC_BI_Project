"""Run inside Mage: python scripts/run_integration_acceptance.py DATABASE mock|sample.
Only accepts an isolated taxi_acceptance_* database. Never writes the main Fact.
Evidence and dbt artifacts are retained under ignored logs/integration/DATABASE/PHASE.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from collections import Counter
import psycopg2
from psycopg2.extras import RealDictCursor

root = Path(__file__).resolve().parents[1]
name, phase = sys.argv[1:]
if not re.fullmatch(r'taxi_acceptance_[a-z0-9_]+', name) or phase not in ('mock', 'sample'):
    raise ValueError('Use an isolated taxi_acceptance_* database and mock/sample phase')
os.environ['POSTGRES_DB'] = name
out = root / 'logs/integration' / name / phase
out.mkdir(parents=True, exist_ok=True)
variables = json.dumps(dict(b01_use_mock=phase == 'mock', integration_acceptance=True))
base = ['--profiles-dir', '.', '--vars', variables, '--target-path', str(out / 'target'),
        '--log-path', str(out / 'dbt_logs'), '--threads', '1']
def dbt(*args):
    command = ['dbt', *args, *base]
    print('RUN:', ' '.join(command), flush=True)
    subprocess.run(command, cwd=root / 'dbt_project', check=True)

dbt('seed', '--full-refresh')
dbt('run', '--select', 'staging', 'marts.dims', 'acceptance')
dbt('test')
results = json.loads((out / 'target/run_results.json').read_text())['results']
counts = dict(Counter(r['status'] for r in results))
warnings = [r for r in results if r['status']=='warn']
assert len(warnings) == (1 if phase=='mock' else 0), counts
if warnings:
    assert 'warn_trip_distance_precision_over_6' in warnings[0]['unique_id']
    assert warnings[0]['failures'] == 1
conn = psycopg2.connect(host=os.environ['POSTGRES_HOST'],port=os.environ['POSTGRES_PORT'],
                       user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'],dbname=name)
def query(sql):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]
report = dict(database=name,phase=phase,dbt_tests=counts)
report['projection'] = query("""select "SourceFileName" as source_file, count(*) as rows,
 sum("TotalAmount") as total_amount, count(*) filter(where "HasDQIssue") as dq_issues,
 count(*) filter(where "VendorKey"=0) as vendor_key_zero,
 count(*) filter(where "RateCodeKey"=0) as rate_key_zero,
 count(*) filter(where "RateCodeKey"=99) as rate_key_99
 from qa.acceptance_fact_input group by 1 order by 1""")
report['staging'] = query('select source_file_name,count(*) as rows,sum(total_amount) as total_amount from staging.stg_yellow_trips group by 1 order by 1')
report['rejects'] = query('select count(*) as rows from staging.stg_yellow_trips_rejects')[0]['rows']
report['rate99_flags'] = query("""select count(*) as rate99_rows,
 count(*) filter (where attribute_quality_flags ~ '(^|;)UNMAPPED_RATE_CODE(;|$)') as unmapped
 from staging.stg_trips_flagged where rate_code_id=99""")[0]
report['dimensions'] = {}
for dim in ['Date','Time','Zone','Vendor','PaymentType','RateCode','PassengerGroup','DistanceBand']:
    report['dimensions'][dim]=query('select count(*) as rows from dds."Dim'+dim+'"')[0]['rows']
report['fact_rows'] = query('select count(*) as rows from dds."FactTaxiTrip"')[0]['rows']
assert report['fact_rows']==0, 'Acceptance must not load the production Fact'
if phase=='mock':
    assert sum(x['rows'] for x in report['projection'])==60
    assert report['rejects']==8
    assert report['dimensions']['RateCode']==8
    assert report['rate99_flags']['rate99_rows']>0 and report['rate99_flags']['unmapped']==0
else:
    report['manifest'] = query('select file_name,source_schema_version,total_rows,staged_rows,load_status from meta.file_manifest order by file_name')
    assert {r['file_name']:r['staged_rows'] for r in report['manifest']}=={
        'yellow_tripdata_2021-01.parquet':1369769, 'yellow_tripdata_2023-02.parquet':2913955}
    assert all(r['total_rows']==r['staged_rows'] and r['load_status']=='SUCCESS' for r in report['manifest'])
    assert sum(x['rows'] for x in report['projection'])==4283724 and report['rejects']==0
    assert report['dimensions']['RateCode']==7
    assert report['rate99_flags']['rate99_rows']==report['rate99_flags']['unmapped']
    report['literal_nulls'] = query(r"""select count(*) as rows from staging.raw_yellow_trips
    where passenger_count=E'\\N' or rate_code_id=E'\\N' or store_and_fwd_flag=E'\\N'""")[0]['rows']
    assert report['literal_nulls']==0
report['status']='PASS'
(out/'summary.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str))
print(json.dumps(report,ensure_ascii=False,indent=2,default=str),flush=True)
conn.close()

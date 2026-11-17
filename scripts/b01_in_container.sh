#!/bin/bash
# B01 + B02 (nbinh): chay ben trong container lab-mage
#   B01: stg_yellow_trips (ep kieu, quarantine)  |  B02: stg_trips_flagged (co DQ)
cd /home/src/dbt_project || exit 1
P="--profiles-dir ."
MOCK='{b01_use_mock: true}'
SEL="staging seed_vendor_mapping seed_rate_code_mapping"
step() { echo; echo "### $1"; shift; "$@"; echo "[exit=$?]"; }

# --full-refresh: tao lai bang seed de nhan cot moi trong CSV (dbt seed khong tu ALTER bang cu)
step "1. dbt seed --full-refresh (8 seed: gia lap, ky vong, mapping vendor/rate)" dbt seed --full-refresh $P
step "2. MOCK - dbt run staging (B01 + B02)"  dbt run  --select staging --vars "$MOCK" $P
step "3. MOCK - dbt test staging + seed mapping" dbt test --select $SEL --vars "$MOCK" $P
step "4. THAT - dbt run staging (tro view ve staging.raw_yellow_trips)" dbt run --select staging $P
step "5. THAT - dbt test staging + seed mapping" dbt test --select $SEL $P
step "6. THAT - ghi dong loi vao staging.quarantine" dbt run-operation load_quarantine --args "{prune_stale: true}" $P

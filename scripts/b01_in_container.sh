#!/bin/bash
# B01 (nbinh): chay ben trong container lab-mage
cd /home/src/dbt_project || exit 1
P="--profiles-dir ."
MOCK='{b01_use_mock: true}'
step() { echo; echo "### $1"; shift; "$@"; echo "[exit=$?]"; }

# --full-refresh: tao lai bang seed de nhan cot moi trong CSV (dbt seed khong tu ALTER bang cu)
step "B01-1. dbt seed --full-refresh (4 seed gia lap vao schema staging)" dbt seed --full-refresh $P
step "B01-2. MOCK - dbt run staging"  dbt run  --select staging --vars "$MOCK" $P
step "B01-3. MOCK - dbt test staging" dbt test --select staging --vars "$MOCK" $P
step "B01-4. THAT - dbt run staging (tro view ve staging.raw_yellow_trips)" dbt run --select staging $P
step "B01-5. THAT - dbt test staging" dbt test --select staging $P
step "B01-6. THAT - ghi dong loi vao staging.quarantine" dbt run-operation load_quarantine --args "{prune_stale: true}" $P

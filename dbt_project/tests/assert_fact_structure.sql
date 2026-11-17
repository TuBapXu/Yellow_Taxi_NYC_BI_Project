-- Fact remains the T01 physical table; no T02 load is implied by this test.
with expected(name, type, required) as (values
 ('TripKey', 'character(32)', true),
 ('PickupDateKey', 'integer', true),
 ('DropoffDateKey', 'integer', true),
 ('PickupTimeKey', 'integer', true),
 ('DropoffTimeKey', 'integer', true),
 ('PickupZoneKey', 'integer', true),
 ('DropoffZoneKey', 'integer', true),
 ('VendorKey', 'integer', true),
 ('PaymentTypeKey', 'integer', true),
 ('RateCodeKey', 'integer', true),
 ('PassengerGroupKey', 'integer', true),
 ('DistanceBandKey', 'integer', true),
 ('PickupDateTime', 'timestamp without time zone', false),
 ('DropoffDateTime', 'timestamp without time zone', false),
 ('StoreAndFwdFlag', 'character varying(1)', false),
 ('TripCount', 'integer', true),
 ('PassengerCount', 'integer', false),
 ('TripDistanceMiles', 'numeric(18,6)', false),
 ('TripDurationSeconds', 'numeric(20,6)', false),
 ('FareAmount', 'numeric(18,2)', false),
 ('Extra', 'numeric(18,2)', false),
 ('MtaTax', 'numeric(18,2)', false),
 ('TipAmount', 'numeric(18,2)', false),
 ('TollsAmount', 'numeric(18,2)', false),
 ('ImprovementSurcharge', 'numeric(18,2)', false),
 ('CongestionSurcharge', 'numeric(18,2)', false),
 ('AirportFee', 'numeric(18,2)', false),
 ('TotalAmount', 'numeric(18,2)', false),
 ('IsTimeUsable', 'boolean', true),
 ('IsDistanceUsable', 'boolean', true),
 ('IsAmountUsable', 'boolean', true),
 ('HasDQIssue', 'boolean', true),
 ('DataQualityFlags', 'text', false),
 ('AttributeQualityFlags', 'text', false),
 ('QualityRuleVersion', 'character varying(32)', true),
 ('SourceFileName', 'character varying(255)', true),
 ('SourceFileSHA256', 'character(64)', true),
 ('SourceRowNumber', 'bigint', true),
 ('LoadBatchID', 'character varying(64)', true),
 ('LoadedAtUTC', 'timestamp without time zone', true)
), actual as (
 select a.attname as name, format_type(a.atttypid,a.atttypmod) as type, a.attnotnull as required
 from pg_attribute a where a.attrelid = 'dds."FactTaxiTrip"'::regclass
 and a.attnum > 0 and not a.attisdropped
), differences as (
 (select * from expected except select * from actual)
 union all
 (select * from actual except select * from expected)
)
select 'column:' || name as failure from differences
union all
select 'foreign_keys' where (select count(*) from pg_constraint
 where conrelid='dds."FactTaxiTrip"'::regclass and contype='f' and convalidated) <> 11
union all
select 'primary_key' where not exists (select 1 from pg_constraint
 where conrelid='dds."FactTaxiTrip"'::regclass and contype='p'
 and pg_get_constraintdef(oid) = 'PRIMARY KEY ("TripKey")')
union all
select 'lineage_unique' where not exists (select 1 from pg_constraint
 where conrelid='dds."FactTaxiTrip"'::regclass and contype='u'
 and pg_get_constraintdef(oid) = 'UNIQUE ("SourceFileSHA256", "SourceRowNumber")')
union all
select 'fk_default:' || column_name from information_schema.columns
where table_schema='dds' and table_name='FactTaxiTrip' and column_name like '%Key'
and column_name <> 'TripKey' and column_default is distinct from '0'

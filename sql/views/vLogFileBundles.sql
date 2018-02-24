create or replace view SBA.vLogFileBundles as 
with data as (
  select *, split(regexp_extract(lower(path), '\\d{6}_[a-z\\-]+_[a-z]{2,7}_\\d{14}'), '_') as BundleCodeArr
  from `bigqueryavb.SBA.logfiles` f
)
select * except(BundleCodeArr),
  BundleCodeArr[offset(0)] as Raid,
  BundleCodeArr[offset(1)] as User,
  BundleCodeArr[offset(2)] as Env,
  BundleCodeArr[offset(3)] as Timestmp
from data
;
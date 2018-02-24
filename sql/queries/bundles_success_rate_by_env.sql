select env, count(1) attempts, sum(if(t.NumOfLogFiles = t.NumOfSuccesses, 1, 0)) success
from (
  select raid, env, timestmp, count(1) NumOfLogFiles, count(case when Success then 1 end) NumOfSuccesses
  from SBA.vSbaLogFileStatuses
  group by raid, env, timestmp
) t
group by env

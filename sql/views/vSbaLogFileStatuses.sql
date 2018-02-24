create or replace view SBA.vSbaLogFileStatuses as
select b.id, b.sys, b.path, b.LastModifiedDt, b.Size, b.Raid, b.User, b.Env, b.Timestmp, count(l.fileid) > 0 as Success
from `bigqueryavb.SBA.vLogFileBundles` b
  left join `bigqueryavb.SBA.loglines` l on b.id = l.fileid and l.linetext like '%System exit requested. Closing application % with Errorlevel=0%'
group by b.id, b.sys, b.path, b.LastModifiedDt, b.Size, b.Raid, b.User, b.Env, b.Timestmp

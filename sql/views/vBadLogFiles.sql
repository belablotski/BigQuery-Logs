create or replace view SBA.vBadLogFiles as
select b.id, b.sys, b.path, b.LastModifiedDt, b.Size, b.Raid, b.User, b.Env, b.Timestmp
from `bigqueryavb.SBA.vLogFileBundles` b
  left join `bigqueryavb.SBA.loglines` l on b.id = l.fileid and l.linetext like '%Errorlevel=%'
group by b.id, b.sys, b.path, b.LastModifiedDt, b.Size, b.Raid, b.User, b.Env, b.Timestmp
having count(l.fileid) = 0

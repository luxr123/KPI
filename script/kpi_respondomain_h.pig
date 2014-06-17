--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 20;
SET job.name 'KPI';


------------------------------------------------------------------------------------------------
------------------------------------- Respondents domain ---------------------------------------
------------------------------------------------------------------------------------------------
--     load the filter data
A = load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, referer:chararray, domain:chararray, 
                                             jobPage:chararray, vtime:chararray, respondomain:chararray, new:chararray, enter:chararray);

register ../lib/kpi.jar
define myConcat com.jobs.pig.UDF.ConcatUDF;

--     account the domain pv count
A1               =  foreach A generate wid, domain, respondomain;
--    store respondomain PV into hbase
groupRespondomain           =  group A1 by myConcat($time, domain, respondomain);
respondomain_pv_count       =  foreach groupRespondomain generate flatten($0), COUNT($1) as count;
STORE respondomain_pv_count INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv');


--    store respondomain UV into hbase
respondomain_uv_count       =  foreach groupRespondomain {uid = A.wid; uniq_wid = distinct uid; generate flatten(group), COUNT(uniq_wid);};
STORE respondomain_uv_count INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');

--    compute the bounce rate
groupRespondomainWid        =  group A1 by (domain, wid, respondomain);
respondomainWidCount        =  foreach groupRespondomainWid generate $0.domain as domain, $0.respondomain as respondomain, COUNT($1) as count;
filterOneCount              =  filter respondomainWidCount by count == 1;
groupOneCount               =  group filterOneCount by myConcat($time, domain, respondomain);
respondomain_bounce_count   =  foreach groupOneCount generate $0, COUNT($1) as count;                          -- respondomain bounce count
joinRespondomainBounceCount =  join  respondomain_pv_count by group left outer, respondomain_bounce_count by group;
respondomain_bounce_rate    =  foreach joinRespondomainBounceCount {pvCount = respondomain_pv_count::count;  
                                    respondomainBounceCount = ((respondomain_bounce_count::count is null) ? 0 : respondomain_bounce_count::count);
                                    generate flatten(respondomain_pv_count::group), CONCAT((chararray)((float)respondomainBounceCount/pvCount * 100),'%');};
STORE respondomain_bounce_rate INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');


--  account respondomain stay average time
B = load '$inputPT' using PigStorage('`') as (page:chararray, referer:chararray, domain:chararray, page_avg_time:long, respondomain:chararray);
B1 = foreach B generate domain, page_avg_time, respondomain;
groupRespondomainPT         =  group B1 by myConcat($time, domain, respondomain);
respondomain_avg_times      =  foreach groupRespondomainPT generate flatten($0), myTime(SUM($1.page_avg_time)/COUNT($1));
STORE respondomain_avg_times INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avg_time');



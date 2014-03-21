--
-- File: kpi_h.pig (Xiaorui Lu)
--
--------------------------------------------------------------------------------------------------------------------------------------------------------
---------------域名下的pv, uv, ip, 平均访问时长, (包括新老访客); 以及--忠诚度 ----------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------
SET job.priority VERY_HIGH;
SET default_parallel 20;
SET job.name 'KPI_DOMAIN_H';

REGISTER ../lib/kpi.jar
DEFINE myConcat       com.jobs.pig.udf.ConcatUDF;
DEFINE myFilterCount  com.jobs.pig.udf.FilterCountUDF;
DEFINE myTime         com.jobs.pig.udf.FormatTimeUDF;
------------------------------------------------------------------------------------------------
--------------------------------------------- Domain -------------------------------------------
------------------------------------------------------------------------------------------------
--     load the filter data
A  =  load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, referer:chararray, domain:chararray, respondomain:chararray,
                                                                                                    enter:chararray, new:int, se:chararray, wb:int, kw:chararray);
--  account page stay average duration

--     account the domain pv count
A1                           =  foreach A generate ip, wid, domain, new;

------------------domain 下的pv uv, ip 包括新老访客--------------------------------------------------------------
groupDomainPU                =  group A1 by domain;

domain_pu_count              =  foreach groupDomainPU { FA = filter $1 by new is null; retIP = distinct FA.ip; retWid = distinct FA.wid;
                                                                                          ip = distinct $1.ip;    wid = distinct $1.wid;
                                    retpv = COUNT(FA); retuv = COUNT(retWid); retip = COUNT(retIP);
                                       pv = COUNT($1);    uv = COUNT(wid);       ip = COUNT(ip);
                                generate myConcat($time, $0) as domain, 
                                                                      pv as pv,              uv as uv,              ip as ip,
                                                                   retpv as retpv,        retuv as retuv,        retip as retip,
                                                            (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

--    compute the domain bounce rate
groupDomainWid               =  group A1 by (domain, wid);
filterOneCount               =  filter groupDomainWid by myFilterCount(A1, 1);
filterOneCountAdv            =  foreach filterOneCount generate $0.domain as domain, flatten($1.new) as new;
groupOneCount                =  group filterOneCountAdv by domain;
one_page_count               =  foreach groupOneCount { FA = filter $1 by new is null; retcount = COUNT(FA); count = COUNT($1);
                                generate myConcat($time, $0) as domain,
                                                                count as count, retcount as retcount, (count - retcount) as newcount;};

joinDomainBounceCount        =  join one_page_count by domain, domain_pu_count by domain;                      -- compute domain bounce rate/pv
domain_bounce_rate           =  foreach joinDomainBounceCount {    pv = domain_pu_count::pv;       bc = one_page_count::count;
                                                                retpv = domain_pu_count::retpv; retbc = one_page_count::retcount;
                                                                newpv = domain_pu_count::newpv; newbc = one_page_count::newcount;
                                generate one_page_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                 CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                 CONCAT((chararray)((float)newbc/newpv * 100),'%');};

STORE domain_pu_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');
--    store Domain bounce rate into hbase
STORE domain_bounce_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');


----------------------------------------------------平均访问时长---------------------------------
--  account page stay average duration
--------------------------------------------
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, ref:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, enter:chararray,
                                                                                                                     new:int, se:chararray, wb:int, kw:chararray);
B1                          =  foreach B generate domain, page_avg_time, new;
groupDoaminPT               =  group B1 by domain;
domain_avg_times            =  foreach groupDoaminPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0) as domain,
                                           myTime(AVG($1.page_avg_time)) as avgtime,
                                           myTime(AVG(FBRet.page_avg_time)) as retavgtime,
                                           myTime(AVG(FBNew.page_avg_time)) as newavgtime;};

STORE domain_avg_times INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');




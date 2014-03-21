--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 20;
SET job.name 'KPI_SOURCE_ANALYSIS';

REGISTER ../lib/kpi.jar
DEFINE myConcat       com.jobs.pig.UDF.ConcatUDF;
DEFINE myFilterCount  com.jobs.pig.UDF.FilterCountUDF;
DEFINE myTime         com.jobs.pig.UDF.FormatTimeUDF;
DEFINE mySubString    com.jobs.pig.UDF.SubString;
------------------------------------------------------------------------------------------------
--------------------------------------------- Domain -------------------------------------------
------------------------------------------------------------------------------------------------
--     load the filter data
A  =  load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, ref:chararray, domain:chararray, respondomain:chararray,
                                                                                                    new:int, enter:chararray, se:chararray, wb:int, kw:chararray);
--  account page stay average duration
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, new:int, se:chararray, wb:int, 
                                                                                                                                      kw:chararray, ref:chararray);

------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------来源分析-------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------




--4----------------------------------------搜素词----------------------------------------------------------
-----------
--  pv uv
-----------
filterAbyKW              =  filter A by kw is not null;
A4                       =  foreach filterAbyKW generate ip, wid, domain, new, kw;
--  total kw of domain
groupKWTotalPU           =  group A4 by domain;
kw_total_pu              =  foreach groupKWTotalPU { FA = filter $1 by new is null; retIP = distinct FA.ip; retWid = distinct FA.wid;
                                                                                       ip = distinct $1.ip;    wid = distinct $1.wid;
                                    retpv = COUNT(FA); retuv = COUNT(retWid); retip = COUNT(retIP);
                                       pv = COUNT($1);    uv = COUNT(wid);       ip = COUNT(ip);
                                generate myConcat($time, $0, '') as domain, 
                                                                      pv as pv,              uv as uv,              ip as ip,
                                                                   retpv as retpv,        retuv as retuv,        retip as retip,
                                                            (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

groupKWPU                =  group A4 by (domain, kw);
kw_pu                    =  foreach groupKWPU { FA = filter $1 by new is null; retIP = distinct FA.ip; retWid = distinct FA.wid;
                                                                                  ip = distinct $1.ip;    wid = distinct $1.wid;
                                    retpv = COUNT(FA); retuv = COUNT(retWid); retip = COUNT(retIP);
                                       pv = COUNT($1);    uv = COUNT(wid);       ip = COUNT(ip);
                                generate myConcat($time, $0.domain, $0.kw) as domain, 
                                                                        pv as pv,              uv as uv,              ip as ip,
                                                                     retpv as retpv,        retuv as retuv,        retip as retip,
                                                              (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

--------------
-- bounce rate
-------------
--  one floor
--  one floor
groupKWWid               =  group A4 by (wid, domain, kw);
filterKWWid              =  filter groupKWWid by myFilterCount($1, 1);
filterKWWidAdv           =  foreach filterKWWid generate flatten($0), flatten($1.new) as new;
--  total kw
groupKWTotalWid          =  group filterKWWidAdv by (group::wid, group::domain);
filterKWTotalWid         =  filter groupKWTotalWid by myFilterCount($1, 1);
filterKWTotalWidAdv      =  foreach filterKWTotalWid generate $0.group::domain as domain, flatten($1.new) as new;

--  one floor
groupKWOne               =  group filterKWWidAdv by (group::domain, group::kw);
kw_one_count             =  foreach groupKWOne { FA = filter $1 by new is null; retcount = COUNT(FA); count = COUNT($1);
                                generate myConcat($time, $0.group::domain, $0.group::kw) as domain,
                                                                count as count, retcount as retcount, (count - retcount) as newcount;};

joinKWBounce             =  join kw_one_count by domain, kw_pu by domain;
kw_bounce_rate           =  foreach joinKWBounce {    pv = kw_pu::pv;       bc = kw_one_count::count;
                                                   retpv = kw_pu::retpv; retbc = kw_one_count::retcount;
                                                   newpv = kw_pu::newpv; newbc = kw_one_count::newcount;
                                generate kw_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                               CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                               CONCAT((chararray)((float)newbc/newpv * 100),'%');}; 

--  total kw of domain
groupKWTotalOne          =  group filterKWTotalWidAdv by domain;
kw_total_one_count       =  foreach groupKWTotalOne { FA = filter $1 by new is null; retcount = COUNT(FA); count = COUNT($1);
                                generate myConcat($time, $0, '') as domain,
                                                                count as count, retcount as retcount, (count - retcount) as newcount;};

joinKWTotalBounce        =  join kw_total_one_count by domain, kw_total_pu by domain;
kw_total_bounce_rate     =  foreach joinKWTotalBounce {    pv = kw_total_pu::pv;       bc = kw_total_one_count::count;
                                                        retpv = kw_total_pu::retpv; retbc = kw_total_one_count::retcount;
                                                        newpv = kw_total_pu::newpv; nekwc = kw_total_one_count::newcount;
                                generate kw_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                     CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                     CONCAT((chararray)((float)nekwc/newpv * 100),'%');};

--------------
-- average time
--------------

filterBbyKW              =  filter B by kw is not null;
B4                       =  foreach filterBbyKW generate domain, page_avg_time, new, kw;

--  one floor
groupKWPT                =  group B4 by (domain, kw);
kw_avgtime               =  foreach groupKWPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0.domain, $0.kw) as domain,
                                           myTime(AVG($1.page_avg_time))     as avgtime,
                                           myTime(AVG(FBRet.page_avg_time))  as retavgtime,
                                           myTime(AVG(FBNew.page_avg_time))  as newavgtime;};
                
-- total
groupKWTotalPT           =  group B4 by domain;
kw_total_avgtime         =  foreach groupKWTotalPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0, '')          as domain,
                                           myTime(AVG($1.page_avg_time))    as avgtime,
                                           myTime(AVG(FBRet.page_avg_time)) as retavgtime,
                                           myTime(AVG(FBNew.page_avg_time)) as newavgtime;};

----   入库hbase
STORE kw_pu INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');
STORE kw_total_pu INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE kw_bounce_rate INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE kw_total_bounce_rate INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');

STORE kw_avgtime INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');
STORE kw_total_avgtime INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');



--5----------------------------------------直接访问----------------------------------------------------------
-----------
--  pv uv
-----------
filterAbyRef              =  filter A by ref is not null;
A5                        =  foreach filterAbyRef generate ip, wid, domain, new;
--  total ref domain
groupRefTotalPU           =  group A5 by domain;
ref_total_pu              =  foreach groupRefTotalPU { FA = filter $1 by new is null; retIP = distinct FA.ip; retWid = distinct FA.wid;
                                                                                         ip = distinct $1.ip;    wid = distinct $1.wid;
                                    retpv = COUNT(FA); retuv = COUNT(retWid); retip = COUNT(retIP);
                                       pv = COUNT($1);    uv = COUNT(wid);       ip = COUNT(ip);
                                generate myConcat($time, $0) as domain, 
                                                                      pv as pv,              uv as uv,              ip as ip,
                                                                   retpv as retpv,        retuv as retuv,        retip as retip,
                                                            (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

--------------
-- bounce rate
-------------

--  total ref
groupRefTotalWid          =  group A5 by (wid, domain);
filterRefTotalWid         =  filter groupRefTotalWid by myFilterCount($1, 1);
filterRefTotalWidAdv      =  foreach filterRefTotalWid generate $0.domain as domain, flatten($1.new) as new;

--  total bounce rate
groupRefTotalOne          =  group filterRefTotalWidAdv by domain;
ref_total_one_count       =  foreach groupRefTotalOne { FA = filter $1 by new is null; retcount = COUNT(FA); count = COUNT($1);
                                generate myConcat($time, $0) as domain,
                                                                count as count, retcount as retcount, (count - retcount) as newcount;};

joinRefTotalBounce        =  join ref_total_one_count by domain, ref_total_pu by domain;
ref_total_bounce_rate     =  foreach joinRefTotalBounce {    pv = ref_total_pu::pv;       bc = ref_total_one_count::count;
                                                          retpv = ref_total_pu::retpv; retbc = ref_total_one_count::retcount;
                                                          newpv = ref_total_pu::newpv; nekwc = ref_total_one_count::newcount;
                                generate ref_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                      CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                      CONCAT((chararray)((float)nekwc/newpv * 100),'%');};

--------------
-- average time
--------------
filterBbyRef             =  filter B by ref is not null;
B5                       =  foreach filterBbyRef generate domain, page_avg_time, new;

-- total
groupRefTotalPT           =  group B5 by domain;
ref_total_avgtime         =  foreach groupRefTotalPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0) as domain,
                                           myTime(AVG($1.page_avg_time))    as avgtime,
                                           myTime(AVG(FBRet.page_avg_time)) as retavgtime,
                                           myTime(AVG(FBNew.page_avg_time)) as newavgtime;};


STORE ref_total_pu INTO 'hbase://kpi_direct_access' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE ref_total_bounce_rate INTO 'hbase://kpi_direct_access' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');

STORE ref_total_avgtime INTO 'hbase://kpi_direct_access' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');




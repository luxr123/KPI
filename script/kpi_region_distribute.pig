--
-- File: kpi_h.pig (Xiaorui Lu)
--
--------------------------------------------------------------------------------------------------------------------------------------------------------
---------------域名下的pv, uv, ip, 平均访问时长, (包括新老访客); 以及--忠诚度 ----------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------
SET job.priority VERY_HIGH;
SET default_parallel 10;
SET job.name 'KPI_REGION_DISTRIBUTE_D';

REGISTER ../lib/kpi.jar
DEFINE myConcat       com.jobs.pig.udf.ConcatUDF;
DEFINE myFilterCount  com.jobs.pig.udf.FilterCountUDF;
DEFINE myFloat        com.jobs.pig.udf.MyFloat;
------------------------------------------------------------------------------------------------
--------------------------------------------- Domain -------------------------------------------
------------------------------------------------------------------------------------------------
--     load the filter data
A  =  load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, referer:chararray, domain:chararray, respondomain:chararray,
                                                                            enter:chararray, new:int, se:chararray, wb:int, kw:chararray, vtime:chararray, 
                                                                                                                        province:chararray, city:chararray);

A1                           =  foreach A generate ip, wid, domain, new, province, city;

------------------domain 下的按地级市  (省\市) --------------------------------------------------------------
-------------  pv \ uv
--省(直辖市)
groupProvincePU              =  group A1 by (domain, province);

province_pu                  =  foreach groupProvincePU { FA = filter $1 by new is null; retIP = distinct FA.ip; retWid = distinct FA.wid;
                                                                                            ip = distinct $1.ip;    wid = distinct $1.wid;
                                                                                          retpv = COUNT(FA);      retuv = COUNT(retWid); retip = COUNT(retIP);
                                                                                             pv = COUNT($1);         uv = COUNT(wid);       ip = COUNT(ip);
                                    generate myConcat($time, $0.domain, $0.province, '') as domain, 
                                                                          pv as pv,              uv as uv,              ip as ip,
                                                                       retpv as retpv,        retuv as retuv,        retip as retip,
                                                                (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

-- 市(区)
A2                          =  filter A1 by city is not null;
groupCityPU                 =  group A2 by (domain, province, city);

city_pu                     =  foreach groupCityPU { FA = filter $1 by new is null; retIP = distinct FA.ip; retWid = distinct FA.wid;
                                                                                       ip = distinct $1.ip;    wid = distinct $1.wid;
                                                                                    retpv = COUNT(FA);       retuv = COUNT(retWid); retip = COUNT(retIP);
                                                                                       pv = COUNT($1);          uv = COUNT(wid);       ip = COUNT(ip);
                                    generate myConcat($time, $0.domain, $0.province, $0.city) as domain,
                                                                          pv as pv,              uv as uv,              ip as ip,
                                                                       retpv as retpv,        retuv as retuv,        retip as retip,
                                                                (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

-------- 跳出率
--省(直辖市)
groupProvinceWid             =  group A1 by (domain, wid, province);
filterProvinceOne            =  filter groupProvinceWid by myFilterCount(A1, 1);
filterProvinceOneAdv         =  foreach filterProvinceOne generate $0.domain as domain, $0.province as province, flatten($1.new) as new;
groupProvinceOne             =  group filterProvinceOneAdv by (domain, province);
province_bounce_count        =  foreach groupProvinceOne { FA = filter $1 by new is null; retcount = COUNT(FA); count = COUNT($1);
                                    generate myConcat($time, $0.domain, $0.province, '') as domain,
                                                                count as count, retcount as retcount, (count - retcount) as newcount;};

joinProvinceBounce           =  join province_bounce_count by domain, province_pu by domain;
province_bounce_rate         =  foreach joinProvinceBounce {    pv = province_pu::pv;       bc = province_bounce_count::count;
                                                             retpv = province_pu::retpv; retbc = province_bounce_count::retcount;
                                                             newpv = province_pu::newpv; newbc = province_bounce_count::newcount;
                                    generate province_bounce_count::domain, myFloat((float)bc/pv),
                                                                            myFloat((float)retbc/retpv),
                                                                            myFloat((float)newbc/newpv);};
-- 市(区)
groupCityWid             =  group A2 by (domain, wid, province, city);
filterCityOne            =  filter groupCityWid by myFilterCount(A2, 1);
filterCityOneAdv         =  foreach filterCityOne generate $0.domain as domain, $0.province as province, $0.city as city, flatten($1.new) as new;
groupCityOne             =  group filterCityOneAdv by (domain, province, city);
city_bounce_count        =  foreach groupCityOne { FA = filter $1 by new is null; retcount = COUNT(FA); count = COUNT($1);
                                    generate myConcat($time, $0.domain, $0.province, $0.city) as domain,
                                                                count as count, retcount as retcount, (count - retcount) as newcount;};

joinCityBounce           =  join city_bounce_count by domain, city_pu by domain;
city_bounce_rate         =  foreach joinCityBounce {    pv = city_pu::pv;       bc = city_bounce_count::count;
                                                     retpv = city_pu::retpv; retbc = city_bounce_count::retcount;
                                                     newpv = city_pu::newpv; newbc = city_bounce_count::newcount;
                                    generate city_bounce_count::domain, myFloat((float)bc/pv),
                                                                        myFloat((float)retbc/retpv),
                                                                        myFloat((float)newbc/newpv);};

STORE province_pu INTO 'hbase://kpi_region_distribute' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');
STORE city_pu INTO 'hbase://kpi_region_distribute' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');
--    store Domain bounce rate into hbase
STORE province_bounce_rate INTO 'hbase://kpi_region_distribute' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE city_bounce_rate INTO 'hbase://kpi_region_distribute' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');


----------------------------------------------------平均访问时长---------------------------------
--  account page stay average duration
--------------------------------------------
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, ref:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, enter:chararray,
                                                                                        new:int, se:chararray, wb:int, kw:chararray, province:chararray, city:chararray);
B1                         =  foreach B generate domain, page_avg_time, new, province, city;
groupProvincePT            =  group B1 by (domain, province);
province_avg_times         =  foreach groupProvincePT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                   generate myConcat($time, $0.domain, $0.province, ''),
                                            (long) AVG($1.page_avg_time),
                                            (long) AVG(FBRet.page_avg_time),
                                            (long) AVG(FBNew.page_avg_time);};

B2                         =  filter B1 by city is not null;
groupCityPT                =  group B2 by (domain, province, city);
city_avg_times             =  foreach groupCityPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                    generate myConcat($time, $0.domain, $0.province, $0.city),
                                            (long) AVG($1.page_avg_time),
                                            (long) AVG(FBRet.page_avg_time),
                                            (long) AVG(FBNew.page_avg_time);}; 

STORE province_avg_times INTO 'hbase://kpi_region_distribute' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');
STORE city_avg_times INTO 'hbase://kpi_region_distribute' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');




--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 20;
SET job.name 'KPI_SOURCE_ANALYSIS';

REGISTER ../lib/kpi.jar
DEFINE myConcat       com.jobs.pig.udf.ConcatUDF;
DEFINE myFilterCount  com.jobs.pig.udf.FilterCountUDF;
DEFINE myTime         com.jobs.pig.udf.FormatTimeUDF;
DEFINE mySubString    com.jobs.pig.udf.SubString;
------------------------------------------------------------------------------------------------
--------------------------------------------- Domain -------------------------------------------
------------------------------------------------------------------------------------------------
--     load the filter data
A  =  load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, ref:chararray, domain:chararray, respondomain:chararray,
                                                                             enter:chararray, new:int, se:chararray, wb:int, kw:chararray, vtime:chararray,
                                                                                                         province:chararray, city:chararray, area:chararray);
--  account page stay average duration
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, ref:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, enter:chararray,
                                                                new:int, se:chararray, wb:int, kw:chararray, province:chararray, city:chararray, area:chararray);

------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------来源分析-------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------搜索引擎------------------------------------------------
-- pv uv
filterAbySE              =  filter A by se is not null;
A2                       =  foreach filterAbySE generate ip, wid, domain, new, se, province, city, area;
filterSSCity             =  filter A2 by city is not null;
--  total se of domain
groupSSTotalPU           =  group A2 by domain;
ss_total_pu              =  foreach groupSSTotalPU {      FA = filter $1 by new is null;   FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                     otherIP = distinct FO.ip;       otherWid = distinct FO.wid;
                                                  otherRetIP = distinct FORet.ip; otherRetWid = distinct FORet.wid;
                                                       retIP = distinct FA.ip;         retWid = distinct FA.wid;
                                                          ip = distinct $1.ip;            wid = distinct $1.wid; 
                                 otherpv = COUNT(FO);              otheruv = COUNT(otherWid);        otherip = COUNT(otherIP);
                               otheretpv = COUNT(FORet);         otheretuv = COUNT(otherRetWid);   otheretip = COUNT(otherRetIP);
                              othernewpv = otherpv - otheretpv; othernewuv = otheruv - otheretuv; othernewip = otherip - otheretip;
                                      pv = COUNT($1);                   uv = COUNT(wid);                  ip = COUNT(ip);
                                   retpv = COUNT(FA);                retuv = COUNT(retWid);            retip = COUNT(retIP);
                                   newpv = pv - retpv;               newuv = uv - retuv;               newip = ip - retip;
                                   generate myConcat($time, $0, '','') as domain, 
                                                                    pv as pv,                         uv as uv,                         ip as ip, 
                                                                 retpv as retpv,                   retuv as retuv,                   retip as retip,
                                                                 newpv as newpv,                   newuv as newuv,                   newip as newip,
                                                               otherpv as otherpv,               otheruv as otheruv,               otherip as otherip,
                                                             otheretpv as otheretpv,           otheretuv as otheretuv,           otheretip as otheretip,
                                                            othernewpv as othernewpv,         othernewuv as othernewuv,         othernewip as othernewip,
                                                        (pv - otherpv) as chinapv,        (uv - otheruv) as chinauv,        (ip - otherip) as chinaip,
                                                     (retpv-otheretpv) as chinaretpv,  (retuv-otheretuv) as chinaretuv,  (retip-otheretip) as chinaretip,
                                                    (newpv-othernewpv) as chinanewpv, (newuv-othernewuv) as chinanewuv, (newip-othernewip) as chinanewip ;};

--  one floor
groupSSPU                =  group A2 by (domain, mySubString(se));
ss_pu                    =  foreach groupSSPU {      FA = filter $1 by new is null; FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                     otherIP = distinct FO.ip;       otherWid = distinct FO.wid;
                                                  otherRetIP = distinct FORet.ip; otherRetWid = distinct FORet.wid;
                                                       retIP = distinct FA.ip;         retWid = distinct FA.wid;
                                                          ip = distinct $1.ip;            wid = distinct $1.wid;
                                 otherpv = COUNT(FO);              otheruv = COUNT(otherWid);        otherip = COUNT(otherIP);
                               otheretpv = COUNT(FORet);         otheretuv = COUNT(otherRetWid);   otheretip = COUNT(otherRetIP);
                              othernewpv = otherpv - otheretpv; othernewuv = otheruv - otheretuv; othernewip = otherip - otheretip;
                                      pv = COUNT($1);                   uv = COUNT(wid);                  ip = COUNT(ip);
                                   retpv = COUNT(FA);                retuv = COUNT(retWid);            retip = COUNT(retIP);
                                   newpv = pv - retpv;               newuv = uv - retuv;               newip = ip - retip;
                                   generate myConcat($time, $0.domain, $0.subString, '') as domain,
                                                                    pv as pv,                         uv as uv,                         ip as ip,
                                                                 retpv as retpv,                   retuv as retuv,                   retip as retip,
                                                                 newpv as newpv,                   newuv as newuv,                   newip as newip,
                                                               otherpv as otherpv,               otheruv as otheruv,               otherip as otherip,
                                                             otheretpv as otheretpv,           otheretuv as otheretuv,           otheretip as otheretip,
                                                            othernewpv as othernewpv,         othernewuv as othernewuv,         othernewip as othernewip,
                                                        (pv - otherpv) as chinapv,        (uv - otheruv) as chinauv,        (ip - otherip) as chinaip,
                                                     (retpv-otheretpv) as chinaretpv,  (retuv-otheretuv) as chinaretuv,  (retip-otheretip) as chinaretip,
                                                    (newpv-othernewpv) as chinanewpv, (newuv-othernewuv) as chinanewuv, (newip-othernewip) as chinanewip ;};      

-- second floor
groupSubSSPU             =  group A2 by (domain, se);
sub_ss_pu                =  foreach groupSubSSPU {      FA = filter $1 by new is null; FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                     otherIP = distinct FO.ip;       otherWid = distinct FO.wid;
                                                  otherRetIP = distinct FORet.ip; otherRetWid = distinct FORet.wid;
                                                       retIP = distinct FA.ip;         retWid = distinct FA.wid;
                                                          ip = distinct $1.ip;            wid = distinct $1.wid;
                                 otherpv = COUNT(FO);              otheruv = COUNT(otherWid);        otherip = COUNT(otherIP);
                               otheretpv = COUNT(FORet);         otheretuv = COUNT(otherRetWid);   otheretip = COUNT(otherRetIP);
                              othernewpv = otherpv - otheretpv; othernewuv = otheruv - otheretuv; othernewip = otherip - otheretip;
                                      pv = COUNT($1);                   uv = COUNT(wid);                  ip = COUNT(ip);
                                   retpv = COUNT(FA);                retuv = COUNT(retWid);            retip = COUNT(retIP);
                                   newpv = pv - retpv;               newuv = uv - retuv;               newip = ip - retip;
                                   generate myConcat($time, $0.domain, mySubString($0.se), $0.se) as domain,
                                                                    pv as pv,                         uv as uv,                         ip as ip,
                                                                 retpv as retpv,                   retuv as retuv,                   retip as retip,
                                                                 newpv as newpv,                   newuv as newuv,                   newip as newip,
                                                               otherpv as otherpv,               otheruv as otheruv,               otherip as otherip,
                                                             otheretpv as otheretpv,           otheretuv as otheretuv,           otheretip as otheretip,
                                                            othernewpv as othernewpv,         othernewuv as othernewuv,         othernewip as othernewip,
                                                        (pv - otherpv) as chinapv,        (uv - otheruv) as chinauv,        (ip - otherip) as chinaip,
                                                     (retpv-otheretpv) as chinaretpv,  (retuv-otheretuv) as chinaretuv,  (retip-otheretip) as chinaretip,
                                                    (newpv-othernewpv) as chinanewpv, (newuv-othernewuv) as chinanewuv, (newip-othernewip) as chinanewip ;};


--------------------------------------搜索引擎 按地域分布  pv uv ip -----------------------------------------------
-- 搜索引擎下的省(直辖市)全部
groupSSProvTotalPU       =  group A2 by (domain, province);
ss_prov_total_pu         =  foreach groupSSProvTotalPU {    FA = filter $1 by new is null;
                                                         retIP = distinct FA.ip;       retWid = distinct FA.wid;
                                                            ip = distinct $1.ip;          wid = distinct $1.wid; 
                                      retpv = COUNT(FA); retuv = COUNT(retWid);         retip = COUNT(retIP);
                                         pv = COUNT($1);    uv = COUNT(wid);               ip = COUNT(ip);
                                generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                                                    pv as pv,                  uv as uv,                  ip as ip, 
                                                                 retpv as retpv,            retuv as retuv,            retip as retip,
                                                          (pv - retpv) as newpv,     (uv - retuv) as newuv,     (ip - retip) as newip ;};

-- 搜索引擎下的省(直辖市) 一级域名, 如,baidu.com
groupSSProvPU            =  group A2 by (domain, mySubString(se), province);
ss_prov_pu               =  foreach groupSSProvPU {    FA = filter $1 by new is null;
                                                    retIP = distinct FA.ip;       retWid = distinct FA.wid;
                                                       ip = distinct $1.ip;          wid = distinct $1.wid; 
                                 retpv = COUNT(FA); retuv = COUNT(retWid);         retip = COUNT(retIP);
                                    pv = COUNT($1);    uv = COUNT(wid);               ip = COUNT(ip);
                                generate myConcat($time, $0.domain, $0.subString, '', $0.province, '') as domain,
                                                                    pv as pv,                  uv as uv,                  ip as ip, 
                                                                 retpv as retpv,            retuv as retuv,            retip as retip,
                                                          (pv - retpv) as newpv,     (uv - retuv) as newuv,     (ip - retip) as newip ;};

-- 搜索引擎下的省(直辖市) 二级域名, 如,www.baidu.com
groupSubSSProvPU         =  group A2 by (domain, se, province);
sub_ss_prov_pu           =  foreach groupSubSSProvPU {    FA = filter $1 by new is null;
                                                       retIP = distinct FA.ip;       retWid = distinct FA.wid;
                                                          ip = distinct $1.ip;          wid = distinct $1.wid;
                                    retpv = COUNT(FA); retuv = COUNT(retWid);         retip = COUNT(retIP);
                                       pv = COUNT($1);    uv = COUNT(wid);               ip = COUNT(ip);
                                generate myConcat($time, $0.domain, mySubString($0.se), $0.se, $0.province, '') as domain,
                                                                    pv as pv,                  uv as uv,                  ip as ip,
                                                                 retpv as retpv,            retuv as retuv,            retip as retip,
                                                          (pv - retpv) as newpv,     (uv - retuv) as newuv,     (ip - retip) as newip ;};


-- 搜索引擎下的市(直辖区)全部
groupSSCityTotalPU       =  group filterSSCity by (domain, province, city);
ss_city_total_pu         =  foreach groupSSCityTotalPU {    FA = filter $1 by new is null;
                                                         retIP = distinct FA.ip;       retWid = distinct FA.wid;
                                                            ip = distinct $1.ip;          wid = distinct $1.wid;
                                      retpv = COUNT(FA); retuv = COUNT(retWid);         retip = COUNT(retIP);
                                         pv = COUNT($1);    uv = COUNT(wid);               ip = COUNT(ip);
                                generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                                                    pv as pv,                  uv as uv,                  ip as ip,
                                                                 retpv as retpv,            retuv as retuv,            retip as retip,
                                                          (pv - retpv) as newpv,     (uv - retuv) as newuv,     (ip - retip) as newip ;};

-- 搜索引擎下的市(直辖区) 一级域名, 如,baidu.com
groupSSCityPU           =  group filterSSCity by (domain, mySubString(se), province, city);
ss_city_pu              =  foreach groupSSCityPU {    FA = filter $1 by new is null;
                                                   retIP = distinct FA.ip;       retWid = distinct FA.wid;
                                                      ip = distinct $1.ip;          wid = distinct $1.wid;
                                retpv = COUNT(FA); retuv = COUNT(retWid);         retip = COUNT(retIP);
                                   pv = COUNT($1);    uv = COUNT(wid);               ip = COUNT(ip);
                                generate myConcat($time, $0.domain, $0.subString, '', $0.province, $0.city) as domain,
                                                                    pv as pv,                  uv as uv,                  ip as ip,
                                                                 retpv as retpv,            retuv as retuv,            retip as retip,
                                                          (pv - retpv) as newpv,     (uv - retuv) as newuv,     (ip - retip) as newip ;};

-- 搜索引擎下的市(直辖区) 二级域名, 如,www.baidu.com
groupSubSSCityPU        =  group filterSSCity by (domain, se, province, city);
sub_ss_city_pu          =  foreach groupSubSSCityPU {    FA = filter $1 by new is null;
                                                      retIP = distinct FA.ip;       retWid = distinct FA.wid;
                                                         ip = distinct $1.ip;          wid = distinct $1.wid;
                                   retpv = COUNT(FA); retuv = COUNT(retWid);         retip = COUNT(retIP);
                                      pv = COUNT($1);    uv = COUNT(wid);               ip = COUNT(ip);
                                generate myConcat($time, $0.domain, mySubString($0.se), $0.se, $0.province, $0.city) as domain,
                                                                    pv as pv,                  uv as uv,                  ip as ip,
                                                                 retpv as retpv,            retuv as retuv,            retip as retip,
                                                          (pv - retpv) as newpv,     (uv - retuv) as newuv,     (ip - retip) as newip ;};

----   入库hbase
STORE sub_ss_pu INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage (
                    'cf:pv       cf:uv       cf:ip       vis:retpv       vis:retuv       vis:retip       vis:newpv       vis:newuv       vis:newip 
                     cf:other:pv cf:other:uv cf:other:ip vis:other:retpv vis:other:retuv vis:other:retip vis:other:newpv vis:other:newuv vis:other:newip 
                     cf:china:pv cf:china:uv cf:china:ip vis:china:retpv vis:china:retuv vis:china:retip vis:china:newpv vis:china:newuv vis:china:newip');

STORE ss_pu INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage (
                    'cf:pv       cf:uv       cf:ip       vis:retpv       vis:retuv       vis:retip       vis:newpv       vis:newuv       vis:newip
                     cf:other:pv cf:other:uv cf:other:ip vis:other:retpv vis:other:retuv vis:other:retip vis:other:newpv vis:other:newuv vis:other:newip
                     cf:china:pv cf:china:uv cf:china:ip vis:china:retpv vis:china:retuv vis:china:retip vis:china:newpv vis:china:newuv vis:china:newip');

STORE ss_total_pu INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage (
                    'cf:pv       cf:uv       cf:ip       vis:retpv       vis:retuv       vis:retip       vis:newpv       vis:newuv       vis:newip
                     cf:other:pv cf:other:uv cf:other:ip vis:other:retpv vis:other:retuv vis:other:retip vis:other:newpv vis:other:newuv vis:other:newip
                     cf:china:pv cf:china:uv cf:china:ip vis:china:retpv vis:china:retuv vis:china:retip vis:china:newpv vis:china:newuv vis:china:newip');

STORE ss_prov_total_pu INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE ss_prov_pu INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE sub_ss_prov_pu INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE ss_city_total_pu INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE ss_city_pu INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE sub_ss_city_pu INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

------------------------
-- bounce rate  ( 小 -> 大 )
-----------------------
--------------------------------------搜索引擎 按地域分布  bounce rate -----------------------------------------------
-- 搜索引擎下的市(直辖区) 二级域名, 如,www.baidu.com
groupSubSSCityWid        =  group filterSSCity by (wid, domain, se, province, city);
filterSubSSCityWid       =  filter groupSubSSCityWid by myFilterCount($1, 1);
filterSubSSCityWidAdv    =  foreach filterSubSSCityWid generate $0.wid as wid, $0.domain as domain, $0.se as se, $0.province as province, 
                                                                                                                        $0.city as city, flatten($1.new) as new;

-- 搜索引擎下的市(直辖区) 一级域名, 如,baidu.com
groupSSCityWid           =  group filterSubSSCityWidAdv by (wid, domain, mySubString(se), province, city);
filterSSCityWid          =  filter groupSSCityWid by myFilterCount($1, 1);
filterSSCityWidAdv       =  foreach filterSSCityWid generate $0.wid as wid, $0.domain as domain, $0.subString as parentse, $0.province as province, 
                                                                                                                        $0.city as city, flatten($1.new) as new;

-- 搜索引擎下的市(直辖区)全部
groupSSCityTotalWid      =  group filterSSCityWidAdv by (wid, domain, province, city);
filterSSCityTotalWid     =  filter groupSSCityTotalWid by myFilterCount($1, 1);
filterSSCityTotalWidAdv  =  foreach filterSSCityTotalWid generate $0.domain as domain, $0.province as province, $0.city as city, flatten($1.new) as new;


-- 搜索引擎下的市(直辖区) 二级域名, 如,www.baidu.com
groupSubSSCityOne           =  group filterSubSSCityWidAdv by (domain, se, province, city);
sub_ss_city_one_count       =  foreach groupSubSSCityOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); newbc = bc - retbc;
                                    generate myConcat($time, $0.domain, mySubString($0.se), $0.se, $0.province, $0.city) as domain,
                                                                                                        bc as count, retbc as retcount, newbc as newcount ;};
joinSubSSCityBounce         =  join sub_ss_city_one_count by domain, sub_ss_city_pu by domain;
sub_ss_city_bounce_rate     =  foreach joinSubSSCityBounce {    pv = sub_ss_city_pu::pv;       bc = sub_ss_city_one_count::count;
                                                             retpv = sub_ss_city_pu::retpv; retbc = sub_ss_city_one_count::retcount;
                                                             newpv = sub_ss_city_pu::newpv; newbc = sub_ss_city_one_count::newcount;
                                    generate sub_ss_city_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                            CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                            CONCAT((chararray)((float)newbc/newpv * 100),'%') ;};
-- 搜索引擎下的市(直辖区) 一级域名, 如,baidu.com
groupSSCityOne             =  group filterSSCityWidAdv by (domain, parentse, province, city);
ss_city_one_count          =  foreach groupSSCityOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); newbc = bc - retbc;
                                    generate myConcat($time, $0.domain, $0.parentse, '', $0.province, $0.city) as domain,
                                                                                                            bc as count, retbc as retcount, newbc as newcount ;};
joinSSCityBounce           =  join ss_city_one_count by domain, ss_city_pu by domain;
ss_city_bounce_rate        =  foreach joinSSCityBounce {    pv = ss_city_pu::pv;       bc = ss_city_one_count::count;
                                                         retpv = ss_city_pu::retpv; retbc = ss_city_one_count::retcount;
                                                         newpv = ss_city_pu::newpv; newbc = ss_city_one_count::newcount;
                                    generate ss_city_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                        CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                        CONCAT((chararray)((float)newbc/newpv * 100),'%') ;};
-- 搜索引擎下的市(直辖区)全部
groupSSCityTotalOne        =  group filterSSCityTotalWidAdv by (domain, province, city);
ss_city_total_one_count    =  foreach groupSSCityTotalOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); newbc = bc - retbc;
                                    generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                                                                                        bc as count, retbc as retcount, newbc as newcount ;};
joinSSCityTotalBounce      =  join ss_city_total_one_count by domain, ss_city_total_pu by domain;
ss_city_total_bounce_rate  =  foreach joinSSCityTotalBounce {    pv = ss_city_total_pu::pv;       bc = ss_city_total_one_count::count;
                                                              retpv = ss_city_total_pu::retpv; retbc = ss_city_total_one_count::retcount;
                                                              newpv = ss_city_total_pu::newpv; newbc = ss_city_total_one_count::newcount;
                                generate ss_city_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                          CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                          CONCAT((chararray)((float)newbc/newpv * 100),'%') ;};

-- 搜索引擎下的省(直辖市) 二级域名, 如,www.baidu.com

groupSubSSProvWid        =  group A2 by (wid, domain, se, province);
filterSubSSProvWid       =  filter groupSubSSProvWid by myFilterCount($1, 1);
filterSubSSProvWidAdv    =  foreach filterSubSSProvWid generate $0.wid as wid, $0.domain as domain, $0.se as se, $0.province as province,
                                                                                                                        flatten($1.new) as new;

-- 搜索引擎下的省(直辖市) 一级域名, 如,baidu.com
groupSSProvWid           =  group filterSubSSProvWidAdv by (wid, domain, mySubString(se), province);
filterSSProvWid          =  filter groupSSProvWid by myFilterCount($1, 1);
filterSSProvWidAdv       =  foreach filterSSProvWid generate $0.wid as wid, $0.domain as domain, $0.subString as parentse, $0.province as province,
                                                                                                                                flatten($1.new) as new;

-- 搜索引擎下的省(直辖市) 全部
groupSSProvTotalWid      =  group filterSSProvWidAdv by (wid, domain, province);
filterSSProvTotalWid     =  filter groupSSProvTotalWid by myFilterCount($1, 1);
filterSSProvTotalWidAdv  =  foreach filterSSProvTotalWid generate $0.domain as domain, $0.province as province, flatten($1.new) as new;


-- 搜索引擎下的省(直辖市) 二级域名, 如,www.baidu.com
groupSubSSProvOne           =  group filterSubSSProvWidAdv by (domain, se, province);
sub_ss_prov_one_count       =  foreach groupSubSSProvOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); newbc = bc - retbc;
                                    generate myConcat($time, $0.domain, mySubString($0.se), $0.se, $0.province, '') as domain,
                                                                                                        bc as count, retbc as retcount, newbc as newcount ;};
joinSubSSProvBounce         =  join sub_ss_prov_one_count by domain, sub_ss_prov_pu by domain;
sub_ss_prov_bounce_rate     =  foreach joinSubSSProvBounce {    pv = sub_ss_prov_pu::pv;       bc = sub_ss_prov_one_count::count;
                                                             retpv = sub_ss_prov_pu::retpv; retbc = sub_ss_prov_one_count::retcount;
                                                             newpv = sub_ss_prov_pu::newpv; newbc = sub_ss_prov_one_count::newcount;
                                    generate sub_ss_prov_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                            CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                            CONCAT((chararray)((float)newbc/newpv * 100),'%') ;};
-- 搜索引擎下的省(直辖市) 一级域名, 如,baidu.com
groupSSProvOne             =  group filterSSProvWidAdv by (domain, parentse, province);
ss_prov_one_count          =  foreach groupSSProvOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); newbc = bc - retbc;
                                    generate myConcat($time, $0.domain, $0.parentse, '', $0.province, '') as domain,
                                                                                                            bc as count, retbc as retcount, newbc as newcount ;};
joinSSProvBounce           =  join ss_prov_one_count by domain, ss_prov_pu by domain;
ss_prov_bounce_rate        =  foreach joinSSProvBounce {    pv = ss_prov_pu::pv;       bc = ss_prov_one_count::count;
                                                         retpv = ss_prov_pu::retpv; retbc = ss_prov_one_count::retcount;
                                                         newpv = ss_prov_pu::newpv; newbc = ss_prov_one_count::newcount;
                                    generate ss_prov_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                        CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                        CONCAT((chararray)((float)newbc/newpv * 100),'%') ;};
-- 搜索引擎下的市(直辖区)全部
groupSSProvTotalOne        =  group filterSSProvTotalWidAdv by (domain, province);
ss_prov_total_one_count    =  foreach groupSSProvTotalOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); newbc = bc - retbc;
                                    generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                                                                                        bc as count, retbc as retcount, newbc as newcount ;};
joinSSProvTotalBounce      =  join ss_prov_total_one_count by domain, ss_prov_total_pu by domain;
ss_prov_total_bounce_rate  =  foreach joinSSProvTotalBounce {    pv = ss_prov_total_pu::pv;       bc = ss_prov_total_one_count::count;
                                                              retpv = ss_prov_total_pu::retpv; retbc = ss_prov_total_one_count::retcount;
                                                              newpv = ss_prov_total_pu::newpv; newbc = ss_prov_total_one_count::newcount;
                                generate ss_prov_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                          CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                          CONCAT((chararray)((float)newbc/newpv * 100),'%') ;};

--------------------------------------搜索引擎 全部,全国,其他  bounce rate -----------------------------------------------
-- second floor
groupSubSSWid            =  group A2 by (wid, domain, se);
filterSubSSWid           =  filter groupSubSSWid by myFilterCount($1, 1);
filterSubSSWidAdv        =  foreach filterSubSSWid generate $0.wid as wid, $0.domain as domain, $0.se as se, flatten($1.new) as new, flatten($1.area) as area;
--  one floor
groupSSWid               =  group filterSubSSWidAdv by (wid, domain, mySubString(se));
filterSSWid              =  filter groupSSWid by myFilterCount($1, 1);
filterSSWidAdv           =  foreach filterSSWid generate $0.wid as wid, $0.domain as domain, $0.subString as parentse, flatten($1.new) as new, flatten($1.area) as area;
--  total se of domain
groupSSTotalWid          =  group filterSSWidAdv by (wid, domain);
filterSSTotalWid         =  filter groupSSTotalWid by myFilterCount($1, 1);
filterSSTotalWidAdv      =  foreach filterSSTotalWid generate $0.domain as domain, flatten($1.new) as new, flatten($1.area) as area;


-- second floor
groupSubSSOne            =  group filterSubSSWidAdv by (domain, se);
sub_ss_one_count         =  foreach groupSubSSOne { FA = filter $1 by new is null; FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                    bc = COUNT($1);     retbc = COUNT(FA);         newbc = bc - retbc;
                                               otherbc = COUNT(FO); otheretbc = COUNT(FORet); othernewbc = otherbc - otheretbc;
                                generate myConcat($time, $0.domain, mySubString($0.se), $0.se) as domain, 
                                                                 bc as count,                    retbc as retcount,                     newbc as newcount,
                                                            otherbc as othercount,           otheretbc as otheretcount,            othernewbc as othernewcount,
                                                     (bc - otherbc) as chinacount, (retbc - otheretbc) as chinaretcount, (newbc - othernewbc) as chinanewcount ;};

joinSubSSBounce          =  join sub_ss_one_count by domain, sub_ss_pu by domain;
sub_ss_bounce_rate       =  foreach joinSubSSBounce {    pv = sub_ss_pu::pv;                 bc = sub_ss_one_count::count; 
                                                      retpv = sub_ss_pu::retpv;           retbc = sub_ss_one_count::retcount;
                                                      newpv = sub_ss_pu::newpv;           newbc = sub_ss_one_count::newcount;
                                                    otherpv = sub_ss_pu::otherpv;       otherbc = sub_ss_one_count::othercount;
                                                  otheretpv = sub_ss_pu::otheretpv;   otheretbc = sub_ss_one_count::otheretcount;
                                                 othernewpv = sub_ss_pu::othernewpv; othernewbc = sub_ss_one_count::othernewcount;
                                                    chinapv = sub_ss_pu::chinapv;       chinabc = sub_ss_one_count::chinacount;
                                                 chinaretpv = sub_ss_pu::chinaretpv; chinaretbc = sub_ss_one_count::chinaretcount;
                                                 chinanewpv = sub_ss_pu::chinanewpv; chinanewbc = sub_ss_one_count::chinanewcount;
                                generate sub_ss_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                   CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                   CONCAT((chararray)((float)newbc/newpv * 100),'%'),
                                                                   CONCAT((chararray)((float)otherbc/otherpv * 100),'%'),
                                                                   CONCAT((chararray)((float)otheretbc/otheretpv * 100),'%'),
                                                                   CONCAT((chararray)((float)othernewbc/othernewpv * 100),'%'),
                                                                   CONCAT((chararray)((float)chinabc/chinapv * 100),'%'),
                                                                   CONCAT((chararray)((float)chinaretbc/chinaretpv * 100),'%'),
                                                                   CONCAT((chararray)((float)chinanewbc/chinanewpv * 100),'%') ;};
--  one floor
groupSSOne               =  group filterSSWidAdv by (domain, parentse);
ss_one_count             =  foreach groupSSOne {    FA = filter $1 by new is null; FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                    bc = COUNT($1);     retbc = COUNT(FA);         newbc = bc - retbc;
                                               otherbc = COUNT(FO); otheretbc = COUNT(FORet); othernewbc = otherbc - otheretbc;
                                generate myConcat($time, $0.domain, $0.parentse, '') as domain,
                                                                 bc as count,                    retbc as retcount,                     newbc as newcount,
                                                            otherbc as othercount,           otheretbc as otheretcount,            othernewbc as othernewcount,
                                                     (bc - otherbc) as chinacount, (retbc - otheretbc) as chinaretcount, (newbc - othernewbc) as chinanewcount ;};

joinSSBounce             =  join ss_one_count by domain, ss_pu by domain;
ss_bounce_rate           =  foreach joinSSBounce {       pv = ss_pu::pv;                 bc = ss_one_count::count;
                                                      retpv = ss_pu::retpv;           retbc = ss_one_count::retcount;
                                                      newpv = ss_pu::newpv;           newbc = ss_one_count::newcount;
                                                    otherpv = ss_pu::otherpv;       otherbc = ss_one_count::othercount;
                                                  otheretpv = ss_pu::otheretpv;   otheretbc = ss_one_count::otheretcount;
                                                 othernewpv = ss_pu::othernewpv; othernewbc = ss_one_count::othernewcount;
                                                    chinapv = ss_pu::chinapv;       chinabc = ss_one_count::chinacount;
                                                 chinaretpv = ss_pu::chinaretpv; chinaretbc = ss_one_count::chinaretcount;
                                                 chinanewpv = ss_pu::chinanewpv; chinanewbc = ss_one_count::chinanewcount;
                                generate ss_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                               CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                               CONCAT((chararray)((float)newbc/newpv * 100),'%'),
                                                               CONCAT((chararray)((float)otherbc/otherpv * 100),'%'),
                                                               CONCAT((chararray)((float)otheretbc/otheretpv * 100),'%'),
                                                               CONCAT((chararray)((float)othernewbc/othernewpv * 100),'%'),
                                                               CONCAT((chararray)((float)chinabc/chinapv * 100),'%'),
                                                               CONCAT((chararray)((float)chinaretbc/chinaretpv * 100),'%'),
                                                               CONCAT((chararray)((float)chinanewbc/chinanewpv * 100),'%') ;};
--  total se of domain
groupSSTotalOne          =  group filterSSTotalWidAdv by domain;
ss_total_one_count       =  foreach groupSSTotalOne  {    FA = filter $1 by new is null; FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                          bc = COUNT($1);     retbc = COUNT(FA);         newbc = bc - retbc;
                                                     otherbc = COUNT(FO); otheretbc = COUNT(FORet); othernewbc = otherbc - otheretbc;
                                generate myConcat($time, $0, '', '') as domain,
                                                                  bc as count,                    retbc as retcount,                     newbc as newcount,
                                                             otherbc as othercount,           otheretbc as otheretcount,            othernewbc as othernewcount,
                                                      (bc - otherbc) as chinacount, (retbc - otheretbc) as chinaretcount, (newbc - othernewbc) as chinanewcount ;};

joinSSTotalBounce        =  join ss_total_one_count by domain, ss_total_pu by domain;
ss_total_bounce_rate     =  foreach joinSSTotalBounce {       pv = ss_total_pu::pv;                 bc = ss_total_one_count::count;
                                                           retpv = ss_total_pu::retpv;           retbc = ss_total_one_count::retcount;
                                                           newpv = ss_total_pu::newpv;           newbc = ss_total_one_count::newcount;
                                                         otherpv = ss_total_pu::otherpv;       otherbc = ss_total_one_count::othercount;
                                                       otheretpv = ss_total_pu::otheretpv;   otheretbc = ss_total_one_count::otheretcount;
                                                      othernewpv = ss_total_pu::othernewpv; othernewbc = ss_total_one_count::othernewcount;
                                                         chinapv = ss_total_pu::chinapv;       chinabc = ss_total_one_count::chinacount;
                                                      chinaretpv = ss_total_pu::chinaretpv; chinaretbc = ss_total_one_count::chinaretcount;
                                                      chinanewpv = ss_total_pu::chinanewpv; chinanewbc = ss_total_one_count::chinanewcount;
                                generate ss_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                     CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                     CONCAT((chararray)((float)newbc/newpv * 100),'%'),
                                                                     CONCAT((chararray)((float)otherbc/otherpv * 100),'%'),
                                                                     CONCAT((chararray)((float)otheretbc/otheretpv * 100),'%'),
                                                                     CONCAT((chararray)((float)othernewbc/othernewpv * 100),'%'),
                                                                     CONCAT((chararray)((float)chinabc/chinapv * 100),'%'),
                                                                     CONCAT((chararray)((float)chinaretbc/chinaretpv * 100),'%'),
                                                                     CONCAT((chararray)((float)chinanewbc/chinanewpv * 100),'%') ;};

----   入库hbase
STORE sub_ss_city_bounce_rate INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE ss_city_bounce_rate INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE ss_city_total_bounce_rate INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE sub_ss_prov_bounce_rate INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE ss_prov_bounce_rate INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE ss_prov_total_bounce_rate INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');

STORE sub_ss_bounce_rate INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr cf:other:br vis:other:retbr vis:other:newbr cf:china:br vis:china:retbr vis:china:newbr');
STORE ss_bounce_rate INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr cf:other:br vis:other:retbr vis:other:newbr cf:china:br vis:china:retbr vis:china:newbr');
STORE ss_total_bounce_rate INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr cf:other:br vis:other:retbr vis:other:newbr cf:china:br vis:china:retbr vis:china:newbr');


--------------
-- average time
--------------

filterBbySE              =  filter B by se is not null;
B2                       =  foreach filterBbySE generate domain, page_avg_time, new, se, province, city, area;

-- second floor
groupSubSSPT             =  group B2 by (domain, se);
sub_ss_avgtime           =  foreach groupSubSSPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                                   FO    = filter $1 by area is null; FC    = filter $1 by area is not null;
                                                   FORet = filter FO by new  is null; FONew = filter FO by new  is not null;
                                                   FCRet = filter FC by new  is null; FCNew = filter FC by new  is not null;
                                generate myConcat($time, $0.domain, mySubString($0.se), $0.se) as domain, 
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)), 
                                         myTime(AVG(FNew.page_avg_time)),
                                         myTime(AVG(FO.page_avg_time)),
                                         myTime(AVG(FORet.page_avg_time)),
                                         myTime(AVG(FONew.page_avg_time)),
                                         myTime(AVG(FC.page_avg_time)),
                                         myTime(AVG(FCRet.page_avg_time)),
                                         myTime(AVG(FCNew.page_avg_time));};
--  one floor
groupSSPT                =  group B2 by (domain, mySubString(se));
ss_avgtime               =  foreach groupSSPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                                   FO    = filter $1 by area is null; FC    = filter $1 by area is not null;
                                                   FORet = filter FO by new  is null; FONew = filter FO by new  is not null;
                                                   FCRet = filter FC by new  is null; FCNew = filter FC by new  is not null;
                                generate myConcat($time, $0.domain, $0.subString, '') as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)), 
                                         myTime(AVG(FNew.page_avg_time)),
                                         myTime(AVG(FO.page_avg_time)),
                                         myTime(AVG(FORet.page_avg_time)),
                                         myTime(AVG(FONew.page_avg_time)),
                                         myTime(AVG(FC.page_avg_time)),
                                         myTime(AVG(FCRet.page_avg_time)),
                                         myTime(AVG(FCNew.page_avg_time));};
-- total 
groupSSTotalPT           =  group B2 by domain;
ss_total_avgtime         =  foreach groupSSTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                                   FO    = filter $1 by area is null; FC    = filter $1 by area is not null;
                                                   FORet = filter FO by new  is null; FONew = filter FO by new  is not null;
                                                   FCRet = filter FC by new  is null; FCNew = filter FC by new  is not null;
                                generate myConcat($time, $0, '', '') as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)), 
                                         myTime(AVG(FNew.page_avg_time)),
                                         myTime(AVG(FO.page_avg_time)),
                                         myTime(AVG(FORet.page_avg_time)),
                                         myTime(AVG(FONew.page_avg_time)),
                                         myTime(AVG(FC.page_avg_time)),
                                         myTime(AVG(FCRet.page_avg_time)),
                                         myTime(AVG(FCNew.page_avg_time));};

--------------------------------------搜索引擎 按地域分布  平均访问时长 -----------------------------------------------
-- 搜索引擎下的省(直辖市) 二级域名, 如,www.baidu.com
groupSubSSProvPT         =  group B2 by (domain, se, province);
sub_ss_prov_avgtime      =  foreach groupSubSSProvPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, mySubString($0.se), $0.se, $0.province, '') as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};

-- 搜索引擎下的省(直辖市) 一级域名, 如,baidu.com
groupSSProvPT           =  group B2 by (domain, mySubString(se), province);
ss_prov_avgtime         =  foreach groupSSProvPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, $0.subString, '', $0.province, '') as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};

-- 搜索引擎下的省(直辖市)全部
groupSSProvTotalPT      =  group B2 by (domain, province);
ss_prov_total_avgtime   =  foreach groupSSProvTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};

filterSSCityPT          =  filter B2 by city is not null;
-- 搜索引擎下的市(直辖区) 二级域名, 如,www.baidu.com
groupSubSSCityPT        =  group filterSSCityPT by (domain, se, province, city);
sub_ss_city_avgtime     =  foreach groupSubSSCityPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, mySubString($0.se), $0.se, $0.province, $0.city) as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};

-- 搜索引擎下的市(直辖区) 一级域名, 如,baidu.com
groupSSCityPT          =  group filterSSCityPT by (domain, mySubString(se), province, city);
ss_city_avgtime        =  foreach groupSSCityPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, $0.subString, '', $0.province, $0.city) as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};

-- 搜索引擎下的市(直辖区)全部
groupSSCityTotalPT     =  group filterSSCityPT by (domain, province, city);
ss_city_total_avgtime  =  foreach groupSSCityTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};
----   入库hbase
STORE sub_ss_avgtime INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime 
                                                       cf:other:avgtime vis:other:retavgtime vis:other:newavgtime 
                                                       cf:china:avgtime vis:china:retavgtime vis:china:newavgtime');
STORE ss_avgtime INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime
                                                       cf:other:avgtime vis:other:retavgtime vis:other:newavgtime
                                                       cf:china:avgtime vis:china:retavgtime vis:china:newavgtime');
STORE ss_total_avgtime INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime
                                                       cf:other:avgtime vis:other:retavgtime vis:other:newavgtime
                                                       cf:china:avgtime vis:china:retavgtime vis:china:newavgtime');

STORE sub_ss_prov_avgtime INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime');

STORE ss_prov_avgtime INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime');

STORE ss_prov_total_avgtime INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime');

STORE sub_ss_city_avgtime INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime');

STORE ss_city_avgtime INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime');

STORE ss_city_total_avgtime INTO 'hbase://kpi_search_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime');

-------------------------------------------外部链接-----------------------------------------------------------------------------------------------------
-----------
--  pv uv
-----------
filterAbyWB              =  filter A by wb == 1;
tmpA3                    =  foreach filterAbyWB generate ip, wid, ref, domain, new, province, city, area;
A3                       =  foreach tmpA3 generate ip, wid, ref, domain, new, province, city;
filterWBCity             =  filter A3 by city is not null;
-- domain
groupWBTotalPU           =  group tmpA3 by domain;
wb_total_pu              =  foreach groupWBTotalPU {      FA = filter $1 by new is null;   FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                     otherIP = distinct FO.ip;       otherWid = distinct FO.wid;
                                                  otherRetIP = distinct FORet.ip; otherRetWid = distinct FORet.wid;
                                                       retIP = distinct FA.ip;         retWid = distinct FA.wid;
                                                          ip = distinct $1.ip;            wid = distinct $1.wid;
                                 otherpv = COUNT(FO);              otheruv = COUNT(otherWid);        otherip = COUNT(otherIP);
                               otheretpv = COUNT(FORet);         otheretuv = COUNT(otherRetWid);   otheretip = COUNT(otherRetIP);
                              othernewpv = otherpv - otheretpv; othernewuv = otheruv - otheretuv; othernewip = otherip - otheretip;
                                      pv = COUNT($1);                   uv = COUNT(wid);                  ip = COUNT(ip);
                                   retpv = COUNT(FA);                retuv = COUNT(retWid);            retip = COUNT(retIP);
                                   newpv = pv - retpv;               newuv = uv - retuv;               newip = ip - retip;
                                   generate myConcat($time, $0, '','') as domain,
                                                                    pv as pv,                         uv as uv,                         ip as ip,
                                                                 retpv as retpv,                   retuv as retuv,                   retip as retip,
                                                                 newpv as newpv,                   newuv as newuv,                   newip as newip,
                                                               otherpv as otherpv,               otheruv as otheruv,               otherip as otherip,
                                                             otheretpv as otheretpv,           otheretuv as otheretuv,           otheretip as otheretip,
                                                            othernewpv as othernewpv,         othernewuv as othernewuv,         othernewip as othernewip,
                                                        (pv - otherpv) as chinapv,        (uv - otheruv) as chinauv,        (ip - otherip) as chinaip,
                                                     (retpv-otheretpv) as chinaretpv,  (retuv-otheretuv) as chinaretuv,  (retip-otheretip) as chinaretip,
                                                    (newpv-othernewpv) as chinanewpv, (newuv-othernewuv) as chinanewuv, (newip-othernewip) as chinanewip ;};


--------------------------------------搜索引擎 按地域分布  pv uv ip -----------------------------------------------
-- 搜索引擎下的省(直辖市)全部
groupWBProvTotalPU       =  group A3 by (domain, province);
wb_prov_total_pu         =  foreach groupWBProvTotalPU {    FA = filter $1 by new is null;
                                                         retIP = distinct FA.ip;       retWid = distinct FA.wid;
                                                            ip = distinct $1.ip;          wid = distinct $1.wid;
                                      retpv = COUNT(FA); retuv = COUNT(retWid);         retip = COUNT(retIP);
                                         pv = COUNT($1);    uv = COUNT(wid);               ip = COUNT(ip);
                                generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                                                    pv as pv,                  uv as uv,                  ip as ip,
                                                                 retpv as retpv,            retuv as retuv,            retip as retip,
                                                          (pv - retpv) as newpv,     (uv - retuv) as newuv,     (ip - retip) as newip ;};

-- 搜索引擎下的市(直辖区)全部
groupWBCityTotalPU       =  group filterWBCity by (domain, province, city);
wb_city_total_pu         =  foreach groupWBCityTotalPU {    FA = filter $1 by new is null;
                                                         retIP = distinct FA.ip;       retWid = distinct FA.wid;
                                                            ip = distinct $1.ip;          wid = distinct $1.wid;
                                      retpv = COUNT(FA); retuv = COUNT(retWid);         retip = COUNT(retIP);
                                         pv = COUNT($1);    uv = COUNT(wid);               ip = COUNT(ip);
                                generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                                                    pv as pv,                  uv as uv,                  ip as ip,
                                                                 retpv as retpv,            retuv as retuv,            retip as retip,
                                                          (pv - retpv) as newpv,     (uv - retuv) as newuv,     (ip - retip) as newip ;};

----   入库hbase
STORE wb_total_pu INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage (
                    'cf:pv       cf:uv       cf:ip       vis:retpv       vis:retuv       vis:retip       vis:newpv       vis:newuv       vis:newip
                     cf:other:pv cf:other:uv cf:other:ip vis:other:retpv vis:other:retuv vis:other:retip vis:other:newpv vis:other:newuv vis:other:newip
                     cf:china:pv cf:china:uv cf:china:ip vis:china:retpv vis:china:retuv vis:china:retip vis:china:newpv vis:china:newuv vis:china:newip');

STORE wb_prov_total_pu INTO 'hbase://kpi_out_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE wb_city_total_pu INTO 'hbase://kpi_out_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

------------------------
-- bounce rate  ( 小 -> 大 )
-----------------------
-- 搜索引擎下的市(直辖区)全部
groupWBCityTotalWid      =  group filterWBCity by (wid, domain, province, city);
filterWBCityTotalWid     =  filter groupWBCityTotalWid by myFilterCount($1, 1);
filterWBCityTotalWidAdv  =  foreach filterWBCityTotalWid generate $0.domain as domain, $0.province as province, $0.city as city, flatten($1.new) as new;

groupWBCityTotalOne        =  group filterWBCityTotalWidAdv by (domain, province, city);
wb_city_total_one_count    =  foreach groupWBCityTotalOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); newbc = bc - retbc;
                                    generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                                                                                        bc as count, retbc as retcount, newbc as newcount ;};
wb_city_total_bounce_rate  =  foreach ( join wb_city_total_one_count by domain, wb_city_total_pu by domain ) 
                                                            {    pv = wb_city_total_pu::pv;       bc = wb_city_total_one_count::count;
                                                              retpv = wb_city_total_pu::retpv; retbc = wb_city_total_one_count::retcount;
                                                              newpv = wb_city_total_pu::newpv; newbc = wb_city_total_one_count::newcount;
                                generate wb_city_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                          CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                          CONCAT((chararray)((float)newbc/newpv * 100),'%') ;};
-- 搜索引擎下的省(直辖市) 全部
groupWBProvTotalWid      =  group A3 by (wid, domain, province);
filterWBProvTotalWid     =  filter groupWBProvTotalWid by myFilterCount($1, 1);
filterWBProvTotalWidAdv  =  foreach filterWBProvTotalWid generate $0.domain as domain, $0.province as province, flatten($1.new) as new;

groupWBProvTotalOne        =  group filterWBProvTotalWidAdv by (domain, province);
wb_prov_total_one_count    =  foreach groupWBProvTotalOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); newbc = bc - retbc;
                                    generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                                                                                        bc as count, retbc as retcount, newbc as newcount ;};
wb_prov_total_bounce_rate  =  foreach ( join wb_prov_total_one_count by domain, wb_prov_total_pu by domain ) 
                                                            {    pv = wb_prov_total_pu::pv;       bc = wb_prov_total_one_count::count;
                                                              retpv = wb_prov_total_pu::retpv; retbc = wb_prov_total_one_count::retcount;
                                                              newpv = wb_prov_total_pu::newpv; newbc = wb_prov_total_one_count::newcount;
                                generate wb_prov_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                          CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                          CONCAT((chararray)((float)newbc/newpv * 100),'%') ;};

------------------------------------------------外部链接下的全部--------------------------------------------------------------------------
--  domain
groupWBTotalWid          =  group tmpA3 by (wid, domain);
filterWBTotalWid         =  filter groupWBTotalWid by myFilterCount($1, 1);
filterWBTotalWidAdv      =  foreach filterWBTotalWid generate $0.domain as domain, flatten($1.new) as new, flatten($1.area) as area;

--  domain
groupWBTotalOne          =  group filterWBTotalWidAdv by domain;
wb_total_one_count       =  foreach groupWBTotalOne {     FA = filter $1 by new is null; FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                          bc = COUNT($1);     retbc = COUNT(FA);         newbc = bc - retbc;
                                                     otherbc = COUNT(FO); otheretbc = COUNT(FORet); othernewbc = otherbc - otheretbc;
                                generate myConcat($time, $0, '', '') as domain,
                                                                  bc as count,                    retbc as retcount,                     newbc as newcount,
                                                             otherbc as othercount,           otheretbc as otheretcount,            othernewbc as othernewcount,
                                                      (bc - otherbc) as chinacount, (retbc - otheretbc) as chinaretcount, (newbc - othernewbc) as chinanewcount ;};

joinWBTotalBounce        =  join wb_total_one_count by domain, wb_total_pu by domain;
wb_total_bounce_rate     =  foreach joinWBTotalBounce {       pv = wb_total_pu::pv;                 bc = wb_total_one_count::count;
                                                           retpv = wb_total_pu::retpv;           retbc = wb_total_one_count::retcount;
                                                           newpv = wb_total_pu::newpv;           newbc = wb_total_one_count::newcount;
                                                         otherpv = wb_total_pu::otherpv;       otherbc = wb_total_one_count::othercount;
                                                       otheretpv = wb_total_pu::otheretpv;   otheretbc = wb_total_one_count::otheretcount;
                                                      othernewpv = wb_total_pu::othernewpv; othernewbc = wb_total_one_count::othernewcount;
                                                         chinapv = wb_total_pu::chinapv;       chinabc = wb_total_one_count::chinacount;
                                                      chinaretpv = wb_total_pu::chinaretpv; chinaretbc = wb_total_one_count::chinaretcount;
                                                      chinanewpv = wb_total_pu::chinanewpv; chinanewbc = wb_total_one_count::chinanewcount;
                                generate wb_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                     CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                     CONCAT((chararray)((float)newbc/newpv * 100),'%'),
                                                                     CONCAT((chararray)((float)otherbc/otherpv * 100),'%'),
                                                                     CONCAT((chararray)((float)otheretbc/otheretpv * 100),'%'),
                                                                     CONCAT((chararray)((float)othernewbc/othernewpv * 100),'%'),
                                                                     CONCAT((chararray)((float)chinabc/chinapv * 100),'%'),
                                                                     CONCAT((chararray)((float)chinaretbc/chinaretpv * 100),'%'),
                                                                     CONCAT((chararray)((float)chinanewbc/chinanewpv * 100),'%') ;};

----   入库hbase
STORE wb_city_total_bounce_rate INTO 'hbase://kpi_out_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE wb_prov_total_bounce_rate INTO 'hbase://kpi_out_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');
STORE wb_total_bounce_rate INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr cf:other:br vis:other:retbr vis:other:newbr cf:china:br vis:china:retbr vis:china:newbr');

--------------
-- average time
--------------
filterBbyWB              =  filter B by wb == 1;
tmpB3                    =  foreach filterBbyWB generate domain, page_avg_time, ref, new, province, city, area;
B3                       =  foreach tmpB3 generate domain, page_avg_time, ref, new, province, city;
filterWBCityPT           =  filter B3 by city is not null;
--  domain
groupWBTotalPT           =  group tmpB3 by domain;
wb_total_avgtime         =  foreach groupWBTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                                     FO    = filter $1 by area is null; FC    = filter $1 by area is not null;
                                                     FORet = filter FO by new  is null; FONew = filter FO by new  is not null;
                                                     FCRet = filter FC by new  is null; FCNew = filter FC by new  is not null;
                                  generate myConcat($time, $0, '', '') as domain,
                                           myTime(AVG($1.page_avg_time)),
                                           myTime(AVG(FRet.page_avg_time)),
                                           myTime(AVG(FNew.page_avg_time)),
                                           myTime(AVG(FO.page_avg_time)),
                                           myTime(AVG(FORet.page_avg_time)),
                                           myTime(AVG(FONew.page_avg_time)),
                                           myTime(AVG(FC.page_avg_time)),
                                           myTime(AVG(FCRet.page_avg_time)),
                                           myTime(AVG(FCNew.page_avg_time));};


--------------------------------------搜索引擎 按地域分布  平均访问时长 -----------------------------------------------
-- 搜索引擎下的省(直辖市)全部
groupWBProvTotalPT      =  group B3 by (domain, province);
wb_prov_total_avgtime   =  foreach groupWBProvTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};

-- 搜索引擎下的市(直辖区)全部
groupWBCityTotalPT      =  group filterWBCityPT by (domain, province, city);
wb_city_total_avgtime   =  foreach groupWBCityTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};

----   入库hbase
STORE wb_prov_total_avgtime INTO 'hbase://kpi_out_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');
STORE wb_city_total_avgtime INTO 'hbase://kpi_out_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');

STORE wb_total_avgtime INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime
                                                       cf:other:avgtime vis:other:retavgtime vis:other:newavgtime
                                                       cf:china:avgtime vis:china:retavgtime vis:china:newavgtime');


--5----------------------------------------直接访问------------------------------------------------------------------------------------------
-----------
--  pv uv
-----------
filterAbyRef              =  filter A by ref is not null;
tmpA5                     =  foreach filterAbyRef generate ip, wid, domain, new, province, city, area;
A5                        =  foreach tmpA5 generate ip, wid, domain, new, province, city;
filterRefCity             =  filter A5 by city is not null;
--  total ref domain
groupRefTotalPU           =  group tmpA5 by domain;
ref_total_pu              =  foreach groupRefTotalPU {    FA = filter $1 by new is null; FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                     otherIP = distinct FO.ip;       otherWid = distinct FO.wid;
                                                  otherRetIP = distinct FORet.ip; otherRetWid = distinct FORet.wid;
                                                       retIP = distinct FA.ip;         retWid = distinct FA.wid;
                                                          ip = distinct $1.ip;            wid = distinct $1.wid;
                                 otherpv = COUNT(FO);              otheruv = COUNT(otherWid);        otherip = COUNT(otherIP);
                               otheretpv = COUNT(FORet);         otheretuv = COUNT(otherRetWid);   otheretip = COUNT(otherRetIP);
                              othernewpv = otherpv - otheretpv; othernewuv = otheruv - otheretuv; othernewip = otherip - otheretip;
                                      pv = COUNT($1);                   uv = COUNT(wid);                  ip = COUNT(ip);
                                   retpv = COUNT(FA);                retuv = COUNT(retWid);            retip = COUNT(retIP);
                                   newpv = pv - retpv;               newuv = uv - retuv;               newip = ip - retip;
                                   generate myConcat($time, $0) as domain,
                                                                    pv as pv,                         uv as uv,                         ip as ip,
                                                                 retpv as retpv,                   retuv as retuv,                   retip as retip,
                                                                 newpv as newpv,                   newuv as newuv,                   newip as newip,
                                                               otherpv as otherpv,               otheruv as otheruv,               otherip as otherip,
                                                             otheretpv as otheretpv,           otheretuv as otheretuv,           otheretip as otheretip,
                                                            othernewpv as othernewpv,         othernewuv as othernewuv,         othernewip as othernewip,
                                                        (pv - otherpv) as chinapv,        (uv - otheruv) as chinauv,        (ip - otherip) as chinaip,
                                                     (retpv-otheretpv) as chinaretpv,  (retuv-otheretuv) as chinaretuv,  (retip-otheretip) as chinaretip,
                                                    (newpv-othernewpv) as chinanewpv, (newuv-othernewuv) as chinanewuv, (newip-othernewip) as chinanewip ;};


--------------------------------------搜索引擎 按地域分布  pv uv ip -----------------------------------------------
-- 搜索引擎下的省(直辖市)全部
ref_prov_total_pu         =  foreach (group A5 by (domain, province)) {    FA = filter $1 by new is null;
                                                         retIP = distinct FA.ip; retWid = distinct FA.wid;
                                                            ip = distinct $1.ip;    wid = distinct $1.wid;
                                      retpv = COUNT(FA); retuv = COUNT(retWid);   retip = COUNT(retIP);
                                         pv = COUNT($1);    uv = COUNT(wid);         ip = COUNT(ip);
                                generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                                                    pv as pv,              uv as uv,              ip as ip,
                                                                 retpv as retpv,        retuv as retuv,        retip as retip,
                                                          (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

-- 搜索引擎下的市(直辖区)全部
groupRefCityTotalPU       =  group filterRefCity by (domain, province, city);
ref_city_total_pu         =  foreach groupRefCityTotalPU {    FA = filter $1 by new is null;
                                                         retIP = distinct FA.ip; retWid = distinct FA.wid;
                                                            ip = distinct $1.ip;    wid = distinct $1.wid;
                                      retpv = COUNT(FA); retuv = COUNT(retWid);   retip = COUNT(retIP);
                                         pv = COUNT($1);    uv = COUNT(wid);         ip = COUNT(ip);
                                generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                                                    pv as pv,              uv as uv,              ip as ip,
                                                                 retpv as retpv,        retuv as retuv,        retip as retip,
                                                          (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

----   入库hbase
STORE ref_total_pu INTO 'hbase://kpi_direct_access' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage (
                    'cf:pv       cf:uv       cf:ip       vis:retpv       vis:retuv       vis:retip       vis:newpv       vis:newuv       vis:newip
                     cf:other:pv cf:other:uv cf:other:ip vis:other:retpv vis:other:retuv vis:other:retip vis:other:newpv vis:other:newuv vis:other:newip
                     cf:china:pv cf:china:uv cf:china:ip vis:china:retpv vis:china:retuv vis:china:retip vis:china:newpv vis:china:newuv vis:china:newip');

STORE ref_prov_total_pu INTO 'hbase://kpi_direct_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

STORE ref_city_total_pu INTO 'hbase://kpi_direct_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');

------------------------
-- bounce rate  ( 小 -> 大 )
-----------------------
-- 搜索引擎下的市(直辖区)全部
groupRefCityTotalWid      =  group filterRefCity by (wid, domain, province, city);
filterRefCityTotalWid     =  filter groupRefCityTotalWid by myFilterCount($1, 1);
filterRefCityTotalWidAdv  =  foreach filterRefCityTotalWid generate $0.domain as domain, $0.province as province, $0.city as city, flatten($1.new) as new;

groupRefCityTotalOne        =  group filterRefCityTotalWidAdv by (domain, province, city);
ref_city_total_one_count    =  foreach groupRefCityTotalOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); nerefc = bc - retbc;
                                    generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                                                                                        bc as count, retbc as retcount, nerefc as newcount ;};
ref_city_total_bounce_rate  =  foreach ( join ref_city_total_one_count by domain, ref_city_total_pu by domain )
                                                            {    pv = ref_city_total_pu::pv;       bc = ref_city_total_one_count::count;
                                                              retpv = ref_city_total_pu::retpv; retbc = ref_city_total_one_count::retcount;
                                                              newpv = ref_city_total_pu::newpv; nerefc = ref_city_total_one_count::newcount;
                                generate ref_city_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                           CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                           CONCAT((chararray)((float)nerefc/newpv * 100),'%') ;};

-- 搜索引擎下的省(直辖市) 全部
groupRefProvTotalWid      =  group A5 by (wid, domain, province);
filterRefProvTotalWid     =  filter groupRefProvTotalWid by myFilterCount($1, 1);
filterRefProvTotalWidAdv  =  foreach filterRefProvTotalWid generate $0.domain as domain, $0.province as province, flatten($1.new) as new;

groupRefProvTotalOne        =  group filterRefProvTotalWidAdv by (domain, province);
ref_prov_total_one_count    =  foreach groupRefProvTotalOne { FA = filter $1 by new is null; bc = COUNT($1); retbc = COUNT(FA); nerefc = bc - retbc;
                                    generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                                                                                        bc as count, retbc as retcount, nerefc as newcount ;};
ref_prov_total_bounce_rate  =  foreach ( join ref_prov_total_one_count by domain, ref_prov_total_pu by domain )
                                                            {    pv = ref_prov_total_pu::pv;       bc = ref_prov_total_one_count::count;
                                                              retpv = ref_prov_total_pu::retpv; retbc = ref_prov_total_one_count::retcount;
                                                              newpv = ref_prov_total_pu::newpv; nerefc = ref_prov_total_one_count::newcount;
                                generate ref_prov_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                           CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                           CONCAT((chararray)((float)nerefc/newpv * 100),'%') ;};

-------------------------------直接链接全部------------------------------------------------------------------------------------------
--  total ref
groupRefTotalWid          =  group tmpA5 by (wid, domain);
filterRefTotalWid         =  filter groupRefTotalWid by myFilterCount($1, 1);
filterRefTotalWidAdv      =  foreach filterRefTotalWid generate $0.domain as domain, flatten($1.new) as new, flatten($1.area) as area;

--  total bounce rate
groupRefTotalOne          =  group filterRefTotalWidAdv by domain;
ref_total_one_count       =  foreach groupRefTotalOne {   FA = filter $1 by new is null; FO = filter $1 by area is null; FORet = filter FO by new is null;
                                                          bc = COUNT($1);     retbc = COUNT(FA);         newbc = bc - retbc;
                                                     otherbc = COUNT(FO); otheretbc = COUNT(FORet); othernewbc = otherbc - otheretbc;
                                generate myConcat($time, $0) as domain,
                                                                  bc as count,                    retbc as retcount,                     newbc as newcount,
                                                             otherbc as othercount,           otheretbc as otheretcount,            othernewbc as othernewcount,
                                                      (bc - otherbc) as chinacount, (retbc - otheretbc) as chinaretcount, (newbc - othernewbc) as chinanewcount ;};

joinRefTotalBounce        =  join ref_total_one_count by domain, ref_total_pu by domain;
ref_total_bounce_rate     =  foreach joinRefTotalBounce {     pv = ref_total_pu::pv;                  bc = ref_total_one_count::count;
                                                           retpv = ref_total_pu::retpv;            retbc = ref_total_one_count::retcount;
                                                           newpv = ref_total_pu::newpv;           nerefc = ref_total_one_count::newcount;
                                                         otherpv = ref_total_pu::otherpv;        otherbc = ref_total_one_count::othercount;
                                                       otheretpv = ref_total_pu::otheretpv;    otheretbc = ref_total_one_count::otheretcount;
                                                      othernewpv = ref_total_pu::othernewpv; othernerefc = ref_total_one_count::othernewcount;
                                                         chinapv = ref_total_pu::chinapv;        chinabc = ref_total_one_count::chinacount;
                                                      chinaretpv = ref_total_pu::chinaretpv;  chinaretbc = ref_total_one_count::chinaretcount;
                                                      chinanewpv = ref_total_pu::chinanewpv; chinanerefc = ref_total_one_count::chinanewcount;
                                generate ref_total_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%'),
                                                                      CONCAT((chararray)((float)retbc/retpv * 100),'%'),
                                                                      CONCAT((chararray)((float)nerefc/newpv * 100),'%'),
                                                                      CONCAT((chararray)((float)otherbc/otherpv * 100),'%'),
                                                                      CONCAT((chararray)((float)otheretbc/otheretpv * 100),'%'),
                                                                      CONCAT((chararray)((float)othernerefc/othernewpv * 100),'%'),
                                                                      CONCAT((chararray)((float)chinabc/chinapv * 100),'%'),
                                                                      CONCAT((chararray)((float)chinaretbc/chinaretpv * 100),'%'),
                                                                      CONCAT((chararray)((float)chinanerefc/chinanewpv * 100),'%') ;};

----   入库hbase
STORE ref_city_total_bounce_rate INTO 'hbase://kpi_direct_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');

STORE ref_prov_total_bounce_rate INTO 'hbase://kpi_direct_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');

STORE ref_total_bounce_rate INTO 'hbase://kpi_direct_access' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr cf:other:br vis:other:retbr vis:other:newbr cf:china:br vis:china:retbr vis:china:newbr');


--------------
-- average time
--------------
filterBbyRef             =  filter B by ref is not null;
tmpB5                    =  foreach filterBbyRef generate domain, page_avg_time, new, province, city, area;
B5                       =  foreach tmpB5 generate domain, page_avg_time, new, province, city;
filterRefCityPT          =  filter B5 by city is not null;     
-- total
groupRefTotalPT          =  group tmpB5 by domain;
ref_total_avgtime        =  foreach groupRefTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                                      FO    = filter $1 by area is null; FC    = filter $1 by area is not null;
                                                      FORet = filter FO by new  is null; FONew = filter FO by new  is not null;
                                                      FCRet = filter FC by new  is null; FCNew = filter FC by new  is not null;
                                generate myConcat($time, $0),
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time)),
                                         myTime(AVG(FO.page_avg_time)),
                                         myTime(AVG(FORet.page_avg_time)),
                                         myTime(AVG(FONew.page_avg_time)),
                                         myTime(AVG(FC.page_avg_time)),
                                         myTime(AVG(FCRet.page_avg_time)),
                                         myTime(AVG(FCNew.page_avg_time));};

--------------------------------------搜索引擎 按地域分布  平均访问时长 -----------------------------------------------
-- 搜索引擎下的省(直辖市)全部
groupRefProvTotalPT     =  group B5 by (domain, province);
ref_prov_total_avgtime  =  foreach groupRefProvTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, '', '', $0.province, '') as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};

-- 搜索引擎下的市(直辖区)全部
groupRefCityTotalPT     =  group filterRefCityPT by (domain, province, city);
ref_city_total_avgtime  =  foreach groupRefCityTotalPT { FRet  = filter $1 by new  is null; FNew  = filter $1 by new  is not null;
                                generate myConcat($time, $0.domain, '', '', $0.province, $0.city) as domain,
                                         myTime(AVG($1.page_avg_time)),
                                         myTime(AVG(FRet.page_avg_time)),
                                         myTime(AVG(FNew.page_avg_time));};


----   入库hbase
STORE ref_total_avgtime INTO 'hbase://kpi_direct_access' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime
                                                       cf:other:avgtime vis:other:retavgtime vis:other:newavgtime
                                                       cf:china:avgtime vis:china:retavgtime vis:china:newavgtime');

STORE ref_prov_total_avgtime INTO 'hbase://kpi_direct_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');

STORE ref_city_total_avgtime INTO 'hbase://kpi_direct_region' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');


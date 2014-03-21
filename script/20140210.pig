--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 10;
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
--  account page stay average duration
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, ref:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, enter:chararray,
                                                                new:int, se:chararray, wb:int, kw:chararray, province:chararray, city:chararray, area:chararray);

------------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------来源分析-------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------

--------------
-- average time
--------------

filterBbySE              =  filter B by se is not null;
B2                       =  foreach filterBbySE generate domain, page_avg_time, new, se, province, city, area;
B4                       =  foreach filterBbySE generate domain, page_avg_time, new, se, kw;

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

groupSubKWPT             =  group B4 by (domain, se, kw);
sub_kw_avgtime           =  foreach groupSubKWPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0.domain, mySubString($0.se), $0.se, $0.kw) ,
                                           myTime(AVG($1.page_avg_time)) ,
                                           myTime(AVG(FBRet.page_avg_time)) ,
                                           myTime(AVG(FBNew.page_avg_time));};

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

groupKWPT                =  group B4 by (domain, mySubString(se), kw);
kw_avgtime               =  foreach groupKWPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0.domain, $0.subString, '', $0.kw) ,
                                           myTime(AVG($1.page_avg_time))   ,
                                           myTime(AVG(FBRet.page_avg_time)),
                                           myTime(AVG(FBNew.page_avg_time));};

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

groupKWTotalPT            =  group B4 by (domain, kw);
kw_total_avgtime          =  foreach groupKWTotalPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0.domain, '', '', $0.kw) ,
                                           myTime(AVG($1.page_avg_time)) ,
                                           myTime(AVG(FBRet.page_avg_time)) ,
                                           myTime(AVG(FBNew.page_avg_time));};

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
STORE sub_kw_avgtime INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');
STORE kw_avgtime INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');
STORE kw_total_avgtime INTO 'hbase://kpi_search_word' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');

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
--------------
-- average time
--------------
filterBbyWB              =  filter B by wb == 1;
tmpB3                    =  foreach filterBbyWB generate domain, page_avg_time, ref, new, province, city, area;
B3                       =  foreach tmpB3 generate domain, page_avg_time, ref, new, province, city;
filterWBCityPT           =  filter B3 by city is not null;
--  page
groupSubWBPT             =  group B3 by (domain, ref);
sub_wb_avgtime           =  foreach groupSubWBPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                generate myConcat($time, $0.domain, mySubString($0.ref), $0.ref) as domain,
                                         myTime(AVG($1.page_avg_time))    ,
                                         myTime(AVG(FBRet.page_avg_time)) ,
                                         myTime(AVG(FBNew.page_avg_time)) ;};

--  respondomain
groupWBPT                =  group B3 by (domain, mySubString(ref));
wb_avgtime               =  foreach groupWBPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0.domain, $0.subString, '')        ,
                                           myTime(AVG($1.page_avg_time))  ,
                                           myTime(AVG(FBRet.page_avg_time)) ,
                                           myTime(AVG(FBNew.page_avg_time)) ;};

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

STORE sub_wb_avgtime INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');
STORE wb_avgtime INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');
STORE wb_total_avgtime INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime       vis:retavgtime       vis:newavgtime
                                                       cf:other:avgtime vis:other:retavgtime vis:other:newavgtime
                                                       cf:china:avgtime vis:china:retavgtime vis:china:newavgtime');

--5----------------------------------------直接访问------------------------------------------------------------------------------------------

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


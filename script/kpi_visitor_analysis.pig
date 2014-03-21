--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 20;
SET job.name 'KPI_DOMAIN_D';

REGISTER ../lib/kpi.jar
DEFINE myConcat       com.jobs.pig.UDF.ConcatUDF;
DEFINE myFilterCount  com.jobs.pig.UDF.FilterCountUDF;
DEFINE myTime         com.jobs.pig.UDF.FormatTimeUDF;
DEFINE myCount        com.jobs.pig.UDF.MyCount;
DEFINE myUnconcat     com.jobs.pig.UDF.UnConcatUDF;
DEFINE myToString     com.jobs.pig.UDF.ToString;
DEFINE myIsEmpty      com.jobs.pig.UDF.IsEmpty;
DEFINE mySubString    com.jobs.pig.UDF.SubString;
------------------------------------------------------------------------------------------------
--------------------------------------------- Domain -------------------------------------------
------------------------------------------------------------------------------------------------
--     load the filter data
A  =  load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, referer:chararray, domain:chararray, jobPage:chararray, 
                                                vtime:chararray, respondomain:chararray, new:int, enter:chararray, wb:int, se:chararray);

--     account the domain pv count
A1               =  foreach A generate wid, domain, new, wb, referer;
groupDomainPU    =  group A1 by domain;

domain_pu_count  =  FOREACH groupDomainPU {uid = A1.wid; uniq_wid = distinct uid; generate myConcat($time,$0) as domain, COUNT($1) as pv, COUNT(uniq_wid) as uv;};




--    compute the new/return customer (pv, uv, bounce rate, avg time)
return_pu_count              =  foreach groupDomainPU {FA = filter $1 by new is null; uid = FA.wid; uniq_wid = distinct uid; 
                                    generate myConcat($time,$0) as domain, COUNT(FA) as pv, COUNT(uniq_wid) as uv;};
joinDomainReturnPU           =  join domain_pu_count by domain left outer, return_pu_count by domain;
new_pu_count                 =  foreach joinDomainReturnPU {allPV = domain_pu_count::pv; allUV = domain_pu_count::uv; 
                                    returnPV = ((return_pu_count::pv is null) ? 0 : return_pu_count::pv);
                                    returnUV = ((return_pu_count::uv is null) ? 0 : return_pu_count::uv);
                                    generate flatten(domain_pu_count::domain) as domain, (allPV - returnPV) as pv, (allUV - returnUV) as uv;};

STORE domain_pu_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv');
STORE new_pu_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newpv vis:newuv');

STORE return_pu_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retpv vis:retuv');

--    compute the domain bounce rate
groupDomainWid               =  group A1 by (domain, wid);
filterOneCount               =  filter groupDomainWid by myFilterCount(A1, 1);
filterOneCountAdv            =  foreach filterOneCount generate $0.domain as domain, $1.new as new, $1.wb as wb, $1.referer as referer;
groupOneCount                =  group filterOneCountAdv by domain;
one_page_count               =  foreach groupOneCount generate myConcat($time, $0) as domain, COUNT($1) as count, $1.new as new;
joinDomainBounceCount        =  join one_page_count by domain, domain_pu_count by domain;                      -- compute domain bounce rate/pv
domain_bounce_rate           =  foreach joinDomainBounceCount {pvCount = domain_pu_count::pv; 
                                    bounceCount = one_page_count::count; 
                                    generate flatten(one_page_count::domain), CONCAT((chararray)((float)bounceCount/pvCount * 100),'%');};
-- new/return
new_bounce_count            =  foreach one_page_count generate $0 as domain, myCount(new) as count;
joinDomainNewBR             =  join one_page_count by domain left outer, new_bounce_count by domain;
return_bounce_count         =  foreach joinDomainNewBR {allBR = one_page_count::count; 
                                    newBR = ((new_bounce_count::count is null) ? 0 : new_bounce_count::count);
                                    generate one_page_count::domain as domain, (allBR - newBR) as count;};
joinNewBounceCount          =  join new_bounce_count by domain, new_pu_count by domain;
new_bounce_rate             =  foreach joinNewBounceCount {pvCount = new_pu_count::pv;
                                bounceCount = new_bounce_count::count;
                                generate flatten(new_bounce_count::domain), CONCAT((chararray)((float)bounceCount/pvCount * 100),'%');};
joinReturnBounceCount       =  join return_bounce_count by domain, return_pu_count by domain;
return_bounce_rate          =  foreach joinReturnBounceCount {pvCount = return_pu_count::pv; 
                                    bounceCount = return_bounce_count::count;
                                    generate flatten(return_bounce_count::domain), CONCAT((chararray)((float)bounceCount/pvCount * 100),'%');};

--    store Domain bounce rate into hbase
STORE domain_bounce_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');
STORE new_bounce_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newbr');

STORE return_bounce_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retbr');


--    compute the customer loyalty 忠诚度
groupDomainWid2            =   foreach groupDomainWid generate $0, $1.wid;
domainWidCount             =   foreach groupDomainWid2 generate myConcat($time, $0.domain) as domain, COUNT($1) as count;
filterTwoCount             =   filter domainWidCount by count == 2;                                          -- 2 page
filterThreeCount           =   filter domainWidCount by count == 3;                                          -- 3 page
filterFourCount            =   filter domainWidCount by count == 4;                                          -- 4 page
filterFiveToTenCount       =   filter domainWidCount by count >= 5 and count <= 10;                          -- 5 - 10 page
filterElevToTwetyCount     =   filter domainWidCount by count >= 11 and count <= 20;                         -- 11 - 20 page
filterTwty1ToFifCount      =   filter domainWidCount by count >= 21 and count <= 50;                         -- 21 - 50 page
filterMoreFiftyCount       =   filter domainWidCount by count > 50;                                          -- >50 page


one_page_count_adv         =  foreach one_page_count generate domain, count;          
joinDomainOnePage          =  join one_page_count_adv by domain, domain_pu_count by domain;
one_page_rate              =  foreach joinDomainOnePage {uvCount = domain_pu_count::uv;  
                                 pcount = one_page_count_adv::count;
                                 generate one_page_count_adv::domain, CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupTwoCount              =  group filterTwoCount by domain;
two_page_count             =  foreach groupTwoCount generate $0, COUNT($1) as count;            -- domain 2 page visit count
joinDomainTwoPage          =  join two_page_count by group, domain_pu_count by domain;
two_page_rate              =  foreach joinDomainTwoPage {uvCount = domain_pu_count::uv;  
                                 pcount = two_page_count::count;
                                 generate two_page_count::group, CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupThreeCount            =  group filterThreeCount by domain;
three_page_count           =  foreach groupThreeCount generate $0, COUNT($1) as count;          -- domain 3 page visit count
joinDomainThreePage        =  join three_page_count by group, domain_pu_count by domain;
three_page_rate            =  foreach joinDomainThreePage {uvCount = domain_pu_count::uv;  
                                 pcount = three_page_count::count;
                                 generate three_page_count::group, CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupFourCount             =  group filterFourCount by domain;
four_page_count            =  foreach groupFourCount generate $0, COUNT($1) as count;           -- domain 4 page visit count
joinDomainFourPage         =  join four_page_count by group, domain_pu_count by domain;
four_page_rate             =  foreach joinDomainFourPage {uvCount = domain_pu_count::uv;  
                                 pcount = four_page_count::count;
                                 generate four_page_count::group, CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupFiveToTenCount        =  group filterFiveToTenCount by domain;
fiveten_page_count         =  foreach groupFiveToTenCount generate $0, COUNT($1) as count;      -- domain 5 - 10 page visit count
joinDomainFiveTenPage      =  join fiveten_page_count by group, domain_pu_count by domain;
fiveten_page_rate          =  foreach joinDomainFiveTenPage {uvCount = domain_pu_count::uv;  
                                 pcount = fiveten_page_count::count;
                                 generate fiveten_page_count::group, CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupElevToTwetyCount      =  group filterElevToTwetyCount by domain;
elevtwenty_page_count      =  foreach groupElevToTwetyCount generate $0, COUNT($1) as count;    -- domain 11 - 20 page visit count
joinDomainElevTwentyPage   =  join elevtwenty_page_count by group, domain_pu_count by domain;
elevtwenty_page_rate       =  foreach joinDomainElevTwentyPage {uvCount = domain_pu_count::uv;  
                                 pcount = elevtwenty_page_count::count;
                                 generate elevtwenty_page_count::group, CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupTwty1ToFifCount       =  group filterTwty1ToFifCount by domain;
twty1fif_page_count        =  foreach groupTwty1ToFifCount generate $0, COUNT($1) as count;      -- domain 21 - 50 page visit count
joinDomainTwty1FifPage     =  join twty1fif_page_count by group, domain_pu_count by domain;
twty1fif_page_rate         =  foreach joinDomainTwty1FifPage {uvCount = domain_pu_count::uv; 
                                 pcount = twty1fif_page_count::count;
                                 generate twty1fif_page_count::group, CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupMoreFiftyCount        =  group filterMoreFiftyCount by domain;
morefifty_page_count       =  foreach groupMoreFiftyCount generate $0, COUNT($1) as count;      -- domain >50 page visit count
joinDomainMoreFiftyPage    =  join morefifty_page_count by group, domain_pu_count by domain;
morefifty_page_rate        =  foreach joinDomainMoreFiftyPage {uvCount = domain_pu_count::uv;  
                                 pcount = morefifty_page_count::count;
                                 generate morefifty_page_count::group, CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

--    store Domain customer loyalty into hbase
STORE one_page_count_adv INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:1:count');
STORE one_page_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:1:rate');

STORE two_page_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:2:count');
STORE two_page_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:2:rate');

STORE three_page_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:3:count');
STORE three_page_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:3:rate');

STORE four_page_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:4:count');
STORE four_page_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:4:rate');

STORE fiveten_page_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:5-10:count');
STORE fiveten_page_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:5-10:rate');

STORE elevtwenty_page_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:11-20:count');
STORE elevtwenty_page_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:11-20:rate');

STORE twty1fif_page_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:21-50:count');
STORE twty1fif_page_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:21-50:rate');

STORE morefifty_page_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:50:count');
STORE morefifty_page_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:50:rate');


--  account page stay average duration
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, referer:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, 
                                                new:int, wb:int, se:chararray);
B1                          =  foreach B generate referer, domain, page_avg_time, new, wb;
groupDoaminPT               =  group B1 by domain;
domain_avg_times            =  foreach groupDoaminPT generate myConcat($time, $0), myTime(SUM($1.page_avg_time)/COUNT($1));

-- return
return_avg_times            =  foreach groupDoaminPT {FB = filter B1 by new is null; generate myConcat($time, $0), myTime(SUM(FB.page_avg_time)/COUNT(FB));};
-- new
new_avg_times               =  foreach groupDoaminPT {FB = filter B1 by new is not null; generate myConcat($time, $0), myTime(SUM(FB.page_avg_time)/COUNT(FB));};

STORE domain_avg_times INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');

STORE new_avg_times INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newavgtime');

STORE return_avg_times INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retavgtime');


----=---    filter by ref      来源过滤
----   外部链接
-- pv uv
return_wb_pu_count          =  foreach groupDomainPU {FA = filter $1 by (new is null) AND (wb == 1); uid = FA.wid; uniq_wid = distinct uid;
                                    generate myConcat($time, $0, 'wb') as domain, COUNT(FA) as pv, COUNT(uniq_wid) as uv;};
new_wb_pu_count             =  foreach groupDomainPU {FA = filter $1 by (new is not null) AND (wb == 1); uid = FA.wid; uniq_wid = distinct uid;
                                    generate myConcat($time, $0, 'wb') as domain, COUNT(FA) as pv, COUNT(uniq_wid) as uv;};
-- bounce rate 跳出率
returnWBOneCount            =  filter filterOneCount by myIsEmpty($1.new) AND (myToString($1.wb) == '1');
returnWBOneCountAdv         =  foreach returnWBOneCount generate $0.domain;
groupReturnWBOneCount       =  group returnWBOneCountAdv by domain;
return_wb_one_count         =  foreach groupReturnWBOneCount generate myConcat($time, $0, 'wb') as domain, COUNT($1) as count;
joinRetWBCount              =  join return_wb_one_count by domain, return_wb_pu_count by domain;
return_wb_bounce_rate       =  foreach joinRetWBCount {pvCount = return_wb_pu_count::pv; bounceCount = return_wb_one_count::count;
                                    generate return_wb_one_count::domain, CONCAT((chararray)((float)bounceCount/pvCount * 100),'%');};

newWBOneCount               =  filter filterOneCount by not myIsEmpty($1.new) AND (myToString($1.wb) == '1');
newWBOneCountAdv            =  foreach newWBOneCount generate $0.domain;
groupNewWBOneCount          =  group newWBOneCountAdv by domain;
new_wb_one_count            =  foreach groupNewWBOneCount generate myConcat($time, $0, 'wb') as domain, COUNT($1) as count;
joinRetWBCount              =  join new_wb_one_count by domain, new_wb_pu_count by domain;
new_wb_bounce_rate          =  foreach joinRetWBCount {pvCount = new_wb_pu_count::pv; bounceCount = new_wb_one_count::count;
                                    generate new_wb_one_count::domain, CONCAT((chararray)((float)bounceCount/pvCount * 100),'%');};
-- 平均访问时长
return_wb_avg_times         =  foreach groupDoaminPT {FB = filter B1 by (new is null) AND (wb == 1); 
                                    generate myConcat($time, $0, 'wb'), myTime(SUM(FB.page_avg_time)/COUNT(FB));};
new_wb_avg_times            =  foreach groupDoaminPT {FB = filter B1 by (new is not null) AND (wb == 1); 
                                    generate myConcat($time, $0, 'wb'), myTime(SUM(FB.page_avg_time)/COUNT(FB));};

----    直接访问
-- pv uv
return_zj_pu_count          =  foreach groupDomainPU {FA = filter $1 by (new is null) AND (referer == '-'); uid = FA.wid; uniq_wid = distinct uid;
                                    generate myConcat($time ,$0, 'zj') as domain, COUNT(FA) as pv, COUNT(uniq_wid) as uv;};
new_zj_pu_count             =  foreach groupDomainPU {FA = filter $1 by (new is not null) AND (referer == '-'); uid = FA.wid; uniq_wid = distinct uid;
                                    generate myConcat($time, $0, 'zj') as domain, COUNT(FA) as pv, COUNT(uniq_wid) as uv;};

-- bounce rate 跳出率
returnZJOneCount            =  filter filterOneCount by myIsEmpty($1.new) AND (myToString($1.referer) == '-');
returnZJOneCountAdv         =  foreach returnZJOneCount generate $0.domain;
groupReturnZJOneCount       =  group returnZJOneCountAdv by domain;
return_zj_one_count         =  foreach groupReturnZJOneCount generate myConcat($time, $0, 'zj') as domain, COUNT($1) as count;
joinRetZJCount              =  join return_zj_one_count by domain, return_zj_pu_count by domain;
return_zj_bounce_rate       =  foreach joinRetZJCount {pvCount = return_zj_pu_count::pv; bounceCount = return_zj_one_count::count;
                                    generate return_zj_one_count::domain, CONCAT((chararray)((float)bounceCount/pvCount * 100),'%');};

newZJOneCount               =  filter filterOneCount by not myIsEmpty($1.new) AND (myToString($1.referer) == '-');
newZJOneCountAdv            =  foreach newZJOneCount generate $0.domain;
groupNewZJOneCount          =  group newZJOneCountAdv by domain;
new_zj_one_count            =  foreach groupNewZJOneCount generate myConcat($time, $0, 'zj') as domain, COUNT($1) as count;
joinRetZJCount              =  join new_zj_one_count by domain, new_zj_pu_count by domain;
new_zj_bounce_rate          =  foreach joinRetZJCount {pvCount = new_zj_pu_count::pv; bounceCount = new_zj_one_count::count;
                                    generate new_zj_one_count::domain, CONCAT((chararray)((float)bounceCount/pvCount * 100),'%');};
-- 平均访问时长
return_zj_avg_times         =  foreach groupDoaminPT {FB = filter B1 by (new is null) AND (referer == '-');
                                    generate myConcat($time, $0, 'zj'), myTime(SUM(FB.page_avg_time)/COUNT(FB));};
new_zj_avg_times            =  foreach groupDoaminPT {FB = filter B1 by (new is not null) AND (referer == '-');
                                    generate myConcat($time, $0, 'zj'), myTime(SUM(FB.page_avg_time)/COUNT(FB));};

STORE return_wb_pu_count INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retpv vis:retuv');
STORE new_wb_pu_count INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newpv vis:newuv');
STORE return_wb_bounce_rate INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retbr');
STORE new_wb_bounce_rate INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newbr');
STORE return_wb_avg_times INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retavgtime');
STORE new_wb_avg_times INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newavgtime');

STORE return_zj_pu_count INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retpv vis:retuv');
STORE new_zj_pu_count INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newpv vis:newuv');
STORE return_zj_bounce_rate INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retbr');
STORE new_zj_bounce_rate INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:nezjr');
STORE return_zj_avg_times INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retavgtime');
STORE new_zj_avg_times INTO 'hbase://kpi_gjsx' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newavgtime');



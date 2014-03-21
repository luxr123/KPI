--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 5;
SET job.name 'KPI_ADS_D';


REGISTER ../lib/kpi.jar
DEFINE myConcat       com.jobs.pig.udf.ConcatUDF;
DEFINE myFilterCount  com.jobs.pig.udf.FilterCountUDF;
DEFINE myFloat        com.jobs.pig.udf.MyFloat;
------------------------------------------------------------------------------------------------
------------------------------------- Ads Page ---------------------------------------------
------------------------------------------------------------------------------------------------

E = load '$inputAds' using PigStorage('`') as (ip:chararray, wid:chararray, domain:chararray, new:int, adsref:chararray, adsnew:int, adsid:chararray);
--  ads pv uv
E1                         =  foreach E generate ip, wid, domain, new, adsid;
ads_pu_count               =  foreach (group E1 by (domain, adsid)) 
                                    {   FE = filter $1 by new is null; 
                                     retIP = distinct FE.ip; retWid = distinct FE.wid;
                                        ip = distinct $1.ip;    wid = distinct $1.wid;
                                     retpv = COUNT(FE);       retuv = COUNT(retWid); retip = COUNT(retIP);
                                        pv = COUNT($1);          uv = COUNT(wid);       ip = COUNT(ip);
                                generate myConcat($time, $0.domain, $0.adsid) as domain,
                                                                      pv as pv,              uv as uv,              ip as ip,
                                                                   retpv as retpv,        retuv as retuv,        retip as retip,
                                                            (pv - retpv) as newpv, (uv - retuv) as newuv, (ip - retip) as newip ;};

--    compute the ads bounce rate
groupAdsWid               =  group E1 by (wid, domain, adsid);
filterOneCount            =  filter groupAdsWid by myFilterCount($1, 1);
filterOneCountAdv         =  foreach filterOneCount generate $0.domain as domain, $0.adsid as ads, flatten($1.new) as new;
groupOneCount             =  group filterOneCountAdv by (domain, ads);
one_page_count            =  foreach groupOneCount { FE = filter $1 by new is null; retcount = COUNT(FE); count = COUNT($1);
                                generate myConcat($time, $0.domain, $0.ads) as domain,
                                                                count as count, retcount as retcount, (count - retcount) as newcount;};
joinAdsBounceCount        =  join one_page_count by domain, ads_pu_count by domain;
ads_bounce_rate           =  foreach joinAdsBounceCount  
                                    {    pv = ads_pu_count::pv;       bc = one_page_count::count;
                                      retpv = ads_pu_count::retpv; retbc = one_page_count::retcount;
                                      newpv = ads_pu_count::newpv; newbc = one_page_count::newcount;
                                generate one_page_count::domain, myFloat((float)bc/pv),
                                                                 myFloat((float)retbc/retpv),
                                                                 myFloat((float)newbc/newpv);};

STORE ads_pu_count INTO 'hbase://kpi_ads' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');
--    store Ads bounce rate into hbase
STORE ads_bounce_rate INTO 'hbase://kpi_ads' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');

----------------------------------
--    compute the customer loyalty 忠诚度^?
----------------------------------

adsWidCount               =  foreach groupAdsWid generate $0.domain, $0.adsid, COUNT($1) as count;
groupAdsWidCount          =  group adsWidCount by (domain, adsid);
loyalty_count             =  foreach (group adsWidCount by (domain, adsid)) 
                                                      { Two          =  filter $1 by count == 2;
                                                        Three        =  filter $1 by count == 3;
                                                        Four         =  filter $1 by count == 4;
                                                        Five2Ten     =  filter $1 by count >= 5  and count <= 10;
                                                        Elev2Twety   =  filter $1 by count >= 11 and count <= 20;
                                                        Twty12Fifty  =  filter $1 by count >= 21 and count <= 50;
                                                        MoreFifty    =  filter $1 by count > 50;
                                generate myConcat($time, $0.domain, $0.adsid) as domain, 
                                                        COUNT(Two)        as two,        COUNT(Three)       as three,
                                                        COUNT(Four)       as four,       COUNT(Five2Ten)    as five2ten,
                                                        COUNT(Elev2Twety) as elev2twety, COUNT(Twty12Fifty) as twty12fifty,
                                                        COUNT(MoreFifty)  as morefifty; };

loyalty_1_count           =  foreach one_page_count generate domain, count;
loyalty_1_rate            =  foreach joinAdsBounceCount { uv  = ads_pu_count::uv;
                                                          one = one_page_count::count;
                                 generate one_page_count::domain, CONCAT((chararray)((float)one/uv * 100),'%');};

loyalty_rate              =  foreach (join loyalty_count by domain, ads_pu_count by domain) 
                                                         {          uv = ads_pu_count::uv;                  two = loyalty_count::two;
                                                                 three = loyalty_count::three;             four = loyalty_count::four;
                                                              five2ten = loyalty_count::five2ten;    elev2twety = loyalty_count::elev2twety;
                                                           twty12fifty = loyalty_count::twty12fifty;  morefifty = loyalty_count::morefifty;
                                generate loyalty_count::domain, myFloat((float)two/uv),
                                                                myFloat((float)three/uv),
                                                                myFloat((float)four/uv),
                                                                myFloat((float)five2ten/uv),
                                                                myFloat((float)elev2twety/uv),
                                                                myFloat((float)twty12fifty/uv),
                                                                myFloat((float)morefifty/uv);};

--    store Ads customer loyalty into hbase
STORE loyalty_1_count INTO 'hbase://kpi_ads' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:1:count');
STORE loyalty_1_rate INTO 'hbase://kpi_ads' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:1:rate');

STORE loyalty_count INTO 'hbase://kpi_ads' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:2:count loty:3:count loty:4:count loty:5-10:count loty:11-20:count loty:21-50:count loty:50:count');
STORE loyalty_rate INTO 'hbase://kpi_ads' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:2:rate loty:3:rate loty:4:rate loty:5-10:rate loty:11-20:rate loty:21-50:rate loty:50:rate');

----------------------------------------------------平均访问时长---------------------------------
--  account page stay average duration
--------------------------------------------
B  =  load '$inputAdsPT' using PigStorage('`') as (domain:chararray, adsid:chararray, new:int, page_avg_time:long);
ads_avg_times             =  foreach (group B by (domain, adsid)) 
                                { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0.domain, $0.adsid),
                                          (long) AVG($1.page_avg_time),
                                          (long) AVG(FBRet.page_avg_time),
                                          (long) AVG(FBNew.page_avg_time);};

STORE ads_avg_times INTO 'hbase://kpi_ads' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');


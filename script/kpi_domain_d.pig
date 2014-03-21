--
-- File: kpi_h.pig (Xiaorui Lu)
--
--------------------------------------------------------------------------------------------------------------------------------------------------------
---------------域名下的pv, uv, ip, 平均访问时长, (包括新老访客); 以及--忠诚度 ----------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------------
SET job.priority VERY_HIGH;
SET default_parallel 10;
SET job.name 'KPI_DOMAIN_D';

REGISTER ../lib/kpi.jar
DEFINE myConcat       com.jobs.pig.udf.ConcatUDF;
DEFINE myFilterCount  com.jobs.pig.udf.FilterCountUDF;
DEFINE myFloat        com.jobs.pig.udf.MyFloat;
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
                                generate one_page_count::domain,myFloat((float)bc/pv),
                                                                myFloat((float)retbc/retpv),
                                                                myFloat((float)newbc/newpv);};

STORE domain_pu_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip vis:retpv vis:retuv vis:retip vis:newpv vis:newuv vis:newip');
--    store Domain bounce rate into hbase
STORE domain_bounce_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br vis:retbr vis:newbr');

----------------------------------
--    compute the customer loyalty 忠诚度
----------------------------------

domainWidCount             =  foreach groupDomainWid generate $0.domain as domain, COUNT($1) as count;
groupDomainWidCount        =  group domainWidCount by domain;
loyalty_count              =  foreach groupDomainWidCount { Two          =  filter $1 by count == 2;
                                                            Three        =  filter $1 by count == 3;
                                                            Four         =  filter $1 by count == 4;
                                                            Five2Ten     =  filter $1 by count >= 5  and count <= 10;
                                                            Elev2Twety   =  filter $1 by count >= 11 and count <= 20;
                                                            Twty12Fifty  =  filter $1 by count >= 21 and count <= 50;
                                                            MoreFifty    =  filter $1 by count > 50;
                                generate myConcat($time, $0) as domain, COUNT(Two)        as two,        COUNT(Three)       as three,
                                                                        COUNT(Four)       as four,       COUNT(Five2Ten)    as five2ten, 
                                                                        COUNT(Elev2Twety) as elev2twety, COUNT(Twty12Fifty) as twty12fifty, 
                                                                        COUNT(MoreFifty)  as morefifty; }; 

loyalty_1_count             =  foreach one_page_count generate domain, count;
loyalty_1_rate              =  foreach joinDomainBounceCount { uv  = domain_pu_count::uv;
                                                               one = one_page_count::count;
                                 generate one_page_count::domain, CONCAT((chararray)((float)one/uv * 100),'%');};

joinDomainWidCount         =  join loyalty_count by domain, domain_pu_count by domain;
loyalty_rate               =  foreach joinDomainWidCount { uv          = domain_pu_count::uv;         two        = loyalty_count::two;
                                                           three       = loyalty_count::three;        four       = loyalty_count::four;
                                                           five2ten    = loyalty_count::five2ten;     elev2twety = loyalty_count::elev2twety;
                                                           twty12fifty = loyalty_count::twty12fifty;  morefifty  = loyalty_count::morefifty;
                                generate loyalty_count::domain, myFloat((float)two/uv),
                                                                myFloat((float)three/uv),
                                                                myFloat((float)four/uv),
                                                                myFloat((float)five2ten/uv),
                                                                myFloat((float)elev2twety/uv),
                                                                myFloat((float)twty12fifty/uv),
                                                                myFloat((float)morefifty/uv);};

--    store Domain customer loyalty into hbase
STORE loyalty_1_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:1:count');
STORE loyalty_1_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:1:rate');

STORE loyalty_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:2:count loty:3:count loty:4:count loty:5-10:count loty:11-20:count loty:21-50:count loty:50:count');
STORE loyalty_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('loty:2:rate loty:3:rate loty:4:rate loty:5-10:rate loty:11-20:rate loty:21-50:rate loty:50:rate');

----------------------------------------------------平均访问时长---------------------------------
--  account page stay average duration
--------------------------------------------
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, ref:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, enter:chararray,
                                                                                                                     new:int, se:chararray, wb:int, kw:chararray);
B1                          =  foreach B generate domain, page_avg_time, new;
groupDoaminPT               =  group B1 by domain;
domain_avg_times            =  foreach groupDoaminPT { FBRet = filter $1 by new is null; FBNew = filter $1 by new is not null;
                                  generate myConcat($time, $0),
                                           (long) AVG($1.page_avg_time),
                                           (long) AVG(FBRet.page_avg_time),
                                           (long) AVG(FBNew.page_avg_time);};

STORE domain_avg_times INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime vis:retavgtime vis:newavgtime');




--
-- File: kpi_h.pig (Xiaorui Lu)
--
set job.priority HIGH

------------------------------------------------------------------------------------------------
--------------------------------------------- Domain -------------------------------------------
------------------------------------------------------------------------------------------------
--     load the filter data
A = load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, referer:chararray, domain:chararray, 
                                             jobPage:chararray, vtime:chararray, respondomain:chararray, new:chararray, enter:chararray);

register ../lib/kpi.jar
define myConcat com.jobs.pig.UDF.ConcatUDF;

--     account the domain pv count
groupDomainPV    =  group A by myConcat(domain, $time);

domain_pv_count  =  foreach groupDomainPV generate flatten($0), COUNT($1) as count;

--     store Domain PV into hbase
STORE domain_pv_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv');

--     account the domain uv count
domain_uv_count  =  FOREACH groupDomainPV {uid = A.wid; uniq_wid = distinct uid; generate flatten(group), COUNT(uniq_wid) as count;};

--    store Domain UV into hbase
STORE domain_uv_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');

--    compute the domain bounce rate
groupDomainWid               =  group A by (domain, wid);                                                      -- page and respondomain bounce rate can also use it
domainWidCount               =  foreach groupDomainWid generate flatten($1), COUNT($1) as count;              
filterUniqDomainWid          =  filter domainWidCount by count == 1;                                           -- 1 page, -- page and respondomain bounce rate can also use it

--    compute the new/return customer (pv, uv, bounce rate, avg time)

filterReturnPU               =  filter A by new != '1';
-- pv
groupReturnPV                =  group filterReturnPU by myConcat(domain, $time);
return_pv_count              =  foreach groupReturnPV generate flatten($0), COUNT($1) as count;

joinDomainReturnPV           =  join domain_pv_count by group left outer, return_pv_count by group;
new_pv_count                 =  foreach joinDomainReturnPV {allPV = domain_pv_count::count;
                                 returnPV = ((return_pv_count::count is null) ? 0 : return_pv_count::count);
                                 generate flatten(domain_pv_count::group), allPV - returnPV;};

STORE new_pv_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newpv');

STORE return_pv_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retpv');
-- uv
return_uv_count              =  FOREACH groupReturnPV {uid = filterReturnPU.wid; uniq_wid = distinct uid; generate flatten(group), COUNT(uniq_wid) as count;};

joinDomainReturnUV           =  join domain_uv_count by group left outer, return_uv_count by group;
new_uv_count                 =  foreach joinDomainReturnUV {allUV = domain_uv_count::count;
                                 returnUV = ((return_uv_count::count is null) ? 0 : return_uv_count::count);
                                 generate flatten(domain_uv_count::group), allUV - returnUV;};

STORE new_uv_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newuv');

STORE return_uv_count INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retuv');

---- bounce rate
firstPageWid                =  filter A by enter == '-';                                                        -- 入口� 页面, 受访域名也会用到

-- return
filterReturnFirstPage       =  filter firstPageWid by new != '1';
groupReturnFirstPage        =  group filterReturnFirstPage by myConcat(domain, $time);
return_load_count           =  foreach groupReturnFirstPage generate flatten($0), COUNT($1) as count;           --  入口� uv
filterReturnUniqWid         =  filter filterUniqDomainWid by A::new != '1';
groupReturnFirstUniq        =  group filterReturnUniqWid by myConcat(A::domain, $time);
return_bounce_count         =  foreach groupReturnFirstUniq generate flatten($0), COUNT($1) as count;
joinReturnBounceCount       =  join return_load_count by group left outer, return_bounce_count by group;
return_bounce_rate          =  foreach joinReturnBounceCount {loadCount = return_load_count::count;
                                bounceCount = ((return_bounce_count::count is null) ? 0 : return_bounce_count::count);
                                generate flatten(return_load_count::group), CONCAT((chararray)((float)bounceCount/loadCount * 100),'%');};
-- new
filterNewFirstPage          =  filter firstPageWid by new == '1';
groupNewFirstPage           =  group filterNewFirstPage by myConcat(domain, $time);
new_load_count              =  foreach groupNewFirstPage generate flatten($0), COUNT($1) as count;                 --  入口访客 uv
filterNewUniqWid            =  filter filterUniqDomainWid by A::new == '1';
groupNewFirstUniq           =  group filterNewUniqWid by myConcat(A::domain, $time);
new_bounce_count            =  foreach groupNewFirstUniq generate flatten($0), COUNT($1) as count;
joinNewBounceCount          =  join new_load_count by group left outer, new_bounce_count by group;
new_bounce_rate             =  foreach joinNewBounceCount {loadCount = new_load_count::count;
                                bounceCount = ((new_bounce_count::count is null) ? 0 : new_bounce_count::count);
                                generate flatten(new_load_count::group), CONCAT((chararray)((float)bounceCount/loadCount * 100),'%');};

STORE new_bounce_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newbt');

STORE return_bounce_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retbt');

---- average visit duration
B = load '$inputPT' using PigStorage('`') as (page:chararray, referer:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, new:chararray);
define myTime com.jobs.pig.UDF.FormatTimeUDF;
-- return
filterReturnPT              =  filter B by new != '1';
groupReturnPT               =  group filterReturnPT by myConcat(domain, $time);
return_avg_times            =  foreach groupReturnPT generate flatten($0), myTime(SUM($1.page_avg_time)/COUNT($1));

-- new
filterNewPT                 =  filter B by new == '1';
groupNewPT                  =  group filterNewPT by myConcat(domain, $time);
new_avg_times               =  foreach groupNewPT generate flatten($0), myTime(SUM($1.page_avg_time)/COUNT($1));

STORE new_avg_times INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:newavgtime');

STORE return_avg_times INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('vis:retavgtime');


--    compute the customer loyalty
domainWidCount2             =   distinct(foreach domainWidCount generate A::domain, A::wid, count);
filterOneDomainWid          =   filter domainWidCount2 by count == 1;                                          -- 1 page
filterTwoDomainWid          =   filter domainWidCount2 by count == 2;                                          -- 2 page
filterThreeDomainWid        =   filter domainWidCount2 by count == 3;                                          -- 3 page
filterFourDomainWid         =   filter domainWidCount2 by count == 4;                                          -- 4 page
filterFiveToTenDomainWid    =   filter domainWidCount2 by count >= 5 and count <= 10;                          -- 5 - 10 page
filterElevToTwetyDomainWid  =   filter domainWidCount2 by count >= 11 and count <= 20;                         -- 11 - 20 page
filterTwty1ToFifDomainWid   =   filter domainWidCount2 by count >= 21 and count <= 50;                         -- 21 - 50 page
filterMoreFiftyDomainWid    =   filter domainWidCount2 by count > 50;                                          -- >50 page

groupOneDomainWid           =  group filterOneDomainWid by myConcat(A::domain, $time);
one_page_count              =  foreach groupOneDomainWid generate flatten($0), COUNT($1) as count;             -- domain 1 page visit count
joinDomainOnePage           =  join domain_uv_count by group left outer, one_page_count by group;
one_page_rate               =  foreach joinDomainOnePage {uvCount = domain_uv_count::count;  
                                 pcount = ((one_page_count::count is null) ? 0 : one_page_count::count);
                                 generate flatten(domain_uv_count::group), CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

joinDomainBounceCount       =  join domain_pv_count by group left outer, one_page_count by group;              -- compute domain bounce rate/pv
domain_bounce_rate          =  foreach joinDomainBounceCount {pvCount = domain_pv_count::count;  
                                 bounceCount = ((one_page_count::count is null) ? 0 : one_page_count::count);
                                 generate flatten(domain_pv_count::group), CONCAT((chararray)((float)bounceCount/pvCount * 100),'%');};


groupTwoDomainWid           =  group filterTwoDomainWid by myConcat(A::domain, $time);
two_page_count              =  foreach groupTwoDomainWid generate flatten($0), COUNT($1) as count;            -- domain 2 page visit count
joinDomainTwoPage           =  join domain_uv_count by group left outer, two_page_count by group;
two_page_rate               =  foreach joinDomainTwoPage {uvCount = domain_uv_count::count;  
                                 pcount = ((two_page_count::count is null) ? 0 : two_page_count::count);
                                 generate flatten(domain_uv_count::group), CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupThreeDomainWid         =  group filterThreeDomainWid by myConcat(A::domain, $time);
three_page_count            =  foreach groupThreeDomainWid generate flatten($0), COUNT($1) as count;          -- domain 3 page visit count
joinDomainThreePage         =  join domain_uv_count by group left outer, three_page_count by group;
three_page_rate             =  foreach joinDomainThreePage {uvCount = domain_uv_count::count;  
                                 pcount = ((three_page_count::count is null) ? 0 : three_page_count::count);
                                 generate flatten(domain_uv_count::group), CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupFourDomainWid          =  group filterFourDomainWid by myConcat(A::domain, $time);
four_page_count             =  foreach groupFourDomainWid generate flatten($0), COUNT($1) as count;           -- domain 4 page visit count
joinDomainFourPage          =  join domain_uv_count by group left outer, four_page_count by group;
four_page_rate              =  foreach joinDomainFourPage {uvCount = domain_uv_count::count;  
                                 pcount = ((four_page_count::count is null) ? 0 : four_page_count::count);
                                 generate flatten(domain_uv_count::group), CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupFiveToTenDomainWid     =  group filterFiveToTenDomainWid by myConcat(A::domain, $time);
fiveten_page_count          =  foreach groupFiveToTenDomainWid generate flatten($0), COUNT($1) as count;      -- domain 5 - 10 page visit count
joinDomainFiveTenPage       =  join domain_uv_count by group left outer, fiveten_page_count by group;
fiveten_page_rate           =  foreach joinDomainFiveTenPage {uvCount = domain_uv_count::count;  
                                 pcount = ((fiveten_page_count::count is null) ? 0 : fiveten_page_count::count);
                                 generate flatten(domain_uv_count::group), CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupElevToTwetyDomainWid   =  group filterElevToTwetyDomainWid by myConcat(A::domain, $time);
elevtwenty_page_count       =  foreach groupElevToTwetyDomainWid generate flatten($0), COUNT($1) as count;    -- domain 11 - 20 page visit count
joinDomainElevTwentyPage    =  join domain_uv_count by group left outer, elevtwenty_page_count by group;
elevtwenty_page_rate        =  foreach joinDomainElevTwentyPage {uvCount = domain_uv_count::count;  
                                 pcount = ((elevtwenty_page_count::count is null) ? 0 : elevtwenty_page_count::count);
                                 generate flatten(domain_uv_count::group), CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupTwty1ToFifDomainWid    =  group filterTwty1ToFifDomainWid by myConcat(A::domain, $time);
twty1fif_page_count         =  foreach groupTwty1ToFifDomainWid generate flatten($0), COUNT($1) as count;      -- domain 21 - 50 page visit count
joinDomainTwty1FifPage      =  join domain_uv_count by group left outer, twty1fif_page_count by group;
twty1fif_page_rate          =  foreach joinDomainTwty1FifPage {uvCount = domain_uv_count::count; 
                                 pcount = ((twty1fif_page_count::count is null) ? 0 : twty1fif_page_count::count);
                                 generate flatten(domain_uv_count::group), CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

groupMoreFiftyDomainWid     =  group filterMoreFiftyDomainWid by myConcat(A::domain, $time);
morefifty_page_count        =  foreach groupMoreFiftyDomainWid generate flatten($0), COUNT($1) as count;      -- domain >50 page visit count
joinDomainMoreFiftyPage     =  join domain_uv_count by group left outer, morefifty_page_count by group;
morefifty_page_rate         =  foreach joinDomainMoreFiftyPage {uvCount = domain_uv_count::count;  
                                 pcount = ((morefifty_page_count::count is null) ? 0 : morefifty_page_count::count);
                                 generate flatten(domain_uv_count::group), CONCAT((chararray)((float)pcount/uvCount * 100),'%');};

--    store Domain bounce rate into hbase
STORE domain_bounce_rate INTO 'hbase://kpi_domain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:bounce_rate');

--    store Domain customer loyalty into hbase
STORE one_page_count INTO 'hbase://kpi_domain' USING
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


------------------------------------------------------------------------------------------------
-------------------------------------------  Domain Page ----------------------------------------------
------------------------------------------------------------------------------------------------

--    store Page PV into hbase
groupPagePU     =  group A by myConcat(domain, $time, page);
page_pv_count   =  foreach groupPagePU generate flatten($0), COUNT($1) as count;
STORE page_pv_count INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv');

--    store Page UV into hbase
page_uv_count   =  foreach groupPagePU {uid = A.wid; uniq_wid = distinct uid; generate flatten(group), COUNT(uniq_wid);};
STORE page_uv_count INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');

--    compute the exit rate
define dist com.jobs.pig.UDF.DistinctUDF();
groupWid        =  group A by wid;
wid_last_page   =  foreach groupWid generate dist(A);
store wid_last_page into 'tmp_wid_last_page';
distA           =  load 'tmp_wid_last_page' using PigStorage(',') as (ip:chararray,wid:chararray,page:chararray,referer:chararray,
                     domain:chararray,jobPage:chararray,vtime:chararray);
--distA = STREAM wid_last_page THROUGH `awk -F "," '{print $1","$2","$3","$4","$5","$6","$7}'` as (ip:chararray,wid:chararray,page:chararray,referer:chararray,domain:chararray,jobPage:chararray,vtime:chararray);
groupPagePE     =  group distA by myConcat(domain, $time, page);
page_pe_count   =  foreach groupPagePE generate flatten($0), COUNT($1) as count;
joinA           =  join page_pv_count by group left outer, page_pe_count by group;
page_pe_rate    =  foreach joinA {pv = page_pv_count::count; pe = ((page_pe_count::count is null) ? 0 : page_pe_count::count); 
                     generate flatten(page_pv_count::group), CONCAT((chararray)((float)pe/pv * 100),'%');};

STORE page_pe_rate INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:exit_rate');


--    compute the bounce rate
groupFirstPageWid   =  group firstPageWid by myConcat(domain, $time, page);

page_load_count     =  foreach groupFirstPageWid generate flatten($0), COUNT($1) as count;    --  入口访客 uv
groupFirstUniqPage  =  group filterUniqDomainWid by myConcat(A::domain, $time, A::page);
page_bounce_count   =  foreach groupFirstUniqPage generate flatten($0), COUNT($1) as count;
joinPageBounceCount =  join page_load_count by group left outer, page_bounce_count by group;
page_bounce_rate    =  foreach joinPageBounceCount {pageLoadCount = page_load_count::count;  
                         pageBounceCount = ((page_bounce_count::count is null) ? 0 : page_bounce_count::count);
                         generate flatten(page_load_count::group), CONCAT((chararray)((float)pageBounceCount/pageLoadCount * 100),'%');};
STORE page_bounce_rate INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:bounce_rate');

--  account page stay average duration
B = load '$inputPT' using PigStorage('`') as (page:chararray, referer:chararray, domain:chararray, page_avg_time:long, respondomain:chararray);
define myTime com.jobs.pig.UDF.FormatTimeUDF;
groupPagePT        =  group B by myConcat(domain, $time, page);
page_avg_times     =  foreach groupPagePT generate flatten($0), myTime(SUM($1.page_avg_time)/COUNT($1));
STORE page_avg_times INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avg_time');


------------------------------------------------------------------------------------------------
----------------------------------------- Page Detail ------------------------------------------
------------------------------------------------------------------------------------------------
--    store the referer page count
groupPageRef      =  group A by myConcat(page,referer);
page_referer      =  foreach groupPageRef generate $0 as page_ref, (chararray)COUNT($1) as count;
page_ref_count    =  foreach page_referer generate myConcat(page_ref, $time, count), (long)count;
STORE page_ref_count INTO 'hbase://kpi_detail' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:count');


------------------------------------------------------------------------------------------------
----------------------------------------- Entry Page -------------------------------------------
------------------------------------------------------------------------------------------------

STORE page_load_count INTO 'hbase://kpi_entry_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');


------------------------------------------------------------------------------------------------
------------------------------------- Respondents domain ---------------------------------------
------------------------------------------------------------------------------------------------
--    store respondomain PV into hbase
groupRespondomain           =  group A by myConcat(domain, $time, respondomain);
respondomain_pv_count       =  foreach groupRespondomain generate flatten($0), COUNT($1) as count;
STORE respondomain_pv_count INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv');


--    store respondomain UV into hbase
respondomain_uv_count       =  foreach groupRespondomain {uid = A.wid; uniq_wid = distinct uid; generate flatten(group), COUNT(uniq_wid);};
STORE respondomain_uv_count INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');

--    compute the bounce rate
groupFirstRespondomainWid   =  group firstPageWid by myConcat(domain, $time, respondomain);
respondomain_load_count     =  foreach groupFirstRespondomainWid generate flatten($0), COUNT($1) as count;      -- all respondomain load count
groupFirstUniqRespondomain  =  group filterUniqDomainWid by myConcat(A::domain, $time, A::respondomain);
respondomain_bounce_count   =  foreach groupFirstUniqRespondomain generate flatten($0), COUNT($1) as count;     -- respondomain bounce count
joinRespondomainBounceCount =  join respondomain_load_count by group left outer, respondomain_bounce_count by group;
respondomain_bounce_rate    =  foreach joinRespondomainBounceCount {respondomainLoadCount = respondomain_load_count::count;  
                                    respondomainBounceCount = ((respondomain_bounce_count::count is null) ? 0 : respondomain_bounce_count::count);
                                    generate flatten(respondomain_load_count::group), CONCAT((chararray)((float)respondomainBounceCount/respondomainLoadCount * 100),'%');};
STORE respondomain_bounce_rate INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:bounce_rate');


--  account respondomain stay average time
groupRespondomainPT         =  group B by myConcat(domain, $time, respondomain);
respondomain_avg_times      =  foreach groupRespondomainPT generate flatten($0), myTime(SUM($1.page_avg_time)/COUNT($1));
STORE respondomain_avg_times INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avg_time');


------------------------------------------------------------------------------------------------
------------------------------------- Thermal Page ---------------------------------------------
------------------------------------------------------------------------------------------------
C = load '$inputThermal' using PigStorage('`') as (page:chararray, click:chararray, wid:chararray);
groupPageClk   =  group C by myConcat(click, page);
pageClkCount   =  foreach groupPageClk generate $0 as pageClk, (chararray)COUNT($1) as count;
clk_count      =  foreach pageClkCount generate myConcat(pageClk, $time), (long)count;
STORE clk_count INTO 'hbase://kpi_heatmap' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:count');


--------------------------------------    Remedial Work     ------------------------------------------------------
rm tmp_wid_last_page;






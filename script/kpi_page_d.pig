--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 5;
SET job.name 'KPI_PAGE_D';


REGISTER ../lib/kpi.jar
DEFINE myConcat            com.jobs.pig.udf.ConcatUDF;
DEFINE CustomFormatToISO   org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix           org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE MaxTupleBy1stField  org.apache.pig.piggybank.evaluation.MaxTupleBy1stField();
DEFINE myFilterCount       com.jobs.pig.udf.FilterCountUDF;
DEFINE myFloat             com.jobs.pig.udf.MyFloat;
------------------------------------------------------------------------------------------------
-------------------------------------------  Domain Page ¿¿¿¿ && ¿¿¿¿ ------------------------
------------------------------------------------------------------------------------------------
--     load the filter data ¿¿
A  =  load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, referer:chararray, domain:chararray, respondomain:chararray,
                                                                                            enter:chararray, new:int, se:chararray, wb:int, kw:chararray, vtime:chararray);
A1              =  foreach A generate ip, wid, page, referer, domain, enter, vtime;

--    store Page PV into hbase
groupPagePU     =  group A1 by (domain, page);
page_pu_count   =  foreach groupPagePU { ip = distinct $1.ip; wid = distinct $1.wid;
                        generate myConcat($time, $0.domain, $0.page) as domain,  COUNT($1) as pv,  COUNT(wid) as uv, COUNT(ip);};

--    compute the exit rate
PEA             =  foreach A1 generate ISOToUnix(CustomFormatToISO(vtime, 'YYYY-MM-dd HH:mm:ss')) AS unixTime:long, wid, page, domain;
groupWid        =  group PEA by wid;
widLastPage     =  foreach groupWid generate flatten(MaxTupleBy1stField(PEA));
groupPagePE     =  group widLastPage by myConcat($time, PEA::domain, PEA::page);
page_pe_count   =  foreach groupPagePE generate $0, COUNT($1) as count;



------------------------------------------------------------------------------------------------
----------------------------------------- Page Detail -¿¿¿¿¿-------------------------------
------------------------------------------------------------------------------------------------
--    store the referer page count
groupPageRef    =  group A1 by (page, referer, domain);
--page_ref_count  =  foreach groupPageRef {count = COUNT($1); generate myConcat($time, count, $0.page, $0.referer), count as count;};
page_ref_count  =  foreach groupPageRef generate myConcat($time, $0.domain, $0.page, $0.referer), COUNT($1) as count;


------------------------------------------------------------------------------------------------
----------------------------------------- Entry Page -------------------------------------------
------------------------------------------------------------------------------------------------
entryPage                   =  filter A1 by enter is null;
groupEntryPage              =  group entryPage by (domain, page);
-- ¿¿¿¿¿--pv
page_load_pu                =  foreach groupEntryPage { ip = distinct $1.ip; wid = distinct $1.wid;
                                    generate myConcat($time, $0.domain, $0.page) as domain, COUNT($1) as pv, COUNT(wid) as uv, COUNT(ip);};


--    ¿¿¿¿¿(¿¿¿¿¿¿¿)
groupPageWid                =  group entryPage by (domain, wid, page);
filterPageOne               =  filter groupPageWid by myFilterCount(entryPage, 1);
filterPageOneAdv            =  foreach filterPageOne generate $0.domain as domain, $0.page as page;
groupPageOne                =  group filterPageOneAdv by (domain, page);
page_bounce_count           =  foreach groupPageOne generate myConcat($time, $0.domain, $0.page) as domain, COUNT($1) as bc;
joinPageBounceCount         =  join page_bounce_count by domain, page_load_pu by domain;
page_bounce_rate            =  foreach joinPageBounceCount { pv = page_load_pu::pv;
                                                             bc = page_bounce_count::bc;
                                    generate page_bounce_count::domain, myFloat((float)bc/pv);};



STORE page_pu_count INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip');
STORE page_pe_count INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:exit_count');

STORE page_ref_count INTO 'hbase://kpi_detail' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:count');

STORE page_load_pu INTO 'hbase://kpi_entry_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip');
STORE page_bounce_rate INTO 'hbase://kpi_entry_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');

------------------------------------------------------------------------------------------------
----------------------------------------- respondomain ¿¿¿¿---------------------------------
------------------------------------------------------------------------------------------------
--     account the domain pv count
A2                          =  foreach A generate ip, wid, domain, respondomain;
--    store respondomain PV into hbase
groupRespondomain           =  group A2 by (domain, respondomain);
respondomain_pu_count       =  foreach groupRespondomain { ip = distinct $1.ip; wid = distinct $1.wid;
                                    generate myConcat($time, $0.domain, $0.respondomain) as domain, COUNT($1) as pv, COUNT(wid) as uv, COUNT(ip) as ip;};

--    compute the bounce rate
groupRespondomainWid        =  group A2 by (domain, wid, respondomain);
filterRespondomainOne       =  filter groupRespondomainWid by myFilterCount(A2, 1);
filterRespondomainOneAdv    =  foreach filterRespondomainOne generate $0.domain as domain, $0.respondomain as respondomain;
groupRespondomainOne        =  group filterRespondomainOneAdv by (domain, respondomain);
respondomain_bounce_count   =  foreach groupRespondomainOne generate myConcat($time, $0.domain, $0.respondomain) as domain, COUNT($1) as bc;
joinRespondomainBounceCount =  join respondomain_bounce_count by domain, respondomain_pu_count by domain;
respondomain_bounce_rate    =  foreach joinRespondomainBounceCount { pv = respondomain_pu_count::pv;
                                                                     bc = respondomain_bounce_count::bc;
                                    generate respondomain_bounce_count::domain, myFloat((float)bc/pv);};

STORE respondomain_pu_count INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv cf:ip');
STORE respondomain_bounce_rate INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');

-------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------¿¿¿¿¿¿ (¿¿¿¿, ¿¿¿¿, ¿¿¿¿)------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------
--  account page stay average duration
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, referer:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, enter:chararray);

B1                         =  foreach B generate page, domain, page_avg_time, enter;
groupPagePT                =  group B1 by (domain, page);
page_avg_times             =  foreach groupPagePT generate myConcat($time, $0.domain, $0.page), (long) AVG($1.page_avg_time);

entryPagePT                =  filter B1 by enter is null;
groupEntryPagePT           =  group entryPagePT by (domain, page);
entry_page_avg_times       =  foreach groupEntryPagePT generate myConcat($time, $0.domain, $0.page), (long) AVG($1.page_avg_time);

B2                         =  foreach B generate domain, page_avg_time, respondomain;
groupRespondomainPT        =  group B2 by (domain, respondomain);
respondomain_avg_times     =  foreach groupRespondomainPT generate myConcat($time, $0.domain, $0.respondomain), (long) AVG($1.page_avg_time);


STORE page_avg_times INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');

STORE entry_page_avg_times INTO 'hbase://kpi_entry_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');

STORE respondomain_avg_times INTO 'hbase://kpi_respondomain' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');

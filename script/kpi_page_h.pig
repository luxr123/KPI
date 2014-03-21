--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 20;
SET job.name 'KPI_PAGE_H';



------------------------------------------------------------------------------------------------
-------------------------------------------  Domain Page ----------------------------------------------
------------------------------------------------------------------------------------------------
--     load the filter data
A   =  load '$inputPU' using PigStorage('`') as (ip:chararray, wid:chararray, page:chararray, referer:chararray, domain:chararray, 
                                             jobPage:chararray, vtime:chararray, respondomain:chararray, new:chararray, enter:chararray);
A1  =  foreach A generate wid, page, referer, domain, vtime, enter;
register ../lib/kpi.jar
DEFINE myConcat com.jobs.pig.UDF.ConcatUDF;

--    store Page PV into hbase
groupPagePU     =  group A1 by myConcat($time, domain, page);
page_pv_count   =  foreach groupPagePU generate flatten($0), COUNT($1) as count;
STORE page_pv_count INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv');

--    store Page UV into hbase
page_uv_count   =  foreach groupPagePU {uid = A1.wid; uniq_wid = distinct uid; generate flatten(group), COUNT(uniq_wid);};
STORE page_uv_count INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');

--    compute the exit rate
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE MaxTupleBy1stField org.apache.pig.piggybank.evaluation.MaxTupleBy1stField();
PEA             =  foreach A1 generate ISOToUnix(CustomFormatToISO(vtime, 'YYYY-MM-dd HH:mm:ss')) AS unixTime:long, wid, page, domain;
groupWid        =  group PEA by wid;
widLastPage     =  FOREACH groupWid GENERATE flatten(MaxTupleBy1stField(PEA));
groupPagePE     =  group widLastPage by myConcat($time, PEA::domain, PEA::page);
page_pe_count   =  foreach groupPagePE generate flatten($0), COUNT($1) as count;
joinA           =  join page_pv_count by group left outer, page_pe_count by group;
page_pe_rate    =  foreach joinA {pv = page_pv_count::count; pe = ((page_pe_count::count is null) ? 0 : page_pe_count::count); 
                     generate flatten(page_pv_count::group), CONCAT((chararray)((float)pe/pv * 100),'%');};

STORE page_pe_count INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:exit_count');
STORE page_pe_rate INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:exit_rate');

--  account page stay average duration
B   =  load '$inputPT' using PigStorage('`') as (page:chararray, referer:chararray, domain:chararray, page_avg_time:long, respondomain:chararray);
B1  =  foreach B generate page, domain, page_avg_time;
DEFINE myTime com.jobs.pig.UDF.FormatTimeUDF;
groupPagePT        =  group B1 by myConcat($time, domain, page);
page_avg_times     =  foreach groupPagePT generate flatten($0), myTime(SUM($1.page_avg_time)/COUNT($1));
STORE page_avg_times INTO 'hbase://kpi_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');


------------------------------------------------------------------------------------------------
----------------------------------------- Page Detail ------------------------------------------
------------------------------------------------------------------------------------------------
--    store the referer page count
groupPageRef      =  group A1 by myConcat(page,referer);
page_ref_count    =  foreach groupPageRef {count = COUNT($1); key = myConcat($time, count, $0); generate key, count as count;};
STORE page_ref_count INTO 'hbase://kpi_detail' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:count');


------------------------------------------------------------------------------------------------
----------------------------------------- Entry Page -------------------------------------------
------------------------------------------------------------------------------------------------
firstPageWid          =  filter A1 by enter == '-';
groupFirstPageWid     =  group firstPageWid by myConcat($time, domain, page);
page_load_count       =  foreach groupFirstPageWid generate $0, COUNT($1) as count;    -- 着陆页�

STORE page_load_count INTO 'hbase://kpi_entry_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv');

groupDomainWid        =  group A1 by (domain, wid, page);
domainWidCount        =  foreach groupDomainWid generate $0.domain as domain, $0.page as page, COUNT($1) as count;
filterOneCount        =  filter domainWidCount by count == 1;                                          -- 1 count
groupOneCount         =  group filterOneCount by myConcat($time, domain, page);
page_bounce_count     =  foreach groupOneCount generate flatten($0), COUNT($1) as count;
joinPageBounceCount   =  join page_load_count by group left outer, page_bounce_count by group;
page_bounce_rate      =  foreach joinPageBounceCount {pageLoadCount = page_load_count::count;  
                                pageBounceCount = ((page_bounce_count::count is null) ? 0 : page_bounce_count::count);
                                generate flatten(page_load_count::group), CONCAT((chararray)((float)pageBounceCount/pageLoadCount * 100),'%');};

STORE page_bounce_rate INTO 'hbase://kpi_entry_page' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');



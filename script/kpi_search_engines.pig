--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 20;
SET job.name 'KPI_SEARCH_ENGINES';

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
--  account page stay average duration
B  =  load '$inputPT' using PigStorage('`') as (page:chararray, referer:chararray, domain:chararray, page_avg_time:long, respondomain:chararray, 
                                                new:int, wb:int, se:chararray);

---------------------------------------------------------来源分析-------------------------------------------------------------------------------------
--------------------搜索引擎-----------------
-- pv uv
filterAbySE              =  filter A by se is not null;
A2                       =  foreach filterAbySE generate wid, domain, se;
groupSSPU                =  group A2 by myConcat(domain, mySubString(se));
subSSPU                  =  foreach groupSSPU { uid = distinct $1.wid;
                                                w_FA = filter $1 by se matches '^(www\\.).*'; w_uid = distinct w_FA.wid; 
                                                m_FA = filter $1 by se matches '^(m\\.).*';   m_uid = distinct m_FA.wid; 
                                                generate myConcat($time, $0) as domain, COUNT($1) as pv, COUNT(uid) as uv,
                                                         myConcat($time, $0, myToString(w_FA.se)) as w_domain, COUNT(w_FA) as w_pv, COUNT(w_uid) as w_uv, 
                                                         myConcat($time, $0, myToString(m_FA.se)) as m_domain, COUNT(m_FA) as m_pv, COUNT(m_uid) as m_uv;};
ss_pu_count              =  foreach subSSPU generate domain, pv, uv;
ssWPUTmp                 =  foreach subSSPU generate w_domain, w_pv, w_uv;
ss_w_pu_count            =  filter ssWPUTmp by w_pv != 0;
ssMPUTmp                 =  foreach subSSPU generate m_domain, m_pv, m_uv;
ss_m_pu_count            =  filter ssMPUTmp by m_pv != 0;

-- bounce rate
groupSSWid               =  group A2 by (wid, domain, se);
filterSSOne              =  filter groupSSWid by myFilterCount(A2, 1);
filterOneAdv             =  foreach filterSSOne generate $0.domain as domain, $0.se as se;
groupSSOne               =  group filterOneAdv by myConcat(domain,  mySubString(se));
subSSBounceCount         =  foreach groupSSOne {w_FA = filter $1 by se matches '^(www\\.).*'; m_FA = filter $1 by se matches '^(m\\.).*';
                                                generate myConcat($time, $0) as domain, COUNT($1) as count,
                                                         myConcat($time, $0, myToString(w_FA.se)) as w_domain, COUNT(w_FA) as w_count,
                                                         myConcat($time, $0, myToString(m_FA.se)) as m_domain, COUNT(m_FA) as m_count;};
joinSSBounce             =  join subSSBounceCount by domain, subSSPU by domain;
subSSBounceRate          =  foreach joinSSBounce {pv = subSSPU::pv; bc = subSSBounceCount::count;
                                                  w_pv = subSSPU::w_pv; w_bc = subSSBounceCount::w_count;
                                                  m_pv = subSSPU::m_pv; m_bc = subSSBounceCount::m_count;
                                                  generate subSSBounceCount::domain as domain, CONCAT((chararray)((float)bc/pv * 100),'%') as br,
                                                           subSSBounceCount::w_domain as w_domain, CONCAT((chararray)((float)w_bc/w_pv * 100),'%') as w_br,
                                                           subSSBounceCount::m_domain as m_domain, CONCAT((chararray)((float)m_bc/m_pv * 100),'%') as m_br;};
ss_bounce_rate           =  foreach subSSBounceRate generate domain, br;
ssWBRTmp                 =  foreach subSSBounceRate generate w_domain, w_br;
ss_w_bounce_rate         =  filter ssWBRTmp by w_br is not null;
ssMBRTmp                 =  foreach subSSBounceRate generate m_domain, m_br;
ss_m_bounce_rate         =  filter ssMBRTmp by m_br is not null;

-- average time 
filterBbySE              =  filter B by se is not null;
B2                       =  foreach filterBbySE generate domain, page_avg_time, se;
groupSSPT                =  group B2 by myConcat(domain, mySubString(se));
subSSAvgTime             =  foreach groupSSPT {w_FB = filter $1 by se matches '^(www\\.).*'; m_FB = filter $1 by se matches '^(m\\.).*';
                                               generate myConcat($time, $0) as domain, myTime(AVG($1.page_avg_time)) as avgtime,
                                                        myConcat($time, $0, myToString(w_FB.se)) as w_domain, myTime(AVG(w_FB.page_avg_time)) as w_avgtime,
                                                        myConcat($time, $0, myToString(m_FB.se)) as m_domain, myTime(AVG(m_FB.page_avg_time)) as m_avgtime;};
ss_avg_time              =  foreach subSSAvgTime generate domain, avgtime;
ssWAvgTimeTmp            =  foreach subSSAvgTime generate w_domain, w_avgtime;
ss_w_avg_time            =  filter ssWAvgTimeTmp by w_avgtime is not null;
ssMAvgTimeTmp            =  foreach subSSAvgTime generate m_domain, m_avgtime;
ss_w_avg_time            =  filter ssMAvgTimeTmp by m_avgtime is not null;

----   入库hbase
STORE ss_pu_count INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv');
STORE ss_w_pu_count INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv');
STORE ss_m_pu_count INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv');


STORE ss_bounce_rate INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');
STORE ss_w_bounce_rate INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');
STORE ss_m_bounce_rate INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');

STORE ss_avg_time INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');
STORE ss_w_avg_time INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');
STORE ss_w_avg_time INTO 'hbase://kpi_search_engines' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');


--------------------外部链接-----------------
-- pv uv
filterAbyWB              =  filter A by wb == 1;
A3                       =  foreach filterAbyWB generate wid, referer, domain, wb;
groupWBPU                =  group A3 by myConcat(domain, mySubString(referer));
wb_pu                    =  foreach groupWBPU { uid = distinct $1.wid;
                                generate myConcat($time, $0) as domain, COUNT($1) as pv, COUNT(uid) as uv;};
groupSubWBPU             =  group A3 by (domain, referer);
sub_wb_pu                =  foreach groupSubWBPU { uid = distinct $1.wid; 
                                generate myConcat($time, $0.domain, mySubString($0.referer), $0.referer) as domain, COUNT($1) as pv, COUNT(uid) as uv;};

-- bounce rate
groupSubWBWid            =  group A3 by (wid, domain, referer);
filterSubWBOne           =  filter groupSubWBWid by myFilterCount(A3, 1);
filterSubWBOneAdv        =  foreach filterSubWBOne generate $0.domain as domain, flatten($1.referer) as referer;
groupSubWBOneAdv         =  group filterSubWBOneAdv by (domain, referer);
sub_wb_one_count         =  foreach groupSubWBOneAdv generate myConcat($time, $0.domain, mySubString($0.referer), $0.referer) as domain, COUNT($1) as count;
joinSubWBBounce          =  join sub_wb_one_count by domain, sub_wb_pu by domain;
sub_wb_bounce_rate       =  foreach joinSubWBBounce {pv = sub_wb_pu::pv; bc = sub_wb_one_count::count;
                                generate sub_wb_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%');};

groupWBWid               =  group A3 by (wid, domain, mySubString(referer));
filterWBOne              =  filter groupWBWid by myFilterCount(A3, 1);
filterWBOneAdv           =  foreach filterWBOne generate $0.domain as domain, flatten($1.referer) as referer;
groupWBOneAdv            =  group filterWBOneAdv by myConcat(domain, mySubString(referer));
wb_one_count             =  foreach groupWBOneAdv generate myConcat($time, $0) as domain, COUNT($1) as count;
joinWBBounce             =  join wb_one_count by domain, wb_pu by domain;
wb_bounce_rate           =  foreach joinWBBounce {pv = wb_pu::pv; bc = wb_one_count::count;
                                generate wb_one_count::domain, CONCAT((chararray)((float)bc/pv * 100),'%');};

-- average time
filterBbyWB              =  filter B by wb == 1;
B3                       =  foreach filterBbyWB generate domain, page_avg_time, referer;
groupSubWBPT             =  group B3 by (domain, referer);
sub_wb_avgtime           =  foreach groupSubWBPT generate myConcat($time, $0.domain, mySubString($0.referer), $0.referer) as domain, 
                                                                myTime(AVG($1.page_avg_time)) as avgtime;

groupWBPT                =  group B3 by myConcat(domain, mySubString(referer));
wb_avgtime               =  foreach groupWBPT generate myConcat($time, $0) as domain, myTime(AVG($1.page_avg_time)) as avgtime;


----   入库hbase
STORE wb_pu INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv');
STORE sub_wb_pu INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv cf:uv');

STORE wb_bounce_rate INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');
STORE sub_wb_bounce_rate INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:br');

STORE wb_avgtime INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');
STORE sub_wb_avgtime INTO 'hbase://kpi_out_link' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:avgtime');


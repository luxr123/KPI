--
-- File: kpi_h.pig (Xiaorui Lu)
--
SET job.priority VERY_HIGH;
SET default_parallel 5;
SET job.name 'KPI_HEATMAP_D';


register ../lib/kpi.jar
define myConcat com.jobs.pig.udf.ConcatUDF;
------------------------------------------------------------------------------------------------
------------------------------------- Thermal Page ---------------------------------------------
------------------------------------------------------------------------------------------------

C  =  load '$inputHeat' using PigStorage('`') as (page:chararray, click:chararray);
groupPageClk        =  group C by (page, click);
--page_clk_count      =  foreach groupPageClk {count = COUNT($1); key = myConcat($time, count, $0.page, $0.click); generate key, count as count;};
page_clk_count      =  foreach groupPageClk generate myConcat($time, $0.page, $0.click), COUNT($1);
STORE page_clk_count INTO 'hbase://kpi_heatmap' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:count');

------------------------------------------------------------------------------------------------
------------------------------------- Hot Link Page --------------------------------------------
------------------------------------------------------------------------------------------------
D = load '$inputHotLink' using PigStorage('`') as (domain:chararray, page:chararray, x:chararray, y:chararray, u:chararray);
groupLinkD          =  group D by (domain, page, u);
link_info           =  foreach groupLinkD generate myConcat($time, $0.domain, $0.page, $0.u), COUNT($1);

STORE link_info INTO 'hbase://kpi_hotlink' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:count');


groupMapD           =  group D by (domain, page, x, y);
map_info            =  foreach groupMapD generate myConcat($time, $0.domain, $0.page, $0.x, $0.y), COUNT($1);

STORE map_info INTO 'hbase://kpi_hotmap' USING
    org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:count');

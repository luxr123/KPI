--
-- File: test.pig (Xiaorui Lu)
--
set job.priority HIGH


-- Domain
--

--     load the filter data
A = load '$input' using PigStorage('`') as (ip:chararray,wid:chararray,page:chararray,querys:chararray,status:chararray,
    referer:chararray,domain:chararray,jobPage:chararray);

register ../lib/kpi.jar
define myConcat com.jobs.pig.UDF.ConcatUDF;

--     account the domain pv count
groupDomainPV = group A by myConcat(domain);

domain_pv_count = foreach groupDomainPV generate flatten($0), COUNT($1);

--     store Domain PV into hbase
STORE domain_pv_count INTO 'hbase://kpi_domain' USING
org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv');

-- account the domain uv count
domain_uv_count = FOREACH groupDomainPV {uid = A.wid; uniq_wid = distinct uid; generate flatten(group), COUNT(uniq_wid);};

--    store Domain UV into hbase
STORE domain_uv_count INTO 'hbase://kpi_domain' USING
org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');

--
-- Page
--
--    store Page PV into hbase
groupPagePV = group A by myConcat(domain,page);
page_pv_count = foreach groupPagePV generate flatten($0), COUNT($1);
STORE page_pv_count INTO 'hbase://kpi_page' USING
org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');

--    store Page UV into hbase
page_uv_count = foreach groupPagePV {uid = A.wid; uniq_wid = distinct uid; generate flatten(group), COUNT(uniq_wid);};
STORE page_uv_count INTO 'hbase://kpi_page' USING
org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:uv');

--    store the referer page count
groupPageRef = group A by myConcat(page,referer);
page_referer = foreach groupPageRef generate $0 as page_ref, COUNT($1) as count:chararray;
page_ref_count = foreach page_referer generate myConcat(page_ref,count), count as c:long;
STORE page_ref_count INTO 'hbase://kpi_detail' USING
org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:count');


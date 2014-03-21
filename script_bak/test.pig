--
-- File: test.pig (Xiaorui Lu)
--
set job.priority HIGH

--
-- Domain
--

--     load the filter data
A = load '$input' using PigStorage('`') as (ip:chararray,wid:chararray,page:chararray,querys:chararray,status:chararray,
    referer:chararray,domain:chararray,jobPage:chararray);

register ../lib/kpi.jar
define myConcat com.jobs.pig.UDF.ConcatUDF;

--     account the domain pv count
groupDomainPV = group A by myConcat(domain, $time);

domain_pv_count = foreach groupDomainPV generate flatten($0), COUNT($1);

--     store Domain PV into hbase
STORE domain_pv_count INTO 'hbase://kpi_domain' USING
org.apache.pig.backend.hadoop.hbase.HBaseStorage ('cf:pv');

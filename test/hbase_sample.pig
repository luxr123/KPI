raw_data = LOAD 'input.csv' USING PigStorage(',') AS (
listing_id: chararray,
fname: chararray,
lname: chararray );


STORE raw_data INTO 'hbase://sample_names' USING 
org.apache.pig.backend.hadoop.hbase.HBaseStorage (
'info:fname info:lname');

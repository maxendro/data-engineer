CREATE EXTERNAL TABLE kuznetsov.external_final_kuznetsov
(
user_id int,
content_id int,
start_s timestamp,
stop_s timestamp,
channel_id int,
region_id int) 
STORED AS PARQUET LOCATION 'hdfs://vm-dlake2-m-1.test.local/user/kuznetsov/data/final_kuznetsov.parquet';

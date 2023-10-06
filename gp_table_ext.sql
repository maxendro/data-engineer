CREATE EXTERNAL TABLE postgres.public.final_kuznetsov_ext (
	user_id int4,
	content_id int4,
	start_s timestamp,
	stop_s timestamp,
	channel_id int4,
	region_id int4
)
LOCATION (
	'pxf://kuznetsov.external_final_kuznetsov?PROFILE=hive&server=hadoop'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

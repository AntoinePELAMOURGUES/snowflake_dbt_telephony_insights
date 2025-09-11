use role sysadmin;
use schema tel_insights_raw.event_raw;
show warehouses;
use warehouse tel_insights_etl;
list @tel_insights_raw.event_raw.my_s3_stage;
select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23 from @tel_insights_raw.event_raw.my_s3_stage (file_format => mon_format_csv) limit 10;
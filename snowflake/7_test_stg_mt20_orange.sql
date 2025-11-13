use role sysadmin;
use warehouse compute_wh;
use database analytics_dev;
use schema stg;

select * from raw.csv.src_mt24_orange_csv limit 10;

select * from analytics_dev.stg.stg_mt20_orange limit 10;

select * from analytics_dev.stg.stg_mt24_orange limit 10;

select * from analytics_dev.stg.stg_mt20_sfr limit 10;

select * from analytics_dev.stg.stg_mt24_sfr limit 10;

use role sysadmin;
use warehouse compute_wh;
use database analytics_dev;
use schema stg;

select * from analytics_dev.stg.stg_mt20_orange limit 10;

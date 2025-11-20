use role sysadmin;
use warehouse compute_wh;
use database auth_db;

select * from auth_db.prod.users limit 10;

truncate table auth_db.prod.users;

select * from dossiers_db.prod.dossiers limit 10;

select * from raw_data.pnij_src.raw_mt24 limit 10;

truncate table raw_data.pnij_src.raw_mt20;

truncate table raw_data.pnij_src.raw_mt24;

truncate table raw_data.pnij_src.raw_href_bouygues;


select distinct(source_filename) from raw_data.pnij_src.raw_mt20 limit 10;

select * from dossiers_db.prod.files_log;

truncate table dossiers_db.prod.files_log;

select * from RAW_DATA.PNIJ_SRC.RAW_HREF_BOUYGUES limit 10;

drop schema staging.stg_dbt_staging;

select * from staging.dbt_staging.stg_mt20 limit 10;

select * from staging.dbt_staging.stg_href_bouygues limit 10;
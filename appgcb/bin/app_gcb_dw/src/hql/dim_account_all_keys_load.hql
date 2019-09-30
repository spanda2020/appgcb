use app_gcb_stg;
set hive.execution.engine = spark ;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
insert OVERWRITE table dim_all_keys partition(
  dim_tbl_name,
  run_dt,
  prc_run_id
  )
select 'acct_nbr' as primary_key_name ,acct_nbr as primary_key_val,'Y' as active_flag,CURRENT_TIMESTAMP as load_tm,
'dim_account' as dim_tbl_name,source_dt as run_dt,1111 as prc_run_id from l1_gcb_ref.account ;

with all_keys as (select * from app_gcb_stg.dim_all_keys where dim_tbl_name='dim_account'),
dim_acct as (select * from app_gcb_dw.dim_account),
dim_acct_keys as (select primary_key_val,all_keys.run_dt as eff_start_dt,dim_acct.* from all_keys left outer join dim_acct on primary_key_val=acct_nbr and dim_acct.run_dt=all_keys.run_dt)
dim_acct_keys_1 as (select primary_key_val as acct_nbr,
COALESCE(country, LAST_VALUE(country) OVER(partition by primary_key_val order by eff_start_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as country,
COALESCE(country, LAST_VALUE(lvid) OVER(partition by primary_key_val order by eff_start_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as lvid,
COALESCE(country, LAST_VALUE(open_dt) OVER(partition by primary_key_val order by eff_start_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as open_dt,
COALESCE(country, LAST_VALUE(load_tm) OVER(partition by primary_key_val order by eff_start_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as load_tm,
eff_start_dt,prc_run_id from dim_acct_keys order by primary_key_val,eff_start_dt)
create or replace view vw_dim_account as select * from dim_acct_keys_1

(acct_nbr varchar(20),
 country varchar(3),
 lvid varchar(20),
 open_dt varchar(10),
 load_tm string)
 partitioned by (prc_run_id bigint,run_dt VARCHAR(8))

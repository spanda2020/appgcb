use app_gcb_stg;
set hive.execution.engine = spark ;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
insert OVERWRITE table dim_account_src_stg partition(
  prc_run_id,
  run_dt
  )
select acct_nbr ,country,'' as lvid, open_dt,CURRENT_TIMESTAMP as load_tm , 1111 as prc_run_id,source_dt as run_dt from l1_gcb_ref.account ;

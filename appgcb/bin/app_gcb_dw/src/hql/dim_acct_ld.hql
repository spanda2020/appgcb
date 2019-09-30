use app_gcb_dw;
set hive.execution.engine = spark ;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
insert OVERWRITE table dim_account partition(
  prc_run_id,
  run_dt
  )
  select * from app_gcb_stg.dim_account_tgt_stg

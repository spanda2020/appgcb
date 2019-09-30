use app_gcb_stg;
set hive.execution.engine = spark ;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

  with src_stg_data as (select *,'src' as rec_ind,md5(concat_ws("~",country,open_dt)) as hash_key from app_gcb_stg.dim_account_src_stg ),
  src_stg_data_1 as (select src_stg_data.*,row_number() over(partition by acct_nbr,hash_key order by run_dt asc) as rnk from src_stg_data),
  dw_data as (select *,'dw' as rec_ind,md5(concat_ws("~",country,open_dt)) as hash_key, row_number() over(partition by acct_nbr order by run_dt desc) as rnk from app_gcb_dw.dim_account),
  src_dw_data as (select * from src_stg_data_1 as src where src.rnk=1 union all select * from dw_data as dw  where dw.rnk=1),
  src_dw_data_1 as (select *,row_number() over(partition by acct_nbr,hash_key order by run_dt asc) as rnk_all from src_dw_data)
  insert OVERWRITE table dim_account_tgt_stg partition(
    prc_run_id,
    run_dt
    )
  select acct_nbr,country,lvid,open_dt,load_tm,prc_run_id,run_dt from src_dw_data_1 where rnk_all=1 and rec_ind='src' ;

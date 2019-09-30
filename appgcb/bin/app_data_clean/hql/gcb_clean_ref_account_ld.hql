source /data/1/appgcb/tmp/hive_env_var.hql;
source /data/1/appgcb/tmp/app_gcb_dw_param.hql;
source /data/1/appgcb/config/dev_env.hql;
use ${app_data_clean_ref_db};

insert overwrite account trdae partition
(
  source_dt
)
select * from ${app_data_clean_ref_stg_db}.account ;

source /data/1/appgcb/config/hive_config_param.hql;
source /data/1/appgcb/config/hive_var.hql;
--source /data/1/appgcb/tmp/app_data_clean_hive_var_file.hql;
--source ${hive_var_file};


use ${prc_cntrl_db};
select * from app_prc_run_cntrl ;

source /data/1/appgcb/config/hive_config_param.hql;
source /data/1/appgcb/config/hive_var.hql;

use ${prc_cntrl_db};
insert into table app_prc_run_cntrl_stg partition (prc_name)
select
"${prc_run_id}" as prc_run_id,
app_id,
db_name,tbl_name,prc_job_name,prc_job_seq, "${prc_start_dt}" as prc_start_dt,"${prc_run_dt}" as prc_run_dt,"${prc_run_status}" as prc_run_status,
date_format(CURRENT_TIMESTAMP,'yyyy-MM-dd:hh:mm:ss') as prc_exec_time,prc_name
from ${prc_cntrl_db}.app_job_detl where trim(app_id)= "${app_id}" and trim(prc_job_name)="${prc_job_name}" and prc_job_seq=${prc_job_seq};

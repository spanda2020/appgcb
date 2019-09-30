


##### Required input

export PROJECT_BACTH_ID='appgcb'
export env='dev'

export date_format='%Y-%m-%d'
########### Derive #################
export PROJECT_DIR='/data/1/'$PROJECT_BACTH_ID
export PROJECT_APP_CONFIG=$PROJECT_DIR/bin/config


app_list=['app_data_clean','app_gcb_dw']

export LOG_DIR=$PROJECT_DIR/logs
export TMP_DIR=$PROJECT_DIR/tmp

##################### Hive properties ################
export beeline_url=jdbc:hive2://quickstart.cloudera:10000/default
export BATCH_ID_FORMAT="yyyyMMddhhmmss"

export hive_verbose='false'
export hive_header='false'
export hive_op_format='dsv'
export field_Delim='~'
# export hive_master='sroot.net'
# #export hive_master1='root.net'
# #export hive_master2='sroot.net'
# export hive_port='10000'
# export hive_dns='OT.NET'
export hive_execution_engine='spark'
#export hive_execution_engine='mr'
##################### Hive & HDFS Schema and Path ################
export baseHdfsPath="hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/"
export prc_cntrl_db=app_gcb_stg;
export prc_cntrl_stg_tbl=app_prc_run_cntrl_stg;
export prc_cntrl_tbl=app_prc_run_cntrl;

export prc_run_cntl_stg_ld_file=/data/1/appgcb/bin/prccntrl/hql/ct_app_prc_run_cntl_stg_ld.hql
export prc_run_cntl_ld_file=/data/1/appgcb/bin/prccntrl/hql/ct_app_prc_run_cntl_ld.hql
export create_app_job_detl_file=/data/1/appgcb/python/gen_create_job_exec_detl.py


#!/bin/bash

###############################################################################
#SCRIPT NAME  - gen_hive_query_exec.sh
#SCRIPT VER   - 1.0
#DESCRIPTION  - Generic script for reusable functions
#CREATED BY   - Sanjeeb Panda
#CREATED DATE - 2019/07/09
#SCRIPT ARUMENTS -app_id,prc_name,prc_job_name,prc_job_seq,job_code_file_detl
#
#
###############################################################################

SCRIPT_NAME=$(basename -- "$0")
LOG_FL_NM=`echo $SCRIPT_NAME |cut -d "." -f1`


  export PROJECT_BACTH_ID='appgcb'
  export PROJECT_DIR='/data/1/'$PROJECT_BACTH_ID
  export PROJECT_BIN_DIR=${PROJECT_DIR}/bin
  . "${PROJECT_BIN_DIR}/config/app_config_detl.sh"
  . "${PROJECT_BIN_DIR}/utils/app_shell_util.sh"
  . "${PROJECT_BIN_DIR}/utils/app_hive_util.sh"
  #source "${PROJECT_DIR}/config/dev_env.txt"
  LOG_FILE="${LOG_DIR}/${LOG_FL_NM}_${DATE}.log"
  export hive_env_var_hql_file=${PROJECT_BIN_DIR}/config/hive_var.hql



######## Input Param Check
########

if [ "$#" -ne 5 ]; then
    echo "Number of Input Parameters are wrong."
    fnLogMsg INFO "Number of Input Parameters are wrong..." ${LOG_FILE}
    exit 1
fi

fnLogMsg INFO "Input Parameters $1..." ${LOG_FILE}

######## Exporting Iput Parameters
########
export app_id="$1"
export prc_name="$2"
export prc_job_name="$3"
export prc_job_seq=$4
export job_code_file_detl="$5"
# export exe_env="$6"
# sh /data/1/appgcb/wrapper/gen_hive_tbl_ld.sh app_data_clean prc_l1_trxn_ld app_data_clean_trade_trxn_ld_stg 2 /data/1/appgcb/bin/app_data_clean/hql/gcb_clean_trxn_trade_ld.hql
# sh -x "${getJobWrapperDetlVal}" ${app_id} ${prc_name} ${prc_job_name} ${prc_seq} "${getJobCodeFileDetlVal}" ${exe_env} &

######## Defaulting the override File Flag
########
# export override_exec='N'


######## Defaulting the override File Flag
########
export APP_DIR=${PROJECT_BIN_DIR}/${app_id}
# export env_hq_file=${PROJECT_BIN_DIR}/config/${exe_env}_env.hql
# export env_file=${PROJECT_BIN_DIR}/${exe_env}_env_detl.txt
export param_file=${PROJECT_DIR}/tmp/${app_id}_param_file.txt


#echo " param file:  ${param_file}"
# . "${APP_DIR}/config/profile.sh" ${app_id}
# source ${env_file}
source ${param_file}
echo "Parametrs done !!!!!"

####################################


export PRC_STG_STATUS_FILE=${LOG_DIR}/${app_id}_${prc_name}_stg_status_${prc_run_id}.txt
export APP_JOB_DETL_FILE=${PROJECT_DIR}/tmp/app_job_detl_${prc_name}_${prc_run_id}.txt


##############
###################
prc_run_status=STARTED
echo " YEEEEEE ${prc_run_cntl_stg_ld_file}"

message="${app_id}_${prc_name}_${prc_run_status}"
fnLogMsg INFO "${message}" ${LOG_FILE}

echo " YEEEEEE ${prc_run_cntl_stg_ld_file}"
fnPrcStgLoad ${beeline_url} ${prc_run_cntl_stg_ld_file} ${prc_run_status} ${app_id} ${prc_name} ${prc_job_name}  ${prc_job_seq} ${LOG_FILE}




message="${PRC_STG_STATUS_FILE} Append ${prc_run_status} : >>${PRC_STG_STATUS_FILE}"
fnLogMsg INFO "${message}" ${LOG_FILE}
fnPrcStatusFileAppend  ${PRC_STG_STATUS_FILE} ${prc_run_id}  ${prc_job_name} ${prc_job_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}


fnBeelineFileExe ${beeline_url} ${job_code_file_detl}  ${prc_run_id} ${prc_start_dt} ${prc_run_dt} ${LOG_FILE}
# beeline -u jdbc:hive2://quickstart.cloudera:10000/default -f /data/1/appgcb/bin/app_data_clean/hql/gcb_clean_trxn_trade_ld.hql --hivevar prc_run_id=20190803082458 --hivevar prc_start_dt=2019-01-01 --hivevar prc_run_dt=2019-01-01

if [[ $? -eq 0 ]]; then
      prc_run_status=COMPLETED
      echo " YEEEEEE ${prc_run_cntl_stg_ld_file}"

      message="${app_id}_${prc_name}_${prc_run_status}"
      fnLogMsg INFO "${message}" ${LOG_FILE}

      echo " YEEEEEE ${prc_run_cntl_stg_ld_file}"
      fnPrcStgLoad ${beeline_url} ${prc_run_cntl_stg_ld_file} ${prc_run_status} ${app_id} ${prc_name} ${prc_job_name}  ${prc_job_seq} ${LOG_FILE}


      message="${PRC_STG_STATUS_FILE} Append ${prc_run_status} : >>${PRC_STG_STATUS_FILE}"
      fnLogMsg INFO "${message}" ${LOG_FILE}
      fnPrcStatusFileAppend  ${PRC_STG_STATUS_FILE} ${prc_run_id}  ${prc_job_name} ${prc_job_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}

      echo " Loading sucess"
      exit 0
else
      prc_run_status=FAILED
      echo " YEEEEEE ${prc_run_cntl_stg_ld_file}"

      message="${app_id}_${prc_name}_${prc_run_status}"
      fnLogMsg INFO "${message}" ${LOG_FILE}

      echo " YEEEEEE ${prc_run_cntl_stg_ld_file}"
      fnPrcStgLoad ${beeline_url} ${prc_run_cntl_stg_ld_file} ${prc_run_status} ${app_id} ${prc_name} ${prc_job_name}  ${prc_job_seq} ${LOG_FILE}


      message="${PRC_STG_STATUS_FILE} Append ${prc_run_status} : >>${PRC_STG_STATUS_FILE}"
      fnLogMsg INFO "${message}" ${LOG_FILE}
      fnPrcStatusFileAppend  ${PRC_STG_STATUS_FILE} ${prc_run_id}  ${prc_job_name} ${prc_job_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}
      echo " Loading Failed"
      exit 400

fi

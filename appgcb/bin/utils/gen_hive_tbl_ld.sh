#!/bin/bash

###############################################################################
#SCRIPT NAME  - gen_create_hive_object.sh
#SCRIPT VER   - 1.0
#DESCRIPTION  - Generic script for reusable functions
#CREATED BY   - Sanjeeb Panda
#CREATED DATE - 2019/01/09
#SCRIPT ARUMENTS -
#
#
###############################################################################

export TSTMP=`date +%Y%m%d%H%M%S`
CURR_DATE=`date +'%Y%m%d'`
DATE=`date +%Y%m%d%H%M%S`

SCRIPT_NAME=$(basename -- "$0")
LOG_FL_NM=`echo $SCRIPT_NAME |cut -d "." -f1`

export PROJECT_BACTH_ID='appgcb'
export PROJECT_DIR='/data/1/'$PROJECT_BACTH_ID
. "${PROJECT_DIR}/config/app_config_detl.sh"
. "${PROJECT_DIR}/utils/app_shell_util.sh"
. "${PROJECT_DIR}/utils/app_hive_util.sh"
#source "${PROJECT_DIR}/config/dev_env.txt"
LOG_FILE="${LOG_DIR}/${LOG_FL_NM}_${DATE}.log"


######## Input Param Check
########

if [ "$#" -ne 4 ]; then
    echo "Number of Input Parameters are wrong."
    fnLogMsg INFO "Number of Input Parameters are wrong..." ${LOG_FILE}
    exit 1
fi

fnLogMsg INFO "Input Parameters $1..." ${LOG_FILE}

######## Exporting Iput Parameters
########
export APP_ID="$1"
export prc_job_name="$2"
export prc_name="$3"
export exe_env="$4"

######## Defaulting the override File Flag
########
export override_exec='N'


######## Defaulting the override File Flag
########
export APP_DIR="${PROJECT_DIR}/bin/${APP_ID}"
export env_hq_file=${PROJECT_DIR}/config/${exe_env}_env.hql
export env_file=${PROJECT_DIR}/config/${exe_env}_env_detl.txt
export param_file=${PROJECT_DIR}/tmp/${APP_ID}_param_file.hql

. "${APP_DIR}/config/profile.sh" ${APP_ID}
source ${env_file}


######## Global Variables
########


. "${APP_DIR}/config/profile.sh" ${APP_ID}
source ${env_file}
prc_run_status=STARTED
#prc_run_cntl_stg_ld_file=/data/1/appgcb/bin/app_data_clean/hql/gcb_clean_trxn_trade_ld.hql


message="${APP_ID}_${prc_job_name}->${param_file}"
fnLogMsg INFO "${message}" ${LOG_FILE}
echo "Log File: ${LOG_FILE}"
fnPrcStgLoad ${beeline_url} ${prc_run_cntl_stg_ld_file} ${param_file} ${prc_run_status} ${APP_ID} ${prc_name} ${prc_job_name}  ${env_hq_file} ${LOG_FILE}
fnBeelineFileExe ${beeline_url} ${prc_run_cntl_stg_ld_file} ${LOG_FILE}

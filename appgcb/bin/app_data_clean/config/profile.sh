#!/bin/bash

###############################################################################
#SCRIPT NAME  - gcs_shell-utils.sh
#SCRIPT VER   - 1.0
#DESCRIPTION  - Generic script for reusable functions
#CREATED BY   - Sanjeeb Panda
#CREATED DATE - 2019/01/09
#SCRIPT ARUMENTS -
#
#
###############################################################################
# Global variables
TSTMP=`date +%Y%m%d%H%M%S`
CURR_DATE=`date +'%Y%m%d'`
DATE=`date +%Y%m%d%H%M%S`


export PROJECT_BACTH_ID='appgcb'
export PROJECT_DIR='/data/1/'$PROJECT_BACTH_ID
. "${PROJECT_DIR}/config/app_config_detl.sh"
. "${PROJECT_DIR}/utils/app_shell_util.sh"
#source "${PROJECT_DIR}/config/dev_env.txt"


export APP_ID="$1"
export APP_DIR="${PROJECT_DIR}/bin/${APP_ID}"
export date_format="%Y-%m-%d"


# fnOverrideFlGen() {
# 	app_id="$1"
#   file_nm=${app_id}_override.txt
# 	echo "$file_nm"
# 	#fnLogMsg() INFO "${app_id} " ${LOG_FILE}
# }



export APP_OVERRIDE_FILE=$(fnOverrideFlGen $APP_ID)
export APP_OVERRIDE_FILE_PATH="${TMP_DIR}/${APP_OVERRIDE_FILE}"

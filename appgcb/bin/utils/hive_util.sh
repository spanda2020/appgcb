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

################### Global variables ###################
TSTMP=`date +%Y%m%d%H%M%S`
CURR_DATE=`date +'%Y%m%d'`
DATE=`date +%Y%m%d%H%M%S`
# export USER_ID="appgcb" #`whoami`
# export BASE_DIR="/data/1"
# #BASE_DIR="/home/${USER_ID}/data/1"
# export RUN_DIR="${BASE_DIR}/${USER_ID}/bin/app_data_clean"

# Source files
# . "${RUN_DIR}/config/profile.sh"

########################### fnBeelineFileExe ###################################
#Function to log messages in a log file
fnBeelineFileExe() {
beeline_url="$1"
file_name="$2"
beeline -u ${beeline_url} -f "${file_name}" >> "${LOG_FILE}"
}


########################### fnAddPartitionHiveExt ###################################
fnAddPartitionHiveExt(){
	schema_tbl=$1 #l1_gcb_trxn.trade
	source_dt=$2 #20190101
	partion_path=$3 #gs://app_gcb_data/appmkt/l1_appmkt_mdx/trade/20190101/ # "$2"
	adddPartitionsql="alter table ${schema_tbl} add partition (source_dt='${source_dt}') location '${partion_path}' ; "
	echo "adddPartitionsql:${adddPartitionsql}" >> "${LOG_FILE}"
	fnLogMsg INFO "${sql}" ${LOG_FILE}
	beeline -u ${beeline_url} -e "${adddPartitionsql}"
  #fnLogMsg INFO "${beelinQuery}" ${LOG_FILE}
	#`${beelinQuery}`
	beeline -u ${beeline_url}  -e "select count(1) as rec_cnt from ${schema_tbl} where source_dt='${source_dt}' ;" >> "${LOG_FILE}"

	}
########################### fnDropPartitionHiveExt ###################################
fnDropPartitionHiveExt(){

	schema_tbl=$1 #l1_gcb_trxn.trade
	source_dt=$2 #20190101
	dropsql="alter table  ${schema_tbl} drop partition(source_dt='${source_dt}') ;"
	echo "dropsql:${dropsql}" >> "${LOG_FILE}"
	beeline -u ${beeline_url} -e "${dropsql}" >> "${LOG_FILE}"
	beeline -u ${beeline_url} -e "select count(1) as rec_cnt  from ${schema_tbl} where source_dt='${source_dt}' ;" >> "${LOG_FILE}"
}

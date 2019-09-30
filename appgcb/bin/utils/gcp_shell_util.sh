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


########################### fnCheckGcsFile ###################################

fnCheckGcsFile() {
	src_folder="$1"
	src_file="$2"
	gsutil ls ${src_folder}/${src_file}
	#gsutil ls gs://app_gcb_inbound/appmkt/raw/20190101_tran_trade_acx.csv
	if [ $? -eq 0 ]; then
		echo "File Exists"

		return 0
	else

		return  1
	fi
}

########################### fnCopyGcsFile ###################################
fnCopyGcsFile() {

	src_folder="$1"
	src_file="$2"
	tgt_folder="$3"
  #tgt_file="$4"

	fl_copy=${src_folder}/${src_file}

	TS=`date +%Y%m%d%H%M%S`
	echo "${TS}: ${src_folder}/${src_file} to move ${tgt_folder}" >> "${LOG_FILE}"
	gsutil -m cp ${fl_copy} ${tgt_folder}
	#echo "${TS} ${DEBUG_LEVEL}: ${MSG}"
}

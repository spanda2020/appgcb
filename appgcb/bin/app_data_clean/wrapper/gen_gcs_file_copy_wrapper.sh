
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

TSTMP=`date +%Y%m%d%H%M%S`
CURR_DATE=`date +'%Y%m%d'`
DATE='date +%Y/%m/%d:%H:%M:%S'
export BASE_DIR="/data/1"
export USER_ID="appgcb" #`whoami`
export RUN_DIR="${BASE_DIR}/${USER_ID}/bin/app_data_clean"
. "${RUN_DIR}/config/profile.sh"
. "${RUN_DIR}/util/gcs_shell_util.sh"


Executable="appgcb_gcs_copy_wrapper" #"${MKT_PROCESS_CYCLE_SCRIPT}"
LOG_FILE="${LOG_DIR}/${Executable}_${TSTMP}.logs"

if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    fnLogMsg INFO "Number of Input Parameters are wrong..." ${LOG_FILE}
    exit 1
else

export SRC_TYPE=$1
export SRC_NM=$2
export TGT_DB=$3
export ODATE=$4

fi

eval "SRC_TBL=\${l1_${SRC_TYPE}_tbl}"
# export l1_gcb_trxn_db_path="gs://app_gcb_data/appmkt/l1_appmkt_mdx"
# export ODATE=20190101
# export SRC_TYPE=trade
# export TGT_DB="l1_gcb_trxn"
eval "TGT_TBL_PARTITION=\${${TGT_DB}_db_path}/${SRC_TYPE}/${ODATE}/"
echo ${TGT_TBL_PARTITION}


fnGcsHiveLoadProcess(){

  fnLogMsg INFO "Calling fnCheckGcsFile()..." ${LOG_FILE}
  export SRC_FILE=${ODATE}_${SRC_TYPE}_${SRC_NM}.csv
  fnCheckGcsFile ${INBOUND_PATH} ${SRC_FILE}

  if [ $? -eq 0 ]; then
    fnLogMsg INFO " ${INBOUND_PATH}/${SRC_FILE} File Exists..." ${LOG_FILE}
    fnCopyGcsFile ${INBOUND_PATH} ${SRC_FILE} ${TGT_TBL_PARTITION} #gs://app_gcb_data/appmkt/l1_appmkt_mdx/trade/20190101/

  else
  fnLogMsg ERROR " ${INBOUND_PATH}/${SRC_FILE} File Doesn't Exists..." ${LOG_FILE}
      exit 1
  fi
  # fnCopyGcsFile gs://app_gcb_inbound/appmkt/raw 20190101_tran_trade_acx.csv gs://app_gcb_data/appmkt/l1_appmkt_mdx/trade/20190101/

  fnLogMsg INFO "Dropping existing partitions if present fnDropPartitionHiveExt()..." ${LOG_FILE}
  fnDropPartitionHiveExt ${TGT_DB}.${SRC_TBL} ${ODATE}
  fnLogMsg INFO "Adding  partitions if present fnAddPartitionHiveExt()..." ${LOG_FILE}

  fnAddPartitionHiveExt ${TGT_DB}.${SRC_TBL} ${ODATE} ${TGT_TBL_PARTITION}

}


#### Calling fnGcsHiveLoadProcess ###########################
fnGcsHiveLoadProcess

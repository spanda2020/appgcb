
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

TSTMP=`date +%Y%m%d%H%M%S`
CURR_DATE=`date +'%Y%m%d'`
DATE=`date +%Y%m%d%H%M%S`


export PROJECT_BACTH_ID='appgcb'
export PROJECT_DIR='/data/1/'$PROJECT_BACTH_ID
. "${PROJECT_DIR}/bin/config/app_config_detl.sh"
. "${PROJECT_DIR}/bin/utils/app_shell_util.sh"
#source "${PROJECT_DIR}/config/dev_env.txt"


SCRIPT_NAME=$(basename -- "$0")
LOG_FILE="${LOG_DIR}/${SCRIPT_NAME}_${DATE}.log"

fnLogMsg INFO "${CURR_DATE} " ${LOG_FILE}


if [ "$#" -ne 2 ]; then
    echo "Number of Input Parameters are wrong."
    fnLogMsg INFO "Number of Input Parameters are wrong..." ${LOG_FILE}
    exit 1
else

   exe_env=$1
   execute_fl=$2
   env_file=${PROJECT_DIR}/config/${exe_env}_env.hql
   export tmp_execute_file=${PROJECT_DIR}/tmp/${execute_fl}

   fnLogMsg INFO "${tmp_execute_file} Process Begins" ${LOG_FILE}
   #fnBeelineFileExe() $beeline_url $
   #beeline -u $beeline_url -f "${PROJECT_DIR}/tmp/create_db_l1_gcb_trxn.hql" --hivevar db_env=dev

   status_file=${PROJECT_DIR}/tmp/$DATE'_status_'${execute_fl}
   touch $status_file

   filelines=`cat $tmp_execute_file`
   echo Start

   for line in $filelines ; do
       echo $line
       hive -f "${line}" --hivevar hql_env_file=${env_file}
       if [ $? -eq 0 ]; then
          fnLogMsg INFO "${line} Executed" ${LOG_FILE}
          echo $line "Sucess" >> $status_file
      else
         echo $line "Failed" >> $status_file
         fnLogMsg ERROR "$line Execution Failed " ${LOG_FILE}
      fi
   done

  grep "Failed" $status_file
      if [ $? -ne 0 ]; then
        fnLogMsg INFO "$tmp_execute_file Execution Completed " ${LOG_FILE}
      else

        fnLogMsg ERROR "$tmp_execute_file Execution Failed " ${LOG_FILE}
        fnLogMsg INFO "Look For $status_file " ${LOG_FILE}
        exit 1
      fi

fi

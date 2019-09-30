
  #!/bin/bash

  ###############################################################################
  #SCRIPT NAME  - gen_job_exec.sh
  #SCRIPT VER   - 1.0
  #DESCRIPTION  - Generic script for reusable functions
  #CREATED BY   - Sanjeeb Panda
  #CREATED DATE - 2019/07/09
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
  export PROJECT_BIN_DIR=${PROJECT_DIR}/bin
  . "${PROJECT_BIN_DIR}/config/app_config_detl.sh"
  . "${PROJECT_BIN_DIR}/utils/app_shell_util.sh"
  . "${PROJECT_BIN_DIR}/utils/app_hive_util.sh"
  #source "${PROJECT_DIR}/config/dev_env.txt"
  LOG_FILE="${LOG_DIR}/${LOG_FL_NM}_${DATE}.log"
  export hive_env_var_hql_file=${PROJECT_BIN_DIR}/config/hive_var.hql



  ######## Input Param Check
  ########

  if [ "$#" -ne 3 ]; then
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

  ### Can put a list check for the input param for batch freq  : Values should be  [D: Daily,W: weekly,BW: 15days,M: Monthly]
  # export batch_freq="$4"
  # export exe_env="$5"

  ######## Defaulting the override File Flag
  ########
  export override_exec='N'
  export prc_job_seq=1


  #echo " param file:  ${param_file}"
  # . "${APP_DIR}/config/profile.sh" ${app_id}
  # source ${env_file}
  export param_file=${PROJECT_DIR}/tmp/${app_id}_param_file.txt
  export hive_var_file=${PROJECT_DIR}/tmp/${app_id}_hive_var_file.hql
  echo "Parametrs done !!!!!"
  source ${param_file}
  echo "Parametrs done !!!!!"

####################################


export PRC_STG_STATUS_FILE=${LOG_DIR}/${app_id}_${prc_name}_stg_status_${prc_run_id}.txt
export APP_JOB_DETL_FILE=${PROJECT_DIR}/tmp/app_job_detl_${prc_name}_${prc_run_id}.txt
export tmp_job_file=${PROJECT_DIR}/logs/tmpjob_${prc_job_name}_${prc_run_id}.txt

fnLogMsg INFO "Creating Tmp Jb File as  ${tmp_job_file}..." ${LOG_FILE}
getJobDetl "${APP_JOB_DETL_FILE}" ${prc_job_name} "${tmp_job_file}"



filelines=`cat "${tmp_job_file}"`
echo Start
for line in $filelines ; do
    echo $line
    prc_job_seq=`echo  ${line} | cut -d"|" -f1`
    getJobWrapperDetlVal=`echo  ${line} |  cut -d"|" -f2`
    getJobCodeFileDetlVal=`echo  ${line} | cut -d"|" -f3`
    echo "$prc_job_seq"
    echo "$getJobWrapperDetlVal"
    echo "$getJobCodeFileDetlVal"
    getPrcJobStatusVal=$(getPrcJobStatus ${PRC_STG_STATUS_FILE} ${prc_job_name} ${prc_job_seq})
    getJobRerunFlag=$(getJobRerunFlag ${APP_JOB_DETL_FILE} ${prc_job_name} ${prc_job_seq} )

    fnLogMsg INFO "Staus of ${prc_job_name}  ${getJobRerunFlag}..." ${LOG_FILE}


    if   [ $getPrcJobStatusVal == 'COMPLETED' ] && [ $getJobRerunFlag == 'N' ]; then
        echo " The ${prc_job_name} has allready completed and No restart mentioned"
        fnLogMsg INFO "The ${prc_job_name} has allready completed and No restart mentioned..." ${LOG_FILE}

    else
        echo "sh -x "${getJobWrapperDetlVal}" ${app_id} ${prc_name} ${prc_job_name} ${prc_job_seq} "${getJobCodeFileDetlVal}"  &"
        fnLogMsg INFO "sh  ${getJobWrapperDetlVal}   ${app_id} ${prc_name} ${prc_job_name} ${prc_job_seq} ${getJobCodeFileDetlVal} ${exe_env} & " ${LOG_FILE}
        sh  ${getJobWrapperDetlVal} ${app_id} ${prc_name} ${prc_job_name} ${prc_job_seq} ${getJobCodeFileDetlVal}  &
        # sh /data/1/appgcb/wrapper/gen_hive_tbl_ld.sh app_data_clean prc_l1_trxn_ld app_data_clean_trade_trxn_ld_stg 2 /data/1/appgcb/bin/app_data_clean/hql/gcb_clean_trxn_trade_ld.hql
        PID="$!"
        echo "$PID:$line" >> ${tmp_job_file}
        PID_LIST+=("$PID")
    fi

    sleep 10

done
echo " PID_LIST ::: $PID_LIST"
for process in ${PID_LIST[@]};do
   wait $process
   exit_status=$?
   if [[ exit_status -eq 0 ]]; then
     script_detl=`egrep $process $tmp_job_file | cut -d":" -f 2`
     echo "$script_detl exit status: $exit_status"
     echo " Exist Staus for the $process : $exit_status"
     fnLogMsg INFO "Exist Staus for the $process : $exit_status." ${LOG_FILE}
     echo " Load is Completed Thanks!!!"
     fnLogMsg ERROR " Load is Completed Thanks!!! " ${LOG_FILE}
  else
    script_detl=`egrep $process $tmp_job_file | cut -d":" -f 2`
    echo "$script_detl exit status: $exit_status"
    echo " Exist Staus for the $process : $exit_status"
    fnLogMsg ERROR "Exist Staus for the $process : $exit_status." ${LOG_FILE}
    echo " Load is Not Completed Thanks!!!"
    fnLogMsg ERROR " Load is Not Completed Please Check!!!" ${LOG_FILE}
  fi
done

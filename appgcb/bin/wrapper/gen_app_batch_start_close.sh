
  #!/bin/bash

  ###############################################################################
  #SCRIPT NAME  - app_batch_start_close.sh
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

  if [ "$#" -ne 4 ]; then
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
  export batch_freq="$4"
  # export exe_env="$5"

  ######## Defaulting the override File Flag
  ########
  export override_exec='N'
  export prc_job_seq=1


  ######## Defaulting the override File Flag
  ########
  # export APP_DIR=${PROJECT_DIR}/bin/${app_id}
  export app_override_fl=${PROJECT_DIR}/tmp/${app_id}_override.txt


  # export env_file=${PROJECT_DIR}/config/${exe_env}_env_detl.txt


  #echo " param file:  ${param_file}"
  # . "${APP_DIR}/config/profile.sh" ${app_id}
  # source ${env_file}
  export param_file=${PROJECT_DIR}/tmp/${app_id}_param_file.txt
  export hive_var_file=${PROJECT_DIR}/tmp/${app_id}_hive_var_file.hql
  echo "Parametrs done !!!!!"

  ######## Global Variables
  ########

  if    [[ $prc_job_name == *"prc_start"* ]]; then
          source ${param_file}
          export prc_run_status=PRC_STARTED
          export PRC_STG_STATUS_FILE="${LOG_DIR}/${app_id}_${prc_name}_stg_status_${prc_run_id}.txt"
          export prc_override_fl=${PROJECT_DIR}/tmp/${prc_name}_override.txt
          export app_job_detl_fl=${PROJECT_DIR}/tmp/app_job_detl_${prc_name}_${prc_run_id}.txt
  elif [[ $prc_job_name == *"prc_complete"* ]]; then

         source ${param_file}
         export prc_run_status=PRC_COMPLETED
         export PRC_STG_STATUS_FILE="${LOG_DIR}/${app_id}_${prc_name}_stg_status_${prc_run_id}.txt"
         # export param_file=${PROJECT_DIR}/tmp/${prc_name}_param_file.txt
         export APP_JOB_DETL_FILE=${PROJECT_DIR}/tmp/app_job_detl_${prc_name}_${prc_run_id}.txt
  elif [[ $prc_job_name == *"app_start"* ]]; then

        export prc_run_status=APP_STARTED

  elif [[ $prc_job_name == *"app_complete"* ]]; then
        export prc_run_status=APP_COMPLETED
        source ${param_file}
        export app_job_detl_fl=${PROJECT_DIR}/tmp/app_job_detl_${prc_name}_${prc_run_id}.txt

  else
        source ${param_file}
        export prc_run_status=PRC_CNTRL_LOAD
        export PRC_STG_STATUS_FILE="${LOG_DIR}/${app_id}_${prc_name}_stg_status_${prc_run_id}.txt"
        # export param_file=${PROJECT_DIR}/tmp/${prc_name}_param_file_${prc_run_id}.txt
        export APP_JOB_DETL_FILE=${PROJECT_DIR}/tmp/app_job_detl_${prc_name}_${prc_run_id}.txt

  fi


  ######### PRC Control Load ####

  if [[ $prc_run_status == 'PRC_CNTRL_LOAD' ]] ; then



    message="${app_id}_${prc_name}_PRC_CNTRL_LOAD"
    fnLogMsg INFO "${message}" ${LOG_FILE}
    fnPrcStgLoad ${beeline_url} ${prc_run_cntl_ld_file} ${prc_run_status} ${app_id} ${prc_name} ${prc_job_name}  ${prc_job_seq} ${LOG_FILE}

    message="${PRC_STG_STATUS_FILE} Append PRC_CNTRL_LOAD_COMPLETE STATUS->${param_file}"
    fnLogMsg INFO "${message}" ${LOG_FILE}
    fnPrcStatusFileAppend  ${PRC_STG_STATUS_FILE} ${prc_run_id}  ${prc_job_name} ${prc_job_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}
    # hdfs dfs -rm  hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg/app_prc_run_cntrl_stg/prc_name=prc_l1_trxn_ld/*


    exit 0

  fi

  ######### PRC Control Load ####

  if [[ $prc_run_status == 'PRC_COMPLETED' ]] ; then

        getJobRerunFlag=$(getJobRerunFlag ${PRC_STG_STATUS_FILE} ${prc_job_name} )

        message="${PRC_STG_STATUS_FILE} Check STATUS"
        fnLogMsg INFO "${message}" ${LOG_FILE}
        getPrcJobStatusVal=$(getPrcJobStatus ${PRC_STG_STATUS_FILE} ${prc_job_name} )
        echo "getPrcJobStatusVal : $getPrcJobStatusVal"


      if [[ $getPrcJobStatusVal == 'COMPLETED' ]]; then
          echo " The ${prc_job_name} has allready completed and No restart mentioned"
          exit 0
      else
          message="${app_id}_${prc_name}"
          fnLogMsg INFO "${message}" ${LOG_FILE}
          fnPrcStgLoad ${beeline_url} ${prc_run_cntl_stg_ld_file} ${prc_run_status} ${app_id} ${prc_name} ${prc_job_name}  ${prc_job_seq} ${LOG_FILE}

          message="${PRC_STG_STATUS_FILE} Append COMPLETED STATUS->${param_file}"
          fnLogMsg INFO "${message}" ${LOG_FILE}
          echo " fnPrcStatusFileAppend  ${PRC_STG_STATUS_FILE} ${prc_run_id}  ${prc_job_name} ${prc_job_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}"
          fnPrcStatusFileAppend  ${PRC_STG_STATUS_FILE} ${prc_run_id}  ${prc_job_name} ${prc_job_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}

            if [ $? -eq 0 ]; then
              echo " Completed. Thanks !!!"
              exit 0
            fi
        fi

  fi
############################################ APP START ########################

######### Adhoc Batch using override File ####

 if [ $prc_run_status == 'APP_STARTED' ] ; then

      # message="${app_id}_${prc_name} Partitions Drop form ${prc_cntrl_db} ${prc_cntrl_stg_tbl} "
      # fnLogMsg INFO "${message}" ${LOG_FILE}

      # fnHdfsPartitionDelete "${baseHdfsPath}" ${prc_cntrl_db} ${prc_cntrl_stg_tbl} prc_name ${prc_name} ${LOG_FILE}

########### Check the app override File or PRC override File exist or not
        message="${app_id}_Batch_Start->${app_override_fl} Exist Check"
        echo "$message"
        $(fnFileExist "${app_override_fl}" "${message}" "${LOG_FILE}")
        fnFileExist_app_override_fl_val=$?
        message="${app_id}_Batch_Start->${prc_override_fl} Exist Check"
        echo "fnFileExist_app_override_fl_val: $fnFileExist_app_override_fl_val"
        overrideFl=${app_override_fl}


########### If any override File Exists then adhoc batch else Normal batch
      echo "  fnFileExist_app_override_fl_val: $fnFileExist_app_override_fl_val"
      # echo "  fnFileExist_prc_override_fl_val: $fnFileExist_prc_override_fl_val"

        if [ $fnFileExist_app_override_fl_val -eq 0  ]  ; then

########### As the override File Exist then check each parameter has valid values

            fnGetStartRunDtval=$(fnGetStartRunDt "${overrideFl}" "${message}" "${LOG_FILE}")
            export prc_start_dt=`echo $fnGetStartRunDtval | cut -d"|" -f1| cut -d"=" -f2`
            export prc_run_dt=`echo $fnGetStartRunDtval | cut -d"|" -f2| cut -d"=" -f2`
            message="${app_id}_batch_start->prc_run_dt Check in ${overrideFl}"
            echo "$message"
            fnVarNullChk "${prc_run_dt}" "${message}" "${LOG_FILE}"
            message="${app_id}_batch_start->prc_start_dt Check in ${overrideFl}"
            echo "$message"
            fnVarNullChk "${prc_start_dt}" "${message}" "${LOG_FILE}"
            message="${app_id}_batch_start->prc_run_dt > prc_start_dt Comparision in ${overrideFl}"
            echo "$message"
            fnDateCompare "${prc_run_dt}" "${prc_start_dt}" "${date_format}" "${message}" "${LOG_FILE}"
            export prc_run_dt=`date -d ${prc_run_dt} +${date_format}`
            export prc_start_dt=`date -d ${prc_start_dt} +${date_format}`

            message="${app_id}_param_file create->${param_file}"
            fnLogMsg INFO "${message}" ${LOG_FILE}
            echo "$message"

########### Parameter File Creates inluding run date starte date and batch id

            fnParamFileCreate  ${param_file} ${TSTMP} ${prc_start_dt} ${prc_run_dt}  ${LOG_FILE}

            exit 0
      else
        #prc_run_cntl_stg_ld_file=/data/1/appgcb/bin/app_gcb_dw/prccntrl/hql/ct_app_prc_run_cntl_stg_ld.hql
        echo " We are in 2nd phase "
        fnGetDatesPrcCntrlVal=$(fnGetDatesPrcCntrl ${beeline_url} ${app_id} ${prc_name} ${batch_freq} ${LOG_FILE})

        if [[ "$fnGetDatesPrcCntrlVal" == 'NOTFOUND' ]]; then
            message="There is no prior sucess batch in prc cntrl table for ${app_id} ${prc_name} "
            echo "$message"
            fnLogMsg INFO "${message}" ${LOG_FILE}
            exit 1
        fi

        echo "fnGetDatesPrcCntrlVal : $fnGetDatesPrcCntrlVal"
        prc_start_dt=`echo $fnGetDatesPrcCntrlVal | cut -d"~" -f2`
        prc_run_dt=`echo $fnGetDatesPrcCntrlVal | cut -d"~" -f3`

########### Parameter File Creates inluding run date starte date and batch id

        message="${app_id}_param_file create->${param_file}"
        fnLogMsg INFO "${message}" ${LOG_FILE}

        fnParamFileCreate  ${param_file} ${TSTMP} ${prc_start_dt} ${prc_run_dt}  ${LOG_FILE}
        # source ${param_file}
        exit 0

    fi

  fi


######### PRC_STARTED ####

  if [ $prc_run_status == 'PRC_STARTED' ] ; then

        message="${app_id}_${prc_name} Partitions Drop form ${prc_cntrl_db} ${prc_cntrl_stg_tbl} "
        fnLogMsg INFO "${message}" ${LOG_FILE}

        fnHdfsPartitionDelete "${baseHdfsPath}" ${prc_cntrl_db} ${prc_cntrl_stg_tbl} prc_name ${prc_name} ${LOG_FILE}

  ########### Hive Env Var File create

            # fnHiveVarFileCreate  ${param_file} ${hive_var_file} ${hive_env_var_hql_file}  ${LOG_FILE}

  ########### Check for the configuration tables are in sycn and then generate the app_job_detl_file for execution

              message="For ${app_id} Create create_APP_JOB_DETL_FILE ->${create_APP_JOB_DETL_FILE}"
              fnLogMsg INFO "${message}" ${LOG_FILE}
              spark-submit ${create_app_job_detl_file} ${app_id} ${prc_name} ${prc_run_id}
              if [[ $? -eq 0 ]]; then
                fnLogMsg INFO "${message} : Sucess" ${LOG_FILE}
              else
                fnLogMsg ERROR "${message} : Failed" ${LOG_FILE}
                exit 1
              fi
  ########### Good to start the batch .So update the process run cntrl stg Table

              message="${app_id}_batch_start_prc_stg_load"
              fnLogMsg INFO "${message}" ${LOG_FILE}
              echo "Log File: ${LOG_FILE}"
              fnPrcStgLoad ${beeline_url} ${prc_run_cntl_stg_ld_file} ${prc_run_status} ${app_id} ${prc_name} ${prc_job_name}  ${prc_job_seq} ${LOG_FILE}

  ########### Keeping the same status in File also

              message="${PRC_STG_STATUS_FILE} Create Process Start STATUS->${param_file}"
              fnLogMsg INFO "${message}" ${LOG_FILE}
              fnPrcStatusFileCreate  ${PRC_STG_STATUS_FILE} ${prc_run_id} ${prc_job_name} ${prc_job_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}

              export override_exec='Y'

              echo " Completed. Thanks !!!"
              exit 0

          fi


######### APP_COMPLETED ####
  if [ $prc_run_status == 'APP_COMPLETED' ] ; then
      fnFileMove ${app_job_detl_fl} ${prc_run_id} ${LOG_DIR}  ${LOG_FILE}
      fnFileMove ${param_file} ${prc_run_id}  ${LOG_DIR}  ${LOG_FILE}
      fnFileMove ${app_override_fl} ${prc_run_id}  ${LOG_DIR}  ${LOG_FILE}
      exit 0
  fi



echo " $SCRIPT_NAME Completed. Thanks !!!"

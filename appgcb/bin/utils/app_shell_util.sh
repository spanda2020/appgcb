#!/bin/bash

###############################################################################
#SCRIPT NAME  - app_shell_utils.sh
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
export LOG_FL=${LOG_FL}

################### fnLogMsg ###################

#Function to log messages in a log file
fnLogMsg() {
	DEBUG_LEVEL="$1"
	MSG="$2"
	LOG_FILE="$3"
	TS=`date +%Y%m%d%H%M%S`
	echo "${TS} ${DEBUG_LEVEL}: ${MSG}" >> "${LOG_FILE}"
}

################### fnOverrideFlGen ###################
fnOverrideFlGen() {
	app_id="$1"
  file_nm=${app_id}_override.txt
	echo $file_nm
	#fnLogMsg() INFO "${app_id} " ${LOG_FILE}
}

################### fnFileExist ###################
fnFileExist() {
	fl_detl="$1"
	msg="$2"
	log_fl="$3"
	if [ -f ${fl_detl} ] ; then
		msg="${msg}:Passed"
		fnLogMsg INFO "${msg}" ${log_fl}
		return 0
	else
    return 1
	fi
}
################### fnFileDelete ###################
fnFileMove() {
	src_file="$1"
	prc_run_id=$2
	tgt_path="$3"
	log_fl="$4"

	echo ${src_file} |awk -F/ '{print $NF}' | awk -F_ '{print $NF}' |awk -F. '{print $1}' |grep -q '[0-9]'
	if [ $? -ne 0 ]; then
				src_file_nm=`echo $src_file| awk -F/ '{print $NF}'|awk -F. '{print $1}'`
				src_file_ext=`echo $src_file| awk -F/ '{print $NF}'|awk -F. '{print $NF}'`
			  tgt_file_nm=${src_file_nm}_${prc_run_id}.${src_file_ext}
				echo "tgt_file_nm :$tgt_file_nm"
	else
			src_file_nm=`echo $src_file| awk -F/ '{print $NF}'`
			tgt_file_nm=$src_file_nm
	fi

	if [ -f ${src_file} ] ; then
		mv  $src_file "$tgt_path/$tgt_file_nm"
			if [ $? -eq 0 ]; then
					msg="${msg}:$src_file Moved "
					fnLogMsg INFO "${msg}" ${log_fl}
					return 0
			else
			    return 1
			fi
	fi
}
################### fnFileExist ###################
fnGetStartRunDt() {
	fl_detl="$1"
  msg="$2"
	log_fl="$3"
	override_date_array=()
	val1=`grep prc_start_dt "$fl_detl"`
	override_date_array+="$val1|"
	val2=`grep prc_run_dt "$fl_detl"`
	override_date_array+=$val2
echo $override_date_array

}

################### fnVarNullChk ###################
fnVarNullChk() {
	var="$1"
	msg="$2"
	log_fl="$3"
	if [[ -z "${var}" ]] ; then

			 msg="${msg}:Failed"
			 fnLogMsg ERROR "${msg}" ${log_fl}
			 exit 400
	else
		msg="${msg}:Passed"
		fnLogMsg INFO "${msg}" ${log_fl}
		return 0
	fi
}
################### fnOverrideFlGen ###################
fnDateCompare(){
	msg="$4"
	log_fl="$5"
	date_format="$3"
	run_dt=$(date -d $1 +${date_format})
	start_dt=$(date -d $2 +${date_format})
	if [[ ${run_dt} -gt ${start_dt} ]] ; then
		msg="${msg}:Failed"
		fnLogMsg ERROR "${msg}" ${log_fl}
		exit 400
	else
		msg="${msg}:Passed"
		fnLogMsg INFO "${msg}" ${log_fl}
		return 0
	fi
}
################### fnParamFileCreate ###################

fnParamFileCreate(){
	fl_nm="$1"
	prc_run_id="$2"
	prc_start_dt="$3"
	prc_run_dt="$4"
	log_fl="$5"
	if [[ -f ${fl_nm} ]]; then
			rm ${fl_nm} 2>/dev/null
				if [[ $? -ne 0 ]] ; then
					msg="${fl_nm} removal Failed"
					fnLogMsg ERROR "${msg}" ${log_fl}
					exit 400
				fi
  fi
			msg="${fl_nm} Removal :Passed"
			fnLogMsg INFO "${msg}" ${log_fl}
			  echo "prc_run_id=${TSTMP};" >>${fl_nm}
			  echo "prc_start_dt="${prc_start_dt}";" >>${fl_nm}
			  echo "prc_run_dt="${prc_run_dt}";" >>${fl_nm}
			msg="${fl_nm} Got created "
			fnLogMsg INFO "${msg}" ${log_fl}
}

################### fnHiveVarFileCreate ###################

fnHiveVarFileCreate(){
	param_file="$1"
	hive_var_file="$2"
	env_hq_file="$3"
	log_fl="$4"


	if [[ -f ${hive_var_file} ]]; then
			rm ${hive_var_file} 2>/dev/null
				if [[ $? -ne 0 ]] ; then
					msg="${hive_var_file} removal Failed"
					fnLogMsg ERROR "${msg}" ${log_fl}
					exit 400
				fi
  fi
			msg="${fl_nm} Removal :Passed"
			fnLogMsg INFO "${msg}" ${log_fl}
			  echo "set hivevar:prc_run_id=${TSTMP};" >>${hive_var_file}
			  echo "set hivevar:prc_start_dt="${prc_start_dt}";" >>${hive_var_file}
			  echo "set hivevar:prc_run_dt="${prc_run_dt}";" >>${hive_var_file}
				cat ${env_hq_file} >>${hive_var_file}
			msg="${hive_var_file} Got created "
			fnLogMsg INFO "${msg}" ${log_fl}
}

##################################################################
fnPrcStatusFileCreate(){
	fl_nm="$1"
	prc_run_id="$2"
	prc_job_name="$3"
	prc_job_seq=$4
	prc_start_dt="$5"
	prc_run_dt="$6"
	prc_run_status="$7"
	LOG_FL="$8"
	if [[ -f ${fl_nm} ]]; then
			rm ${fl_nm} 2>/dev/null
				if [[ $? -ne 0 ]] ; then
					msg="${fl_nm} removal Failed"
					fnLogMsg ERROR "${msg}" "${LOG_FL}"
					exit 400
				fi
  fi

	msg="${fl_nm} Removal :Passed"
	fnLogMsg INFO "${msg}" "${LOG_FL}"
	echo "prc_run_id=${prc_run_id}|prc_job_name="${prc_job_name}"|prc_job_seq=${prc_job_seq}|prc_start_dt="${prc_start_dt}"|prc_run_dt="${prc_run_dt}"|prc_run_status="${prc_run_status}"" >>${fl_nm}
	msg="${fl_nm} Got created "
	fnLogMsg INFO "${msg}" ${LOG_FL}
	if [[ -f ${fl_nm} ]]; then
			return 0
	else
		return 1
  fi
}

######################### fnPrcStatusFileAppend #########################################
fnPrcStatusFileAppend(){
	fl_nm="$1"
	prc_run_id="$2"
	prc_job_name="$3"
	prc_job_seq=$4
	prc_start_dt="$5"
	prc_run_dt="$6"
	prc_run_status="$7"
	LOG_FL="$8"
	# fnPrcStatusFileAppend  ${PRC_STG_STATUS_FILE} ${prc_run_id}  ${prc_job_name} ${prc_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}
	if [[ -f ${fl_nm} ]]; then
		msg="${fl_nm} fl_nm :Exist"
		fnLogMsg INFO "${msg}" ${LOG_FL}
		echo "prc_run_id=${prc_run_id}|prc_job_name="${prc_job_name}"|prc_job_seq=${prc_job_seq}|prc_start_dt="${prc_start_dt}"|prc_run_dt="${prc_run_dt}"|prc_run_status="${prc_run_status}"" >>${fl_nm}
		msg="${fl_nm} Got Appended for ${prc_job_name} ${prc_run_id} "
	else
					msg="${fl_nm} Missing"
					fnLogMsg ERROR "${msg}" ${LOG_FL}
					exit 400
fi


}

##############################  getPrcJobStatus() ####################################
getPrcJobStatus(){
	fl_nm="$1"
	prc_job_name="$2"
	prc_job_seq="prc_job_seq=$3"
	prc_job_status=`cat  ${fl_nm} | grep -w ${prc_job_name}|awk -v prc_job_seq="$prc_job_seq" -F"|"  '$3 == prc_job_seq  { print $6; } '| tail -1| awk -F"=" '{ print $2; }'`
	echo ${prc_job_status:-NOTFOUND}
	# echo $prc_job_status
}
##############################  getJobRerunFlag() ####################################
getJobRerunFlag(){
	fl_nm="$1"
	prc_job_name="$2"
	prc_job_seq=$3
	job_rerun_fl=`cat  ${fl_nm} | grep -w ${prc_job_name}|  awk -v prc_job_seq="$prc_job_seq" -F"|"  '$4 == prc_job_seq  { print $7; } '`
	echo ${job_rerun_fl:-NOTFOUND}
}

##############################  getJobWrapperDetl() ####################################
getJobWrapperDetl(){
	fl_nm="$1"
	prc_job_name="$2"
	job_wrapper_detl=`cat  ${fl_nm} | grep -w ${prc_job_name}| cut -d"|" -f5`
	echo ${job_wrapper_detl:-NOTFOUND}
}
##############################  getJobDetl() ####################################
getJobDetl(){
	fl_nm="$1"
	prc_job_name="$2"
	outputFl="$3"
	if [[ -f ${outputFl} ]]; then
				rm ${outputFl} 2>/dev/null
	fi
				cat  ${fl_nm} | grep -w ${prc_job_name}| cut -d"|" -f 4-6 >> "${outputFl}"

	# echo ${job_code_detl:-NOTFOUND}
}

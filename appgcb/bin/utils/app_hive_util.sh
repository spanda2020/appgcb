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

LOG_FILE="$1"
export PROJECT_DIR='/data/1/'$PROJECT_BACTH_ID
export PROJECT_BIN_DIR=${PROJECT_DIR}/bin
source "${PROJECT_BIN_DIR}/config/app_config_detl.sh"

########################### fnBeelineFileExe ###################################
#Function to log messages in a log file
fnBeelineFileExe() {
beeline_url="$1"
query_file="$2"
prc_run_id=$3
prc_start_dt=$4
prc_run_dt=$5
log_fl="$6"

beeline -u ${beeline_url} -f "${query_file}"  --hivevar prc_run_id=${prc_run_id}  \
--hivevar prc_start_dt=${prc_start_dt} --hivevar prc_run_dt=${prc_run_dt}
# if [ $? -eq 0 ]; then
# 	 fnLogMsg INFO "${query_file} Executed" $LOG_FL
# else
# 	fnLogMsg ERROR "$query_file Execution Failed " $LOG_FL
# 	exit 300
# fi
# }
}
########################### fnStatusChkPrcStg ###################################
#Function to log messages in a log file
fnStatusChkPrcStg() {
beeline_url="$1"
prc_name="$2"
prc_job_name="$3"
prc_run_id=$4
chk="${prc_job_nam}~${prc_run_status}"
query="select concat_ws('~',prc_job_name,prc_run_status) from app_gcb_stg.app_prc_run_cntrl_stg where prc_run_id=${prc_run_id} and  prc_name='${prc_name}' and prc_job_name='${prc_job_name}' ; "
#beeline -u ${beeline_url} -e "select prc_run_status from app_gcb_stg.app_prc_run_cntrl where prc_run_id=20190729173520 and prc_name='prc_l1_trxn_ld' and prc_job_name='app_data_clean_trade_trxn_ld_stg' ;"

beeline -u ${beeline_url} -e "${query}" >> "${LOG_DIR}/"
if [ $? -eq 0 ]; then
	 fnLogMsg INFO "${query} Executed" $LOG_FILE
   grep $chk ${LOG_FILE}
   return 0
else
	fnLogMsg ERROR "$query Execution Failed " $LOG_FILE
	return 300
fi
}

########################### fnPrcStgLoad ###################################
#Function to log messages in a log file
fnPrcStgLoad() {
beeline_url="$1"
query_file="$2"
prc_run_status="$3"
app_id="$4"
prc_name="$5"
prc_job_name="$6"
prc_job_seq="$7"
# env_hq_file="$8"
LOG_FL="$8"
echo " 8th variable is $8"

source $param_file

#${beeline_url} ${prc_run_cntl_stg_ld_file} ${param_file} ${prc_run_status} ${app_id} ${prc_job_name} ${env_file} ${LOG_FILE}
beeline -u $beeline_url -f "${query_file}" --hivevar prc_run_id=${prc_run_id}  --hivevar prc_job_seq=${prc_job_seq}  \
--hivevar prc_start_dt=${prc_start_dt} --hivevar prc_run_dt=${prc_run_dt} \
--hivevar prc_run_status=${prc_run_status} --hivevar app_id=${app_id}   \
--hivevar prc_name=${prc_name} --hivevar prc_job_name=${prc_job_name}

if [ $? -eq 0 ]; then
	 fnLogMsg INFO "${query_file} Executed" ${LOG_FL}
else
	fnLogMsg ERROR "$query_file Execution Failed " ${LOG_FL}
	exit 300
fi
}
########################### fnGetDatesPrcCntrl ###################################
#Function to log messages in a log file
fnGetDatesPrcCntrl() {
beeline_url="$1"
app_id="$2"
prc_name="$3"
batch_freq="$4"
LOG_FL="$5"


if [ $batch_freq == 'D' ]
then
		query="select prc_name,date_add(prc_run_dt,1) as prc_start_dt,date_add(prc_run_dt,1) as prc_run_dt from (select prc_name,prc_start_dt,prc_run_dt,row_number() over
		(partition by 1 order by prc_run_id desc,prc_exec_time desc) as rnk from app_gcb_stg.app_prc_run_cntrl
		 where prc_name='${prc_name}' and prc_run_status='PRC_CNTRL_LOAD') inq where rnk=1"

elif [ $batch_freq == 'W' ]
then
	 		query="select prc_name,date_add(prc_run_dt,1) as prc_start_dt,date_add(prc_run_dt,7) as prc_run_dt from
			(select prc_name,prc_start_dt,prc_run_dt,row_number() over (partition by 1 order by prc_run_id desc,prc_exec_time desc) as rnk
			from app_gcb_stg.app_prc_run_cntrl where prc_name='${prc_name}' and prc_run_status='PRC_CNTRL_LOAD') inq where rnk=1"
elif [ $batch_freq == 'BW' ]
then
	 		query="select prc_name,date_add(prc_run_dt,1) as prc_start_dt,case when (dayofmonth(date_add(prc_run_dt,1))==1) then date_add(prc_start_dt,15)
		  else last_day(date_add(prc_start_dt,1)) end as prc_run_dt from (select prc_name,prc_start_dt,prc_run_dt,row_number()
			over (partition by 1 order by prc_run_id desc,prc_exec_time desc) as rnk from app_gcb_stg.app_prc_run_cntrl
			where prc_name='${prc_name}' and prc_run_status='PRC_CNTRL_LOAD') inq where rnk=1"
elif [ $batch_freq == 'M' ]
then
	 		query="select prc_name,date_add(prc_run_dt,1) as prc_start_dt,last_day(date_add(prc_run_dt,1))
			as prc_run_dt from (select prc_name,prc_start_dt,prc_run_dt,row_number() over (partition by 1 order by prc_run_id desc,prc_exec_time desc) as rnk
			from app_gcb_stg.app_prc_run_cntrl where prc_name='${prc_name}' and prc_run_status='PRC_CNTRL_LOAD') inq where rnk=1"
else
	 	  query="NOTFOUND"
fi

if [ "$query" = "NOTFOUND" ] ; then
		msg="Batch Frquency has not valid values"
		fnLogMsg INFO "${msg}" ${LOG_FL}
		echo "${msg}"}
		exit 1
fi
# field_Delim="~"

returnVal=`beeline --silent='$hive_verbose' --showHeader=${hive_header} --outputformat=dsv --delimiterForDSV=${field_Delim} -u ${beeline_url} -e "${query}"`

if [ $? -eq 0 ]; then
	 fnLogMsg INFO "$query Executed" ${LOG_FL}
	 echo ${returnVal:-NOTFOUND}
else
	fnLogMsg ERROR "$query Execution Failed " ${LOG_FL}
	exit 300
fi
}

################################### fnHdfsPartitionDelete ###############################
fnHdfsPartitionDelete(){
	# hdfs dfs -rm  hdfs://quickstart.cloudera:8020/user/hive/app_gcb_data/app_gcb_stg/app_prc_run_cntrl_stg/prc_name=prc_l1_trxn_ld/*
	baseHdfsPath="$1"
	schema="$2"
	tblName="$3"
	partitionName="$4"
	partitionVal="$5"
	LOG_FL="$6"
	flPath="${baseHdfsPath}"/$schema/$tblName/"$partitionName=$partitionVal"
	# fnPrcStatusFileAppend  ${PRC_STG_STATUS_FILE} ${prc_run_id}  ${prc_job_name} ${prc_seq} ${prc_start_dt} ${prc_run_dt} ${prc_run_status} ${LOG_FILE}
	hdfs dfs -ls ${flPath}
	if  [[ $? -ne 0 ]] ; then
		msg="${flPath} Not : Exist"
		fnLogMsg INFO "${msg}" ${LOG_FL}
	else
		msg="${flPath} : Exist"
		fnLogMsg INFO "${msg}" ${LOG_FL}
		hdfs dfs -rm  ${flPath}/*
		hdfs dfs -ls ${flPath}
			if  [[ $? -ne 0 ]] ; then
					msg="${flPath} Couldn't be deleted"
					fnLogMsg ERROR "${msg}" ${LOG_FL}
					exit 100
			else
					msg="${flPath} Got deleted"
					fnLogMsg INFO "${msg}" ${LOG_FL}
					echo " ${flPath} deleted !!!"
			fi
	fi

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


import os
import os.path
from os import path
import re
import sys
import importlib
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import utils.args as arg
from utils.cfg import config
from utils.sparkSession import *
from utils.logsession import getloggingSession
import pandas as pd



logger=getloggingSession()
script_nm = sys.argv[0]
logger.info(script_nm + ' -> Execution Started')

## Get all the input parameters

def getInputParam():
    logger.info('Getting all the input parameters')

    arg.parser.add_argument('app_id', help='App for which Job to be started', type=str)
    arg.parser.add_argument('prc_name', help='Process with in App to be executed', type=str)
    arg.parser.add_argument('prc_run_id', help='Execution run_id', type=int)
    # arg.parser.add_argument('prc_run_id', help='Execution run_id', type=bigint)
    args, _ = arg.parser.parse_known_args()


    global app_id
    global prc_name
    global prc_run_id
    global app_job_detl_fl
    global app_job_detl_fl_detl
    global override_fl


    app_id=args.app_id.strip()
    prc_name=args.prc_name.strip()
    prc_run_id=int(str(args.prc_run_id).strip())
    app_job_detl_fl='app_job_detl'+'_'+prc_name+'_'+str(prc_run_id)+'.txt'

    app_job_detl_fl_detl=app_tmp_dir+app_job_detl_fl
    logger.info(script_nm + ' -> app_job_detl_fl_detl :: '+ app_job_detl_fl_detl)

    override_fl=app_id+'_override_fl'
    override_fl = config.get('override_file', override_fl)
    logger.info(script_nm + ' -> override_fl :: '+ override_fl)


    logger.info(script_nm + ' -> app_id :: '+ app_id)
    logger.info(script_nm + ' -> prc_name :: '+ prc_name)
    logger.info(script_nm + ' -> prc_run_id :: '+ str(prc_run_id))
    logger.info(script_nm + ' -> app_job_detl_fl :: '+ app_job_detl_fl)


def getConfigParam():
    logger.info('Getting all the parameters from Config')
    global base_app_dir
    global prc_cntrl_db
    global app_tmp_dir

    base_app_dir = config.get('dir_details', 'base_app_dir')
    logger.info(script_nm + ' -> base_app_dir :: '+ base_app_dir)

    prc_cntrl_db = config.get('hive_db_details', 'prc_cntrl_db')
    logger.info(script_nm + ' -> prc_cntrl_db :: '+ prc_cntrl_db)

    app_tmp_dir = config.get('dir_details', 'app_tmp_dir')
    logger.info(script_nm + ' -> app_tmp_dir :: '+ app_tmp_dir)


def getAppJobDetl(app_id,prc_name,prc_run_id):
    colList=['app_id','prc_name','prc_job_name','prc_job_seq','wrapper_file_detl','code_file_detl','rerun_flag']
    if path.exists(override_fl):
        pd_df_override_fl= pd.read_csv(override_fl,sep="|",header=None,skiprows=range(0, 2),names=colList)
    else:
        pd_df_override_fl = pd.DataFrame()

    # pd_df_override_fl= pd.read_csv("/data/1/appgcb/tmp/app_data_clean_override.txt",sep="|",header=None,skiprows=range(0, 2),names=colList)
    if (pd_df_override_fl.shape[0] ==0):
        df = spark.sql(''' select *,concat(wrapper_path,'/',wrapper_file_name) as wrapper_file_detl, concat(code_path,'/',code_file_name) as code_file_detl,'N' as rerun_flag
         from %s.app_job_detl  ''' %(prc_cntrl_db)).select(*colList)
    else:
        spark_df_override=spark.createDataFrame(pd_df_override_fl).replace(float('nan'), None)
        spark_df_override.show(10,False)
        spark_df_override.createOrReplaceTempView('override_tbl')
        df = spark.sql('''select app_id,prc_name,prc_job_name,prc_job_seq,
            COALESCE(wrapper_file_detl_1,wrapper_file_detl) as wrapper_file_detl ,COALESCE(code_file_detl_1,code_file_detl) as code_file_detl,
            COALESCE(rerun_flag_1,rerun_flag) as rerun_flag from (
            select ajdt.*,'N' as rerun_flag,concat(wrapper_path,'/',wrapper_file_name) as wrapper_file_detl, concat(code_path,'/',code_file_name) as code_file_detl,
            ovbt.wrapper_file_detl as wrapper_file_detl_1,ovbt.code_file_detl as code_file_detl_1,ovbt.rerun_flag as rerun_flag_1
            from %s.app_job_detl as ajdt left outer join  override_tbl ovbt on ajdt.prc_job_name = ovbt.prc_job_name and ajdt.prc_job_seq = ovbt.prc_job_seq )''' %(prc_cntrl_db)).select(*colList)


    print( " getAppJobDetl data loaded in Data Frame")
    df.show(10,False)
    df_pandas= df.toPandas()
    df_pandas.to_csv(app_job_detl_fl_detl, sep="|", float_format='%.2f',index=False, line_terminator='\n',encoding='utf-8')
    print(app_job_detl_fl_detl," File Created !!!  ")



if __name__ == '__main__':
    print (" Sanjeeb Main Start")
    config.read('/data/1/appgcb/bin/config/param_detl.ini')
    spark = getSparkSession()
    spark.sparkContext.setLogLevel('ERROR')
    getConfigParam()
    getInputParam()
    getAppJobDetl(app_id,prc_name,prc_run_id)

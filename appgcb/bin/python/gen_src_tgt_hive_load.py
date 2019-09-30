
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
from utils.commonmodules import *
import pyspark.sql.functions as psf
############### Modules Imported #####################
logger=getloggingSession()
script_nm = sys.argv[0]
logger.info(script_nm + ' -> Execution Started')
spark = getSparkSession()
spark.sparkContext.setLogLevel('ERROR')

## Get all the input parameters

def getInputParam():
    logger.info('Getting all the input parameters')
    global sourceTbl
    global targetTbl
    global filterDtCol
    global startDt
    global runDt
    global insertOverwriteFlag
    global filterFlag
    if (sysArglen < 3):
        print(" Number of arguments are Less than expected  ")
    elif (sysArglen < 5):
        arg.parser.add_argument('sourceTbl', help='Source Db and Tbl ', type=str)
        arg.parser.add_argument('targetTbl', help='Tatget Db and Tbl' , type=str)
        arg.parser.add_argument('insertOverwriteFlag', help='Process with in App to be executed', type=str)
        args, _ = arg.parser.parse_known_args()
        sourceTbl=args.sourceTbl.strip()
        targetTbl=args.targetTbl.strip()
        insertOverwriteFlag=args.insertOverwriteFlag.strip()
        filterFlag='N'
        logger.info(script_nm + ' -> sourceTbl :: '+ sourceTbl)
        logger.info(script_nm + ' -> targetTbl :: '+ targetTbl)
        logger.info(script_nm + ' -> insertOverwriteFlag :: '+ insertOverwriteFlag)
        logger.info(script_nm + ' -> filterFlag :: '+ filterFlag)
    else:
        arg.parser.add_argument('sourceTbl', help='Source Db and Tbl ', type=str)
        arg.parser.add_argument('targetTbl', help='Tatget Db and Tbl' , type=str)
        arg.parser.add_argument('insertOverwriteFlag', help='Process with in App to be executed', type=str)
        arg.parser.add_argument('filterDtCol', help='Column to be used as filter  eap_as_of_dt', type=str)
        arg.parser.add_argument('startDt', help='Date range Start ', type=str)
        arg.parser.add_argument('runDt', help='Date range End', type=str)
        # arg.parser.add_argument('prc_run_id', help='Execution run_id', type=bigint)
        args, _ = arg.parser.parse_known_args()

        sourceTbl=args.sourceTbl.strip()
        targetTbl=args.targetTbl.strip()
        filterDtCol=args.filterDtCol.strip()
        startDt=args.startDt.strip()
        runDt=args.runDt.strip()
        insertOverwriteFlag=args.insertOverwriteFlag.strip()
        filterFlag='Y'

        logger.info(script_nm + ' -> sourceTbl :: '+ sourceTbl)
        logger.info(script_nm + ' -> targetTbl :: '+ targetTbl)
        logger.info(script_nm + ' -> filterDtCol :: '+ str(filterDtCol))
        logger.info(script_nm + ' -> startDt :: '+ startDt)
        logger.info(script_nm + ' -> runDt :: '+ runDt)
        logger.info(script_nm + ' -> insertOverwriteFlag :: '+ insertOverwriteFlag)
        logger.info(script_nm + ' -> filterFlag :: '+ filterFlag)


def loadTgtTable(sourceTbl,srcData,tgtTbl,filterFlag,tgtColLst,tgtPartitionLst,insertOverwriteFlag):

    if (filterFlag.upper() =='N' and insertOverwriteFlag.upper()=='COPY'):
        srcTblPath=spark.sql("desc formatted {}".format(sourceTbl)).filter('col_name == "Location" ').collect()[0][1]
        srcTblPath=srcTblPath+'/*'
        tgtTblPath=spark.sql("desc formatted {}".format(tgtTbl)).filter('col_name == "Location" ').collect()[0][1]
        (ret, out, err)= spark_run_cmd(['hdfs', 'dfs', '-cp','-f', srcTblPath, tgtTblPath])
        print("ret : ",ret)
    elif (insertOverwriteFlag.upper()=='APPEND'):
        partitionStr=','.join(tgtPartitionLst)
        srcDf=srcData.select(*tgtColLst)
        spark.catalog.dropTempView("srcDfView")
        srcDf.createTempView('srcDfView')
        spark.sql(''' insert into table {} partition({}) select * from srcDfView'''.format(tgtTbl,partitionStr))
    else :
        partitionStr=','.join(tgtPartitionLst)
        srcDf=srcData.select(*tgtColLst)
        spark.catalog.dropTempView("srcDfView")
        srcDf.createTempView('srcDfView')
        spark.sql(''' insert overwrite table {} partition({}) select * from srcDfView'''.format(tgtTbl,partitionStr))

if __name__ == '__main__':
    sysArglen=len(sys.argv)
    print("sysArglen :",sysArglen)
    getInputParam()
    srcData=selectSrcData(sourceTbl,filterFlag)
    tgtColLst=getColPartionDetl(targetTbl)[0]
    tgtPartitionLst=getColPartionDetl(targetTbl)[2]
    loadTgtTable(sourceTbl,srcData,targetTbl,filterFlag,tgtColLst,tgtPartitionLst,insertOverwriteFlag)
    print(" Completed !!!")

    # filterCol='load_dt'
    # startDt='20190101'
    # runDt='20190101'
    # srcData=selectSrcData(srcTbl,filterCol,startDt,runDt)
    # tgtColLst=getColPartionDetl(tgtTbl)[0]
    # tgtPartitionLst=getColPartionDetl(tgtTbl)[2]
    # loadTable(srcTbl,tgtTbl,tgtColLst,tgtPartitionLst,'Y')


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
import subprocess
############### Modules Imported #####################
logger=getloggingSession()
script_nm = sys.argv[0]
logger.info(script_nm + ' -> Execution Started')
spark = getSparkSession()
spark.sparkContext.setLogLevel('ERROR')




def spark_run_cmd(args_list):
        """
        run linux commands
        """
        # import subprocess
        print('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err

def getColPartionDetl(schemaTbl):
    dfCol=spark.sql(''' desc {} '''.format(schemaTbl))
    dfColDetllist = [list(row) for row in dfCol.collect()]
    partitionIndex=[var[0] for var in  dfColDetllist].index('# Partition Information')
    collist=[var[0] for var in  dfColDetllist[:partitionIndex]]
    colDict={var[0]:var[1] for var in  dfColDetllist[:partitionIndex] }
    partitionCollist=[var[0] for var in  dfColDetllist[partitionIndex+2:]]
    partitionColDict={var[0]:var[1] for var in  dfColDetllist[partitionIndex+2:] }
    return(collist,colDict,partitionCollist,partitionColDict)

def selectSrcData(sourceTbl,filterFlag):
    # selectSrcData(sourceTbl,filterFlag,filterDtCol,startDt,endDt):
    if (filterFlag =='Y'):
        srcDf=spark.sql(''' select * from {} where {} >='{}' and {} <='{}' '''.format(sourceTbl,filterDtCol,startDt,filterDtCol,endDt))
    else:
        srcDf=spark.sql(''' select * from {}'''.format(sourceTbl))
    srcDf.show(2,False)
    return srcDf

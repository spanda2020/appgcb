from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("my app").enableHiveSupport().getOrCreate()
# Defaults to INFO
sc = spark.sparkContext
sc.setLogLevel("WARN")
# Loads CSV file from Google Storage Bucket
#     df_loans = spark \
#         .read \
#         .format("csv") \
#         .option("header", "true") \
#         .option("inferSchema", "true") \
#         .load(storage_bucket + "/" + data_file)
# # Creates temporary view using DataFrame
#     df_loans.withColumnRenamed("Country", "country") \
#         .withColumnRenamed("Country Code", "country_code") \
#         .withColumnRenamed("Disbursed Amount", "disbursed") \
#         .withColumnRenamed("Borrower's Obligation", "obligation") \
#         .withColumnRenamed("Interest Rate", "interest_rate") \
#         .createOrReplaceTempView("loans")
# Performs basic analysis of dataset
# df = spark.sql(''' select * from app_gcb_stg.app_prc_run_cntrl_STG ''' )
# df.show(10)
# spark.sql(''' show databases ''').show()

def read_df():
    df = spark.sql(''' select * from app_gcb_stg.app_prc_run_cntrl ''' )
    df.show(10)


# spark.stop()
if __name__ == "__main__":
    # main(sys.argv[1:])
    read_df()
import os.path
from os import path
colList=['app_id','prc_name','prc_job_name','prc_job_seq','wrapper_file_detl','code_file_detl','rerun_flag']

if path.exists("/data/1/appgcb/tmp/app_data_clean_override.txt"):
    pd_df_override_fl= pd.read_csv("/data/1/appgcb/tmp/app_data_clean_override.txt",sep="|",header=None,skiprows=range(0, 2),names=colList)
else:
    pd_df_override_fl = pd.DataFrame()

def getColPartionDetl(schemaTbl):
    dfCol=spark.sql(''' desc {} '''.format(schemaTbl))
    dfColDetllist = [list(row) for row in dfCol.collect()]
    partitionIndex=[var[0] for var in  dfColDetllist].index('# Partition Information')
    collist=[var[0] for var in  dfColDetllist[:partitionIndex]]
    colDict={var[0]:var[1] for var in  dfColDetlSrclist[:partitionIndex] }
    partitionCollist=[var[0] for var in  dfColDetllist[partitionIndex+2:]]
    partitionColDict={var[0]:var[1] for var in  dfColDetlSrclist[partitionIndex+2:] }
    return(collist,colDict,partitionCollist,partitionColDict)

def loadTable(srcData,tgtTbl,tgtColLst,tgtPartitionLst,insertOverwriteFlag):
    partitionStr=','.join(tgtPartitionLst)
    srcDf=spark.sql(''' select * from {}'''.format(srcData)).select(*tgtColLst)
    spark.catalog.dropTempView("srcDfView")
    srcDf.createTempView('srcDfView')
    spark.sql(''' insert overwrite table {} partition({}) select * from srcDfView'''.format(tgtTbl,partitionStr))

def selectSrcData(sourceTbl,filterDtCol,startDt,endDt):
    print('sourceTbl',sourceTbl)
    print('filterDtCol',filterDtCol)
    print('startDt',startDt)
    print('endDt',endDt)
    srcDf=spark.sql(''' select * from {} where {} >='{}' and {} <='{}' '''.format(sourceTbl,filterDtCol,startDt,filterDtCol,endDt))
    # srcDf.show(5,False)
    # print(" Count of the source Data is ",srcDf.count())
    return srcDf

filterCol='load_dt'
startDt='20190101'
runDt='20190101'
srcData=selectSrcData(srcTbl,filterCol,startDt,runDt)
tgtColLst=getColPartionDetl(tgtTbl)[0]
tgtPartitionLst=getColPartionDetl(tgtTbl)[2]
loadTable(srcTbl,tgtTbl,tgtColLst,tgtPartitionLst,'Y')
srcTbl='app_gcb_stg.sampleTbl1'
tgtTbl='app_gcb_stg.sampleTbl2'
filterCol='load_dt'
startDt='20190101'
runDt='20190101'
insertOverwriteFlag='Y'
spark-submit /data/1/appgcb/python/gen_src_tgt_hive_load.py  "${srcTbl}" "${tgtTbl}" $filterCol ${startDt} ${runDt} $insertOverwriteFlag
srcTbl='app_gcb_stg.sampleTbl1'
tgtTbl='app_gcb_stg.sampleTbl2'
dfColSrc=spark.sql(''' desc {} '''.format(srcTbl))
dfColTgt=spark.sql(''' desc {} '''.format(tgtTbl))
print(" Out is",getColPartionDetl(srcTbl))
print(" Out is",getColPartionDetl(tgtTbl))

dfCol=spark.sql(''' desc {} '''.format('app_gcb_stg.sampleTbl2'))


tgtColLst=getColPartionDetl(tgtTbl)[0]
tgtPartitionLst=getColPartionDetl(tgtTbl)[2]

loadTable(srcTbl,tgtTbl,tgtColLst,tgtPartitionLst,'Y')
inert into table {} partitions()
lst=['refnbr','load_dt']
ls=','.join(lst)
x='refnbr,load_dt'
spark.sql(''' insert overwrite table app_gcb_stg.sampleTbl2 partition(refnbr, load_dt) select acct1, tranid, refnbr, load_dt from app_gcb_stg.sampleTbl1''')
spark.sql(''' insert overwrite table app_gcb_stg.sampleTbl2 partition({}) select acct1, tranid, refnbr, load_dt from app_gcb_stg.sampleTbl1'''.format(x))

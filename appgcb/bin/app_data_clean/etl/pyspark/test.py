import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window as W
import pyspark.sql.functions as psf
from pyspark.sql.functions import udf,lit,col,count,max,when,sum,countDistinct
from pyspark.sql.types import *
import math
#import numpy as np
#import pandas as pd
from datetime import datetime, timedelta
from pyspark.conf import SparkConf


if __name__ == "__main__":
           # Configure Spark
        SparkSession.builder.config(conf=SparkConf())
        spark = SparkSession.builder.appName("generic_data_load_process").enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict")
        #.config("spark.executor.instances", "10").config("spark.executor.cores","4")\
        #.config("spark.executor.memory","16G").config("spark.executor.memoryOverhead","8G").config("spark.driver.memory","5G")\
        #.config("spark.driver.memoryOverhead","2G").getOrCreate()
        #sc = spark.sparkContext

        src_schema = sys.argv[1]
        src_tbl = sys.argv[2]
        tgt_schema = sys.argv[3]
        tgt_tbl = sys.argv[4]

        spark.sql(''' use %s ''' %(src_schema))
        spark.sql(''' select * from %src_tbl limit 10 ''' %(src_tbl)).show(10,False)

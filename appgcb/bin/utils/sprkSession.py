'''
This file is exposes a utility function called get SparkSession to return a valid spark session

'''
from util.cfg import config
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def getSparkSession():
    spark = SparkSession.builder.config(conf=SparkConf()).enableHiveSupport().getOrCreate()
    spark.sql('SET hive.execution.engine = spark')
    spark.sql('USE {}'.format(config.get('hive', 'schema')))
    spark.sql('SET hive.exec.dynamic.partition = true')
    spark.sql('SET hive.exec.dynamic.partition.mode = nonstrict')
    return spark

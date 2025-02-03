

from pyspark.sql import SparkSession
from lib.ConfigLoader import get_config_spark


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_config_spark(env))\
            .config('spark.driver.extraJavaOptions',
                    '-Dlog4j.configuration=file:log4j.properties') \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .config(conf= get_config_spark(env))\
            .enableHiveSupport() \
            .getOrCreate()



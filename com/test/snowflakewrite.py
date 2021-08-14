from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import yaml
import os.path
import sys
#from com.utils import aws_utils as ut

def s3_data_load_snow(spark, file_read):
    df_snow = spark \
        .read \
        .option("mode", "DROPMALFORMED") \
        .option("header", "False") \
        .option("delimiter", "~") \
        .option("inferSchema", "true") \
        .csv(file_read)

    return df_snow





if __name__ == '__main__':
    os.environ["PYSPARK_SUBMIT_ARGS"] = ('--packages "com.amazonaws:aws-java-sdk:1.7.4","org.apache.hadoop:hadoop-aws:2.7.4","net.snowflake:snowflake-jdbc:3.11.1","net.snowflake:spark-snowflake_2.11:2.4.14-spark_2.4" pyspark-shell')
    current_dir = os.path.abspath(os.path.dirname(__file__))

    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secret_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secret_path)
    app_secret = yaml.load(secret,Loader=yaml.FullLoader)

    #Create spark session
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .getOrCreate()

    # snowflake property setting
    #spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    print('bala')
    sc = spark.sparkContext
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])


    s3_path = s3_data_load_snow(spark,'s3a://' + app_conf['CP']['s3_conf_read']['s3_bucket_read'] + '/' + 'cust_name.csv')
    s3_path.show(5,False)

    # set the below snowflake properties for connectivity

    sfOptions = {
        "sfURL": "https://ow87045.eu-west-1.snowflakecomputing.com",
        "sfAccount": "OW87045",
        "sfUser": "balait560",
        "sfPassword": "Lakshan@06",
        "sfDatabase": "DEMO_DB",
        "sfSchema": "PUBLIC",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "sysadmin"
    }

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    s3_path.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "cust_name").mode('append').save()



##spark-submit --packages "com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.4,net.snowflake:snowflake-jdbc:3.11.1,net.snowflake:spark-snowflake_2.11:2.4.14-spark_2.4" com/test/snowflakewrite.py
#working spark-submit --packages "com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.4,net.snowflake:snowflake-jdbc:3.12.8,net.snowflake:spark-snowflake_2.11:2.7.2-spark_2.4" com/test/snowflakewrite.py

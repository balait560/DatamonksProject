from pyspark.sql import SparkSession
import yaml
import os.path
import sys
from pyspark.sql.functions import current_date

from com.utils import aws_utils as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15","com.amazonaws:aws-java-sdk:1.7.4","org.apache.hadoop:hadoop-aws:2.7.4","com.springml:spark-sftp_2.11:1.1.3","org.mongodb.spark:mongo-spark-connector_2.11:2.2.2" pyspark-shell'
    )
    current_dir = os.path.abspath(os.path.dirname(__file__))

    app_config_path = os.path.abspath(current_dir  + "/../../" + "application.yml")

    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read com.test enterprise applications") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"] + "/customer.address") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])
    #hadoop_conf.set("fs.s3a.endpoint", "s3.eu-west-1.amazonaws.com")
    #hadoop_conf.set("fs.s3a.readahead.range", '512M')

    src_list = app_conf['source_list']

    for src in src_list:
        src_config = app_conf[src]
        stg_path = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_location'] + '/' + src
        #s3_read = 's3a://'  + app_conf['ADDR']['s3_conf_read']['s3_bucket_read'] +'/KC_Extract_1_20171009.csv'

        if src == 'SB':
            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            mysql_transaction_df = ut.mysql_SB_data_load(spark,app_secret,src_config)
            mysql_transaction_df.show()
            mysql_transaction_df.write.partitionBy('ins_dt').mode('overwrite').parquet(stg_path)
            print("Write the SB data in S3 completed")
    # SFTP source
        elif src == 'OL':
            print("\nReading data from SFTP  using SparkSession.read.format(),")

            sftp_loyalty_df = ut.sftp_data_load(spark,
                                                src_config["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv",
                                                app_secret,os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]))\
                                                .withColumn('ins_dt', current_date())
            sftp_loyalty_df.show(5, False)
            sftp_loyalty_df.write.partitionBy('ins_dt').mode('overwrite').parquet(stg_path)
            print("Write the OL data in S3 completed")
            # S3 Source
        elif src == 'CP':
            print("\nReading data from Read s3 using SparkSession.read.format(),")
            s3_campaigns_df = ut.s3_data_load(spark, 's3a://' + src_config['s3_conf_read']['s3_bucket_read'] + '/KC_Extract_1_20171009.csv')
            s3_campaigns_df.show(5, False)
            s3_campaigns_df.write.partitionBy('ins_dt').mode('overwrite').parquet(stg_path)
            print("Write the CP data in S3 completed")
    #MongoDB Source
        elif src == 'ADDR':
            print("\nReading data from mongodb using SparkSession.read.format(),")
            mongo_customer_df = ut.mongo_data_load(spark,src_config["mongodb_config"]["database"],
                                                   src_config["mongodb_config"]["collection"])
            mongo_customer_df.show(5,False)
            mongo_customer_df.write.partitionBy('ins_dt').mode('overwrite').parquet(stg_path)
            print("Write the ADDR data in S3 completed")

# spark-submit --packages "mysql:mysql-connector-java:8.0.15" /home/hadoop/DatamonksProject/com/test/source_data_loading.py
#.config("spark.mongodb.input.uri=mongodb://34.251.201.160/customer.address") \

##spark - submit --packages "mysql:mysql-connector-java:8.0.15,org.mongodb.spark:mongo-spark-connector_2.11:2.2.2" / home / hadoop / DatamonksProject / com / test / source_data_loading.py

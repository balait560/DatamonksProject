import os.path
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from com.utils import aws_utils as ut

if __name__ == '__main__':
    os.environ["PYSPARK_SUBMIT_ARGS"] = ('--packages "mysql:mysql-connector-java:8.0.15","com.amazonaws:aws-java-sdk:1.7.4","org.apache.hadoop:hadoop-aws:2.7.4","com.springml:spark-sftp_2.11:1.1.3","org.mongodb.spark:mongo-spark-connector_2.11:2.2.2" pyspark-shell')
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")
    conf = open(app_config_path)
    app_conf = yaml.load(conf,Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret,Loader=yaml.FullLoader)
    #Create spark session
    spark = SparkSession \
            .builder\
            .appName("Read s3 staging files for datamart creation")\
            .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key",app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key",app_secret["s3_conf"]["secret_access_key"])
    tgt_list = app_conf["target_list"]
    for tgt in tgt_list:
        tgt_conf = app_conf[tgt]
        if tgt == 'REGIS_DIM':
           print("Loading source data from S3")
           for src in tgt_conf['source_data']:
               file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" +app_conf["s3_conf"]["staging_location"] + "/" + src
               src_df = ut.read_parquet_from_s3(spark,file_path)
               src_df.show()
               if src == 'ADDR':
                   src_df = src_df.withColumn("street",col("address.street")) \
                            .withColumn("city",col("address.city")) \
                            .withColumn("state", col("address.state")) \
                            .drop("address")
               src_df.createOrReplaceTempView(src)
               src_df.printSchema()
           print("Preparing target data for Data Mart ",tgt)
           regis_dim_df = spark.sql(tgt_conf['loadingQuery'])
           regis_dim_df.show()
           jdbc_url = ut.get_redshift_jdbc_url(app_secret)
           print(jdbc_url)
           print("Write data in redshift")
           ut.write_data_to_Redshift(regis_dim_df,
                                     jdbc_url,
                                     "s3a://" + app_conf["s3_conf"]["s3_bucket"],
                                     tgt_conf["tableName"])

           print("Write completed")
        elif tgt == 'CHILD_DIM':
           src = tgt_conf['source_data']
           file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_location"] + "/" + src
           src_df = ut.read_parquet_from_s3(spark,file_path)
           src_df.createOrReplaceTempView(src)
           src_df.printSchema()
           src_df.show(5, False)
           print('Preparing the',tgt,'data,')
           child_dim_df = spark.sql(tgt_conf['loadingQuery'])
           child_dim_df.show()
           jdbc_url = ut.get_redshift_jdbc_url(app_secret)
           ut.write_data_to_Redshift(child_dim_df,
                                     jdbc_url,"s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                     tgt_conf['tableName'])
           print("Completed <<<<<<<< child table")
        elif tgt == 'RTL_TXN_FCT':
            for tgt in tgt_list:
                print('preparing',tgt,'data,')
                tgt_conf = app_conf[tgt]
                print('Loading the source data')
                for src in tgt_conf['source_data']:
                    file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_location"] + "/" + src
                    src_df = ut.read_parquet_from_s3(spark,file_path)
                    src_df.createOrReplaceView(src)
                    src_df.printSchema()
                    src_df.show(5,False)
                jdbc_url = ut.get_redshift_jdbc_url(app_secret)
                print(jdbc_url)
                REGIS_DIM = spark.read \
                            .format("io.github.spark_redshift_community.spark.redshift")\
                            .option("jdbc",jdbc_url) \
                            .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                            .option("forward_spark_s3_credentials", "true") \
                            .option("dbtable", "DATAMART.REGIS_DIM") \
                            .load()
                REGIS_DIM.show(5,False)
                REGIS_DIM.createTempView("REGIS_DIM")
                print('preparing ', tgt,'data ,')
                rtl_txf_fct_df = spark.sql(tgt_conf['loadingQuery'])
                rtl_txf_fct_df.show()
                ut.write_data_to_Redshift(rtl_txf_fct_df,jdbc_url,
                                          "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                         tgt_conf['tablename'])
                print('Completed <<<<<<<<',tgt_conf['tablename'])





# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py
#  spark-submit  --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py






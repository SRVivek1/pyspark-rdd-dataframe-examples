"""
This application demonstrates how to read/write Apache Parquet files in spark.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os.path
import yaml


if __name__ == '__main__':
    print('\n************************** Spark - Read/Write Parquet files **************************')

    # Using legacy version of Parquet file.
    sparkSession = SparkSession \
        .builder \
        .appName('parquet-to-dataframe')\
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("\nCreating dataframe ingestion parquet file using 'SparkSession.read.parquet()',")
    nyc_omo_df = sparkSession.read \
        .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/NYC_OMO") \
        .repartition(5)

    print('\n************* # of partitions : ' + str(nyc_omo_df.rdd.getNumPartitions()))
    print('\n************* # of records : ' + str(nyc_omo_df.count()))

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(5, False)')
    nyc_omo_df.show(5, False)

    # Repartition
    print('\n************* nyc_omo_df = nyc_omo_df.repartition(5)')
    nyc_omo_df = nyc_omo_df.repartition(5)

    print('\n************* # of partitions : ' + str(nyc_omo_df.rdd.getNumPartitions()))
    print('\n************* # of records : ' + str(nyc_omo_df.count()))

    print('\n************* nyc_omo_df.printSchema()')
    nyc_omo_df.printSchema()

    print('\n************* nyc_omo_df.show(5, False)')
    nyc_omo_df.show(5, False)

    print('\n************* Summery of NYC Open Market Order (OMO) charges dataset : nyc_omo_df.describe().show()')
    nyc_omo_df.describe().show()

    print('\n************* Distinct Boroughs in record : nyc_omo_df.select(col(\'Boro\')).distinct().show(100)')
    nyc_omo_df.select(col('Boro')).distinct().show(100)

    print('\n************* OMO frequency distribution of different Boroughs')
    nyc_omo_df \
        .groupBy('Boro') \
        .avg({'Boro': 'count'}) \
        .show()


# Command
# -----------------
#   spark-submit dataframe/ingestion/dataframe_from_files/parquet_to_dataframe.py
#
# Output
# -----------------
#

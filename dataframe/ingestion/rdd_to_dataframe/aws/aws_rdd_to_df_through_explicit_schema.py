"""
    Problem:
    -------------
        --> Create DataFrame from RDD with explicitly defining the schema.

    Platform:
    -------------
        --> AWS EMR, Cloud

"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField,IntegerType, LongType, DoubleType, StringType, TimestampType
import os
import yaml


if __name__ == '__main__':

    spark = SparkSession.builder\
        .appName('AWS - RDD to DataFrame - explicit schema')\
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # Read configuration files
    current_dir = os.path.abspath((os.path.dirname(__file__)))
    app_conf = yaml.load(
        open(os.path.abspath(current_dir + '../../../../../' + 'application.yaml')), Loader=yaml.FullLoader)
    secrets = yaml.load(
        open(os.path.abspath(current_dir + '../../../../../../' + '.secrets')), Loader=yaml.FullLoader)

    # Update S3 access tokens
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.access.key', secrets['s3_conf']['access_key'])
    hadoop_conf.set('fs.s3a.secret.key', secrets['s3_conf']['secret_access_key'])

    # read data from AWS S3
    # Row(int, int, float, int, int, int, str)
    txn_fct_rdd = sc.textFile('s3a://' + app_conf['s3_conf']['s3_bucket'] + 'txn_fct.csv') \
        .filter(lambda rec: rec.find('txn_id|create_time|')) \
        .map(lambda rec: rec.split('|')) \
        .map(lambda rec: Row(int(rec[0]), int(rec[1]), float(rec[2]), int(rec[3]), int(rec[4]), int(rec[5]), str(rec[6])))

    # Define a Schema of StructType
    txn_fct_schema = StructType([
        StructField(name='txn_id', dataType=LongType(), nullable=False),
        StructField(name='created_time_str', dataType=LongType(), nullable=False),
        StructField(name='amount', dataType=DoubleType(), nullable=False),
        StructField(name='cust_id', dataType=LongType(), nullable=False),
        StructField(name='status', dataType=IntegerType(), nullable=False),
        StructField(name='merchant_id', dataType=LongType(), nullable=False),
        StructField(name='created_time_ist', dataType=StringType(), nullable=False)
    ])

    txn_fct_df = spark.createDataFrame(txn_fct_rdd, txn_fct_schema);
    txn_fct_df.printSchema()
    txn_fct_df.show(10, False, False)


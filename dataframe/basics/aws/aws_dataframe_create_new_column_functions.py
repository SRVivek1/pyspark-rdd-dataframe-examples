"""
    This program demonstrates how to create/replace/update columns in dataframe.
"""
from jupyter_server.utils import unix_socket_in_use
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import unix_timestamp, approx_count_distinct, sum
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType, TimestampType
import os
import yaml

if __name__ == '__main__':

    # Create session
    spark = SparkSession.builder \
        .appName('DF POC - wwithColumns API') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # READ CONF
    cur_dir = os.path.abspath(os.path.dirname(__file__))
    app_conf = yaml.load(open(os.path.abspath(cur_dir + '../../../../' + 'application.yml')), Loader=yaml.FullLoader)
    secrets = yaml.load(open(os.path.abspath(cur_dir + '../../../../' + '.secrets')), Loader=yaml.FullLoader)

    # Update s3 access config
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.access.key', secrets['s3_conf']['access_key'])
    hadoop_conf.set('fs.s3a.secret.key', secrets['s3_conf']['secret_access_key'])

    # Read file from s3
    txn_fct_rdd = sc.textFile('s3a://' + app_conf['s3_conf']['s3_bucket'] + 'txn_fct.csv') \
        .filter(lambda rec: rec.find('txn_id|create_time|amount')) \
        .map(lambda rec: rec.split('|')) \
        .map(lambda rec: Row(int(rec[0]), int(rec[1]), float(rec[2]), int(rec[3]), int(rec[4]), int(rec[5]), str(rec[6])))

    # Create schema
    txn_fct_schema = StructType([
        StructField(name='txn_id', dataType=LongType(), nullable=False),
        StructField(name='create_date', dataType=LongType(), nullable=False),
        StructField(name='amount', dataType=DoubleType(), nullable=False),
        StructField(name='cust_id', dataType=LongType(), nullable=False),
        StructField(name='status', dataType=IntegerType(), nullable=False),
        StructField(name='merchant_id', dataType=LongType(), nullable=False),
        StructField(name='create_time_ist', dataType=StringType(), nullable=False)
    ])

    # Create DataFrame
    txn_fct_df = spark.createDataFrame(txn_fct_rdd, txn_fct_schema)

    txn_fct_df.printSchema()
    txn_fct_df.show(n=10, truncate=False, vertical=False)

    # Create a new column - withColumn(colName: str, col: Column)
    # Convert str date format to Unix time stamp
    txn_updated_df = txn_fct_df.withColumn(
        colName='create_time_ist_date',
        col=unix_timestamp(timestamp=txn_fct_df['create_time_ist'], format='yyyy-MM-dd HH:mm:ss')\
            .cast(TimestampType())
    )

    # Add new column MRP -> amount * 1.20
    txn_updated_df = txn_updated_df.withColumn(colName='mrp', col=txn_updated_df['amount'] * 1.20)

    txn_updated_df.printSchema()
    txn_updated_df.show(n=10, truncate=False, vertical=False)

    # Find record counts
    records = txn_updated_df.count()
    print(f'total records in DataFrame : {records}')

    # Unique merchants
    merchants = txn_updated_df.select(['merchant_id']).distinct().count()
    print(f'Unique merchants count # : {merchants}')

    # Get partition info
    partitions = txn_updated_df.rdd.getNumPartitions()
    print(f'Partitions {partitions}')

    # Group by operations
    temp = txn_updated_df.repartition(10, txn_updated_df['merchant_id'])

    partitions = temp.rdd.getNumPartitions()
    print(f'Partitions {partitions}')

    # Group by function
    print('************88Group By function in DataFrame')
    tempDF = txn_updated_df \
        .repartition(10, txn_updated_df['merchant_id']) \
        .groupBy(txn_updated_df['merchant_id']) \
        .agg(sum('amount'), approx_count_distinct(txn_fct_df['status']))

    tempDF.sort('merchant_id', ascending=True).show()

    # Rename existing columns
    # Provide better names to new cols created by agg functions.
    tempDF = tempDF \
        .withColumnRenamed('sum(amount)', 'total_amount') \
        .withColumnRenamed('approx_count_distinct(status)', 'distinct_status_count')

    print('--------------------- With new column names')
    tempDF.show()

# Command
# spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' --master yarn ./program.py

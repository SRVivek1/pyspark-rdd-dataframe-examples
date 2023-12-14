"""
This program demonstrates the how to validate the schema when creating DataFrame objects.
"""


from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import unix_timestamp, approx_count_distinct, sum
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType, TimestampType

import constants.app_constants as app_constants


if __name__ == '__main__':
    sparkSession = SparkSession \
        .builder \
        .appName('rdd-to-df-schema-validation') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel('ERROR')

    # RDD[Row(int, int, float, int, int, int, str)]
    txn_fct_rdd = sparkContext.textFile(app_constants.txn_fct_csv_file) \
        .filter(lambda rec: rec.find('txn_id')) \
        .map(lambda rec: rec.split("|")) \
        .map(lambda rec: Row(int(rec[0]),
                             int(rec[1]),
                             float(rec[2]),
                             int(rec[3]),
                             int(rec[4]),
                             int(rec[5]),
                             str(rec[6])))

    print('\n*************** RDD sample data read from file')
    for row in txn_fct_rdd.take(5):
        print(row)

    # Define Schema
    txn_fct_rdd_schema = StructType([
        StructField('txn_id', LongType(), False),
        StructField('create_time', LongType(), False),
        StructField('amount', DoubleType(), True),
        StructField('cust_id', LongType(), True),
        StructField('status', IntegerType(), True),
        StructField('merchant_id', LongType(), True),
        StructField('create_time_ist', StringType(), True)
    ])

    # Create DataFrame
    print('\n**************** Rdd to DF using scheme - sparkSession.createDataFrame(txn_fct_rdd, txn_fct_rdd_schema)')
    txn_fct_df = sparkSession.createDataFrame(txn_fct_rdd, txn_fct_rdd_schema)

    print('\n**************** DF Schema - txn_fct_df.printSchema()')
    txn_fct_df.printSchema()

    print('\n**************** txn_fct_df.show(5) ')
    txn_fct_df.show(5)

    # Transformation on DataFrame using DSL
    txn_fct_df = txn_fct_df \
        .withColumn('create_time_ist',
                    unix_timestamp(txn_fct_df['create_time_ist'], 'yyyy-MM-dd HH:mm:ss')
                    .cast(TimestampType()))

    print('\n**************** DF New Schema - txn_fct_df.printSchema()')
    txn_fct_df.printSchema()

    print('\n**************** txn_fct_df.show(5) ')
    txn_fct_df.show(5)

    # Find record counts
    print('# of records : ' + str(txn_fct_df.count()))
    print('# of merchants : ' + str(txn_fct_df.select(txn_fct_df['merchant_id']).distinct().count()))

    # Apply GroupBy Functions
    txnAggDf = txn_fct_df \
        .repartition(10, txn_fct_df['merchant_id']) \
        .groupBy('merchant_id') \
        .agg(sum('amount'), approx_count_distinct(txn_fct_df['status']))

    print('\n************** New DF after agg functions')
    txnAggDf.show()

    # Provide better names to new cols created by agg functions.
    txnAggDf = txnAggDf \
        .withColumnRenamed('sum(amount)', 'total_amount') \
        .withColumnRenamed('approx_count_distinct(status)', 'distinct_status_count')

    print('\n************** New DF with updated column names')
    txnAggDf.show()

# Command
# -------------------
#   spark-submit --master 'local[*]' ./dataframe/ingestion/rdd_to_dataframe/rdd_to_df_through_explicit_schema.py
#   spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' --master yarn ./dataframe/ingestion/rdd_to_dataframe/rdd_to_df_through_explicit_schema.py

# Output
# -------------------
# *************** RDD sample data read from file
# <Row(3509928476, 20190101, 292.0, 7155966814, 0, 10492, '2019-01-01 00:15:25')>
# <Row(3509960732, 20190101, 199.0, 7155969502, 0, 259, '2019-01-01 01:15:41')>
# <Row(3509992988, 20190101, 885.9, 7155972190, 0, 1429, '2019-01-01 04:07:17')>
# <Row(3510025244, 20190101, 119.0, 7155974878, 0, 262, '2019-01-01 07:06:56')>
# <Row(3510057500, 20190101, 169.0, 7155977566, 1, 259, '2019-01-01 07:56:02')>
#
# **************** Rdd to DF using scheme - sparkSession.createDataFrame(txn_fct_rdd, txn_fct_rdd_schema)
#
# **************** DF Schema - txn_fct_df.printSchema()
# root
#  |-- txn_id: long (nullable = false)
#  |-- create_time: long (nullable = false)
#  |-- amount: double (nullable = true)
#  |-- cust_id: long (nullable = true)
#  |-- status: integer (nullable = true)
#  |-- merchant_id: long (nullable = true)
#  |-- create_time_ist: string (nullable = true)
#
#
# **************** txn_fct_df.show(5)
# +----------+-----------+------+----------+------+-----------+-------------------+
# |    txn_id|create_time|amount|   cust_id|status|merchant_id|    create_time_ist|
# +----------+-----------+------+----------+------+-----------+-------------------+
# |3509928476|   20190101| 292.0|7155966814|     0|      10492|2019-01-01 00:15:25|
# |3509960732|   20190101| 199.0|7155969502|     0|        259|2019-01-01 01:15:41|
# |3509992988|   20190101| 885.9|7155972190|     0|       1429|2019-01-01 04:07:17|
# |3510025244|   20190101| 119.0|7155974878|     0|        262|2019-01-01 07:06:56|
# |3510057500|   20190101| 169.0|7155977566|     1|        259|2019-01-01 07:56:02|
# +----------+-----------+------+----------+------+-----------+-------------------+
# only showing top 5 rows
#
#
# **************** DF New Schema - txn_fct_df.printSchema()
# root
#  |-- txn_id: long (nullable = false)
#  |-- create_time: long (nullable = false)
#  |-- amount: double (nullable = true)
#  |-- cust_id: long (nullable = true)
#  |-- status: integer (nullable = true)
#  |-- merchant_id: long (nullable = true)
#  |-- create_time_ist: timestamp (nullable = true)
#
#
# **************** txn_fct_df.show(5)
# +----------+-----------+------+----------+------+-----------+-------------------+
# |    txn_id|create_time|amount|   cust_id|status|merchant_id|    create_time_ist|
# +----------+-----------+------+----------+------+-----------+-------------------+
# |3509928476|   20190101| 292.0|7155966814|     0|      10492|2019-01-01 00:15:25|
# |3509960732|   20190101| 199.0|7155969502|     0|        259|2019-01-01 01:15:41|
# |3509992988|   20190101| 885.9|7155972190|     0|       1429|2019-01-01 04:07:17|
# |3510025244|   20190101| 119.0|7155974878|     0|        262|2019-01-01 07:06:56|
# |3510057500|   20190101| 169.0|7155977566|     1|        259|2019-01-01 07:56:02|
# +----------+-----------+------+----------+------+-----------+-------------------+
# only showing top 5 rows
#
# # of records : 100
# # of merchants : 47
#
# ************** New DF after agg functions
# +-----------+-----------+-----------------------------+
# |merchant_id|sum(amount)|approx_count_distinct(status)|
# +-----------+-----------+-----------------------------+
# |        247|       35.0|                            1|
# |       2333|     3011.5|                            1|
# |        229|     264.14|                            1|
# |        268|      548.0|                            1|
# |        227|      471.0|                            1|
# |        250|      119.0|                            1|
# |        244|     825.58|                            1|
# |       8596|     781.92|                            1|
# |       7384|     2500.0|                            1|
# |        267|      399.0|                            1|
# |      30287|     7146.0|                            1|
# |        222|      397.0|                            1|
# |        234|    1178.46|                            1|
# |        213|     1250.0|                            1|
# |       5861|     643.58|                            1|
# |        261|       35.0|                            1|
# |        243|     1887.0|                            1|
# |       1429|    5137.72|                            2|
# |        248|      154.0|                            2|
# |        257|      448.0|                            1|
# +-----------+-----------+-----------------------------+
# only showing top 20 rows
#
#
# ************** New DF with updated column names
# +-----------+------------+---------------------+
# |merchant_id|total_amount|distinct_status_count|
# +-----------+------------+---------------------+
# |        247|        35.0|                    1|
# |       2333|      3011.5|                    1|
# |        229|      264.14|                    1|
# |        268|       548.0|                    1|
# |        227|       471.0|                    1|
# |        250|       119.0|                    1|
# |        244|      825.58|                    1|
# |       8596|      781.92|                    1|
# |       7384|      2500.0|                    1|
# |        267|       399.0|                    1|
# |      30287|      7146.0|                    1|
# |        222|       397.0|                    1|
# |        234|     1178.46|                    1|
# |        213|      1250.0|                    1|
# |       5861|      643.58|                    1|
# |        261|        35.0|                    1|
# |        243|      1887.0|                    1|
# |       1429|     5137.72|                    2|
# |        248|       154.0|                    2|
# |        257|       448.0|                    1|
# +-----------+------------+---------------------+
# only showing top 20 rows

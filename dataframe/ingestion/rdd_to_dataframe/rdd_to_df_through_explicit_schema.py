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

    txn_fct_rdd = sparkContext.textFile(app_constants.txn_fct_csv_file) \
        .filter(lambda rec: rec.find('txn_id')) \
        .map(lambda rec: rec.split("|")) \
        .map(lambda rec: Row(int(rec[0]), int(rec[1]), float(rec[2]), int(rec[3]), int(rec[4]), int(rec[5]), str(rec[6])))
        # RDD[Row(int, int, float, int, int, int, str)]

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
    txn_fct_df \
        .withColumn('create_time_ist', unix_timestamp(txn_fct_df['create_time_ist'], 'yyyy-MM-dd HH:mm:ss') \
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
        .agg(sum('amount'), approx_count_distinct('status'))

    print('\n************** New DF after agg functions')
    txnAggDf.show()

    # Provide better names to new cols created by agg functions.
    txnAggDf \
        .withColumnRenamed('sum(amount)', 'total_amount') \
        .withColumnRenamed('approx_count_distinct(status)', 'distinct_status_count')

    print('\n************** New DF with updated column names')
    txnAggDf.show()

# Command
#   spark-submit --master 'local[*]' ./dataframe/ingestion/rdd_to_dataframe/rdd_to_df_through_explicit_schema.py

# Output
#
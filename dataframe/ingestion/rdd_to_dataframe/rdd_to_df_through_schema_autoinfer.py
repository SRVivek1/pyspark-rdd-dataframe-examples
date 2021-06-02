"""
This program demonstrates how to create DataFrame from RDD without Schema definition.
"""


from pyspark.sql import SparkSession
import constants.app_constants as appConstants


if __name__ == '__main__':

    sparkSession = SparkSession \
        .builder \
        .appName('rdd-to-dataframe') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("ERROR")

    # Load data from local
    print('\n**************** Resource : {0}'.format(appConstants.txn_fct_csv_file))

    txn_fct_rdd = sparkContext.textFile(appConstants.txn_fct_csv_file)
    print('\n***************** Raw data snippet : ')
    print(txn_fct_rdd.take(5))

    txn_fct_rdd = txn_fct_rdd.filter(lambda record: record.find('txn_id|create_time|'))
    print("\n***************** Remove header : record.find('txn_id|create_time|')")
    print(txn_fct_rdd.take(5))

    txn_fct_rdd = txn_fct_rdd \
        .map(lambda record: record.split('|')) \
        .map(lambda record: (int(record[0]), record[1], float(record[2]), record[3], record[4], record[5], record[6]))

    print("\n***************** Final transformed RDD")
    for rec in txn_fct_rdd.take(5):
        print(rec)

    # Create dataframe object from rdd using RDD.toDF() method - without column names
    print('\n***************** Convert RDD to DataFrame using toDF() - without column names')
    txnDfNoColumnNames = txn_fct_rdd.toDF()

    # Print schema
    print('\n***************** txnDfNoColumnNames.printSchema()')
    txnDfNoColumnNames.printSchema()

    # Show 5 records without truncating
    print('\n***************** txnDfNoColumnNames.show(5, False)')
    txnDfNoColumnNames.show(5, False)

    # Create dataframe object from rdd using RDD.toDF() method - with column names
    print('\n***************** Convert RDD to DataFrame using toDF() - with column names')
    txnDfWithColumnNames = txn_fct_rdd.toDF(
        ['txn_id', 'create_time', 'amount', 'cust_id', 'status', 'merchant_id', 'create_time_ist'])

    # Print schema
    print('\n***************** txnDfWithColumnNames.printSchema()')
    txnDfWithColumnNames.printSchema()

    # Show sample (5) records
    print('\n***************** Print first 5 records - txnDfWithColumnNames.show(5)')
    txnDfWithColumnNames.show(5, truncate=False)

    # Create DataFrame using sparkSession
    print('\n***************** Convert RDD to DataFrame using '
          'sparkSession.createDataFrame(txn_fct_rdd) - without column names')
    txnDf_1 = sparkSession.createDataFrame(txn_fct_rdd)

    # print schema
    print('\n*************** txnDf_1.printSchema()')
    txnDf_1.printSchema()

    # Print first 5 records
    print('\n*************** ')
    txnDf_1.show(5, False)

# Command
#   spark-submit --master 'local[*]' dataframe/ingestion/rdd_to_dataframe/rdd_to_df_through_schema_autoinfer.py

# Result
#   **************** Resource : /home/viveksingh/project-data/sidharth/data/txn_fct.csv
#
# ***************** Raw data snippet :
# ['txn_id|create_time|amount|cust_id|status|merchant_id|create_time_ist', '3509928476|20190101|292|7155966814|0|10492|2019-01-01 00:15:25', '3509960732|20190101|199|7155969502|0|259|2019-01-01 01:15:41', '3509992988|20190101|885.9|7155972190|0|1429|2019-01-01 04:07:17', '3510025244|20190101|119|7155974878|0|262|2019-01-01 07:06:56']
#
# ***************** Remove header : record.find('txn_id|create_time|')
# ['3509928476|20190101|292|7155966814|0|10492|2019-01-01 00:15:25', '3509960732|20190101|199|7155969502|0|259|2019-01-01 01:15:41', '3509992988|20190101|885.9|7155972190|0|1429|2019-01-01 04:07:17', '3510025244|20190101|119|7155974878|0|262|2019-01-01 07:06:56', '3510057500|20190101|169|7155977566|1|259|2019-01-01 07:56:02']
#
# ***************** Final transformed RDD
# (3509928476, '20190101', 292.0, '7155966814', '0', '10492', '2019-01-01 00:15:25')
# (3509960732, '20190101', 199.0, '7155969502', '0', '259', '2019-01-01 01:15:41')
# (3509992988, '20190101', 885.9, '7155972190', '0', '1429', '2019-01-01 04:07:17')
# (3510025244, '20190101', 119.0, '7155974878', '0', '262', '2019-01-01 07:06:56')
# (3510057500, '20190101', 169.0, '7155977566', '1', '259', '2019-01-01 07:56:02')
#
# ***************** Convert RDD to DataFrame using toDF() - without column names
#
# ***************** txnDfNoColumnNames.printSchema()
# root
#  |-- _1: long (nullable = true)
#  |-- _2: string (nullable = true)
#  |-- _3: double (nullable = true)
#  |-- _4: string (nullable = true)
#  |-- _5: string (nullable = true)
#  |-- _6: string (nullable = true)
#  |-- _7: string (nullable = true)
#
#
# ***************** txnDfNoColumnNames.show(5, False)
# +----------+--------+-----+----------+---+-----+-------------------+
# |_1        |_2      |_3   |_4        |_5 |_6   |_7                 |
# +----------+--------+-----+----------+---+-----+-------------------+
# |3509928476|20190101|292.0|7155966814|0  |10492|2019-01-01 00:15:25|
# |3509960732|20190101|199.0|7155969502|0  |259  |2019-01-01 01:15:41|
# |3509992988|20190101|885.9|7155972190|0  |1429 |2019-01-01 04:07:17|
# |3510025244|20190101|119.0|7155974878|0  |262  |2019-01-01 07:06:56|
# |3510057500|20190101|169.0|7155977566|1  |259  |2019-01-01 07:56:02|
# +----------+--------+-----+----------+---+-----+-------------------+
# only showing top 5 rows
#

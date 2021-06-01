"""
This program demonstrates how to create DataFrame from RDD without Schema definition.
"""


from pyspark.sql import SparkSession
import constants.app_constants as appConstants


if __name__ == '__main__':

    sparkSession = SparkSession \
        .builder \
        .appName('rdd-to-dataframe') \
        .master('local[*]') \
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

    print('\n***************** Convert RDD to DataFrame using toDF() - without column names')
    txnDfNoColumnNames = txn_fct_rdd.toDF()

    # Print schema
    txnDfNoColumnNames.printSchema()

    # Show 5 records without truncating
    txnDfNoColumnNames.show(5, False)

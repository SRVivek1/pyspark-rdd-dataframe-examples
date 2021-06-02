"""
This program demonstrates the how to validate the schema when creating DataFrame objects.
"""


from pyspark.sql import SparkSession, Row
import constants.app_constants as appConstants

if __name__ == '__main__':
    sparkSession = SparkSession \
        .builder \
        .appName('rdd-to-df-schema-validation') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel('ERROR')

    txn_fct_rdd = sparkContext.textFile(appConstants.txn_fct_csv_file) \
        .filter(lambda rec: rec.find('txn_id')) \
        .map(lambda rec: rec.split("|")) \
        .map(lambda rec: Row(int(rec[0]), int(rec[1]), float(rec[2]), int(rec[3]), int(rec[4]), int(rec[5]), str(rec[6])))
        # RDD[Row(int, int, float, int, int, int, str)]

    print('\n*************** RDD data read from file')
    txn_fct_rdd.foreach(print)

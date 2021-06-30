"""
This program demonstrates the use of Windows functions in spark.
"""


from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_date, from_unixtime, unix_timestamp, avg
from constants.app_constants import file_read_path, finances_small_parquet


if __name__ == '__main__':
    """
    Driver program
    """

    sparkSession = SparkSession\
        .builder\
        .appName('window-function-demo')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    print('\n*************************** Spark Window Function ***************************\n')

    # Read data from parquet file
    finance_small_df = sparkSession.read.parquet(file_read_path + finances_small_parquet)

    print('\n**************** finance_small_df.printSchema()')
    finance_small_df.printSchema()

    print('\n**************** finance_small_df.show()')
    finance_small_df.show()

    # define window specification
    accNumPrev4WindowSpec = Window\
        .partitionBy('AccountNumber')\
        .orderBy('Date')\
        .rowsBetween(-4, 0)

    finance_small_df\
        .withColumn('Date', to_date(from_unixtime(unix_timestamp('Date', 'MM/dd/yyyy'))))\
        .withColumn('RollingAvg', avg("Amount").over(accNumPrev4WindowSpec))\
        .show(20, False)

"""
This program demonstrates the use of Windows functions in spark.
"""


from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
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

    # To support legacy date format in prev. version of parquet files
    sparkSession.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')

    print('\n*************************** Spark Window Function ***************************\n')

    # Read data from parquet file
    finance_small_df = sparkSession.read.parquet(file_read_path + finances_small_parquet)

    print('\n**************** finance_small_df.printSchema()')
    finance_small_df.printSchema()

    print('\n**************** finance_small_df.show()')
    finance_small_df.show()

    # Start : Find average of amount for last 4 rows
    # define window specification
    # This window spec. will work on current and previous 4 rows
    # If there are less than 4 rows than it will consider only available ones
    # If there are more then 4 rows then it will only consider last 4 rows
    print("\n************** Window Specification : accNumPrev4WindowSpec = "
          "Window.partitionBy('AccountNumber').orderBy('Date').rowsBetween(-4, 0)")
    accNumPrev4WindowSpec = Window\
        .partitionBy('AccountNumber')\
        .orderBy('Date')\
        .rowsBetween(-4, 0)

    print("\n************* result_df = finance_small_df.withColumn('Date', "
          "to_date(from_unixtime(unix_timestamp('Date', 'MM/dd/yyyy'))))"
          ".withColumn('RollingAvg', avg('Amount').over(accNumPrev4WindowSpec))")
    result_df = finance_small_df\
        .withColumn('Date', to_date(from_unixtime(unix_timestamp('Date', 'MM/dd/yyyy'))))\
        .withColumn('RollingAvg', avg("Amount").over(accNumPrev4WindowSpec))

    print('\n*************** result_df.show(20, False)')
    result_df.show(20, False)
    # End : Find average of amount for last 4 rows

    # Start : Find sum of amount for all preceding rows
    print("\n************** Window Specification : sumOfAllPrecedingRecUbWindowSpec = "
          "Window.partitionBy('AccountNumber').orderBy(asc('Amount')).rowsBetween(Window.unboundedPreceding, 0)")
    sumOfAllPrecedingRecUbWindowSpec = Window\
        .partitionBy('AccountNumber')\
        .orderBy(asc('Date'))\
        .rowsBetween(Window.unboundedPreceding, 0)

    print("\n************** result_df = finance_small_df"
          ".withColumn('Date', to_date(from_unixtime(unix_timestamp('Date', 'MM/dd/yyyy'))))"
          ".withColumn('RollingAvg', avg('Amount').over(accNumPrev4WindowSpec))"
          ".withColumn('CommutativeSum', sum('Amount').over(sumOfAllPrecedingRecUbWindowSpec))")
    result_df = finance_small_df\
        .withColumn('Date', to_date(from_unixtime(unix_timestamp('Date', 'MM/dd/yyyy'))))\
        .withColumn('RollingAvg', avg('Amount').over(accNumPrev4WindowSpec))\
        .withColumn('CommutativeSum', sum('Amount').over(sumOfAllPrecedingRecUbWindowSpec))

    print('\n*************** result_df.show(20, False)')
    result_df.show(20, False)
    # End : Find sum of amount for all preceding rows


# Command
# ---------------------
#
#
# Output
# ---------------------
#

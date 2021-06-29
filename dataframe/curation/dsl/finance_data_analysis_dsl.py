"""
This program demonstrates different transformation examples on DataFrame instance.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import constants.app_constants as app_const

if __name__ == "__main__":
    sparkSession = SparkSession\
        .builder\
        .appName('Data curation using DSL')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    # Provide support to date format written by older spark
    sparkSession.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')

    print("\n***************************** Data curation using DSL *****************************\n")

    # File path of AWS S3
    file_path = app_const.file_read_path + app_const.finances_small_parquet
    print('\n************************ Data URL : ' + file_path)

    # Read data in Data frame
    finances_df = sparkSession.read.parquet(file_path)

    # Print schema and sample records
    finances_df.printSchema()
    print('\n******************* Sample records : finances_df.show(5, truncate=False)\n')
    finances_df.show(5, truncate=False)

    # Sort the data using 'Amount' column
    print('\n******************* Ascending order sorting : '
          'finances_df.orderBy(col(\'Amount\')).show(5, truncate=False)\n')
    finances_df.orderBy(col('Amount')).show(5, truncate=False)

    print('\n******************* Descending order sorting : '
          'finances_df.orderBy(col(\'Amount\'), ascending=False).show(5, truncate=False)\n')
    finances_df.orderBy(col('Amount'), ascending=False).show(5, truncate=False)

    # Fetch record as new Column by adding two are more columns together
    print('\n******************* finances_df.select(concat_ws(\' ~ \', \'AccountNumber\', \'Description\').alias(...))')
    finances_df.select(concat_ws(' ~ ', 'AccountNumber', 'Description')
                       .alias('AccountNumber ~ Description'))\
        .show(truncate=False)

    # Create a new DataFrame with new Column
    finances_df\
        .withColumn('AccountDetails', concat_ws(' ~ ', 'AccountNumber', 'Description'))\
        .show(5, truncate=False)

    # Group records
    print('\n******************* Create new DataFrame by applying Transformations (Aggregate functions)')
    agg_finance_df = finances_df\
        .groupBy('AccountNumber')\
        .agg(avg('Amount').alias('AverageTransactionAmount'),
             sum('Amount').alias('TotalTransactionAmount'),
             count('Amount').alias('NumberOfTransactions'),
             max('Amount').alias('MaxTransactionValue'),
             min('Amount').alias('MinTransactionValue'),
             collect_set('Description').alias('UniqueTransactionsDescriptions'),
             collect_list('Description').alias('AllTransactionsDescriptions'))

    agg_finance_df.show(5, False)

    agg_finance_df\
        .select('AccountNumber', 'UniqueTransactionsDescriptions',
                size('UniqueTransactionsDescriptions').alias('CountOfUniqueTransactionsTypes'),
                sort_array('UniqueTransactionsDescriptions', False).alias('OrderedUniqueTransactionDescriptions'),
                array_contains('UniqueTransactionsDescriptions', 'Movies').alias('WentToMovies'))\
        .show(5, False)

    # Extracting different data from date
    print('\n*************************** Transforming Dates ***************************')

    date_demo_df = finances_df.select('AccountNumber', 'Amount', 'Date')
    print("\n**************** finances_df.select('AccountNumber', 'Amount', 'Date').show()")
    date_demo_df.show()

    # Create new column from date for unix timestamp
    date_demo_df = date_demo_df\
        .withColumn('DateTS',
                    unix_timestamp(col('Date'), 'MM/dd/yyyy')
                    .cast('timestamp'))
    print("\n**************** date_demo_df"
          ".withColumn('DateTS',unix_timestamp(col('Date'), 'MM/dd/yyyy').cast('timestamp')).show()")
    date_demo_df.show()

    # Find year from the date
    date_demo_df = date_demo_df.withColumn('Year', year(col('DateTS')))
    date_demo_df = date_demo_df.withColumn('day_of_year', dayofyear('DateTS'))
    date_demo_df = date_demo_df.withColumn('date_format_yyyy', date_format(col('DateTS'), 'yyyy'))

    # Extract Month from the date
    date_demo_df = date_demo_df.withColumn('Month', month('DateTS'))
    date_demo_df = date_demo_df.withColumn('day_of_month', dayofmonth('DateTS'))
    date_demo_df = date_demo_df.withColumn('date_format_MM', date_format(col('DateTS'), 'MM'))

    # Extract day from the date
    date_demo_df = date_demo_df.withColumn('Day', dayofmonth('DateTS'))
    date_demo_df = date_demo_df.withColumn('date_format_dd', date_format(col('DateTS'), 'dd'))

    # Week number
    date_demo_df = date_demo_df.withColumn('day_of_week', dayofweek('DateTS'))

    # last day
    date_demo_df = date_demo_df.withColumn('last_day_of_month', last_day('DateTS'))

    # next day
    date_demo_df = date_demo_df.withColumn('next_day', next_day('DateTS', "Mon"))

    # Find quarter of the date
    date_demo_df = date_demo_df.withColumn('quarter', ((date_format('DateTS', "MM") + 2) / 3).cast('int'))

    print("\n**************** Different date transformations per column")
    date_demo_df.show()
# Command
# --------------------
# spark-submit dataframe/curation/dsl/finance_data_analysis_dsl.py
#
# Output
# ---------------------
# ***************************** Data curation using DSL *****************************
#
#
# ************************ Data URL : /home/viveksingh/project-data/sidharth/data/finances-small
# root
#  |-- AccountNumber: string (nullable = true)
#  |-- Amount: double (nullable = true)
#  |-- Date: string (nullable = true)
#  |-- Description: string (nullable = true)
#
#
# ******************* Sample records : finances_df.show(5, truncate=False)
#
# +-------------+------+---------+-------------+
# |AccountNumber|Amount|Date     |Description  |
# +-------------+------+---------+-------------+
# |123-ABC-789  |1.23  |1/1/2015 |Drug Store   |
# |456-DEF-456  |200.0 |1/3/2015 |Electronics  |
# |333-XYZ-999  |106.0 |1/4/2015 |Gas          |
# |123-ABC-789  |2.36  |1/9/2015 |Grocery Store|
# |456-DEF-456  |23.16 |1/11/2015|Unknown      |
# +-------------+------+---------+-------------+
# only showing top 5 rows
#
#
# ******************* Ascending order sorting : finances_df.orderBy(col('Amount')).show(5, truncate=False)
#
# +-------------+------+---------+-------------+
# |AccountNumber|Amount|Date     |Description  |
# +-------------+------+---------+-------------+
# |123-ABC-789  |0.0   |2/14/2015|Unknown      |
# |123-ABC-789  |1.23  |1/1/2015 |Drug Store   |
# |987-CBA-321  |2.29  |1/31/2015|Drug Store   |
# |123-ABC-789  |2.36  |1/9/2015 |Grocery Store|
# |456-DEF-456  |6.78  |2/23/2015|Drug Store   |
# +-------------+------+---------+-------------+
# only showing top 5 rows
#
#
# ******************* Descending order sorting : finances_df.orderBy(col('Amount'), ascending=False).show(5, truncate=False)
#
# +-------------+------+---------+-------------+
# |AccountNumber|Amount|Date     |Description  |
# +-------------+------+---------+-------------+
# |123-ABC-789  |4000.0|3/2/2015 |Electronics  |
# |456-DEF-456  |281.0 |3/9/2015 |Electronics  |
# |987-CBA-321  |267.93|3/11/2015|Grocery Store|
# |123-ABC-789  |254.87|3/4/2015 |Grocery Store|
# |333-XYZ-999  |241.8 |3/4/2015 |Electronics  |
# +-------------+------+---------+-------------+
# only showing top 5 rows
#
#
# ******************* finances_df.select(concat_ws(' ~ ', 'AccountNumber', 'Description').alias(...))
# +------------------------------------------------+
# |AccountNumber ~ Description                     |
# +------------------------------------------------+
# |123-ABC-789 ~ Drug Store                        |
# |456-DEF-456 ~ Electronics                       |
# |333-XYZ-999 ~ Gas                               |
# |123-ABC-789 ~ Grocery Store                     |
# |456-DEF-456 ~ Unknown                           |
# |123-ABC-789 ~ Park                              |
# |456-DEF-456 ~ Electronics                       |
# |333-XYZ-999 ~ Gas                               |
# |333-XYZ-999 ~ Some Totally Fake Long Description|
# |333-XYZ-999 ~ Gas                               |
# |987-CBA-321 ~ Grocery Store                     |
# |123-ABC-789 ~ Electronics                       |
# |456-DEF-456 ~ Grocery Store                     |
# |333-XYZ-999 ~ Movies                            |
# |456-DEF-456 ~ Grocery Store                     |
# |987-CBA-321 ~ Drug Store                        |
# |456-DEF-456 ~ Park                              |
# |456-DEF-456 ~ Books                             |
# |123-ABC-789 ~ Grocery Store                     |
# |333-XYZ-999 ~ Electronics                       |
# +------------------------------------------------+
# only showing top 20 rows
#
# +-------------+------+---------+-------------+---------------------------+
# |AccountNumber|Amount|Date     |Description  |AccountDetails             |
# +-------------+------+---------+-------------+---------------------------+
# |123-ABC-789  |1.23  |1/1/2015 |Drug Store   |123-ABC-789 ~ Drug Store   |
# |456-DEF-456  |200.0 |1/3/2015 |Electronics  |456-DEF-456 ~ Electronics  |
# |333-XYZ-999  |106.0 |1/4/2015 |Gas          |333-XYZ-999 ~ Gas          |
# |123-ABC-789  |2.36  |1/9/2015 |Grocery Store|123-ABC-789 ~ Grocery Store|
# |456-DEF-456  |23.16 |1/11/2015|Unknown      |456-DEF-456 ~ Unknown      |
# +-------------+------+---------+-------------+---------------------------+
# only showing top 5 rows
#
#
# ******************* Create new DataFrame by applying Transformations (Aggregate functions)
# +-------------+------------------------+----------------------+--------------------+-------------------+-------------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |AccountNumber|AverageTransactionAmount|TotalTransactionAmount|NumberOfTransactions|MaxTransactionValue|MinTransactionValue|UniqueTransactionsDescriptions                                                      |AllTransactionsDescriptions                                                                                                                                                   |
# +-------------+------------------------+----------------------+--------------------+-------------------+-------------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
# |456-DEF-456  |104.75142857142855      |1466.5199999999998    |14                  |281.0              |6.78               |[Electronics, Grocery Store, Books, Unknown, Park, Drug Store, Gas]                 |[Electronics, Unknown, Electronics, Grocery Store, Grocery Store, Park, Books, Drug Store, Electronics, Gas, Drug Store, Books, Electronics, Gas]                             |
# |333-XYZ-999  |104.09833333333334      |1249.18               |12                  |241.8              |41.67              |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|[Gas, Gas, Some Totally Fake Long Description, Gas, Movies, Electronics, Gas, Grocery Store, Gas, Books, Grocery Store, Electronics]                                          |
# |987-CBA-321  |96.87888888888887       |871.9099999999999     |9                   |267.93             |2.29               |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |[Grocery Store, Drug Store, Electronics, Park, Grocery Store, Gas, Movies, Books, Grocery Store]                                                                              |
# |123-ABC-789  |362.9785714285714       |5081.7                |14                  |4000.0             |0.0                |[Electronics, Grocery Store, Unknown, Park, Drug Store, Movies]                     |[Drug Store, Grocery Store, Park, Electronics, Grocery Store, Grocery Store, Grocery Store, Unknown, Movies, Grocery Store, Grocery Store, Movies, Electronics, Grocery Store]|
# +-------------+------------------------+----------------------+--------------------+-------------------+-------------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#
# +-------------+------------------------------------------------------------------------------------+------------------------------+------------------------------------------------------------------------------------+------------+
# |AccountNumber|UniqueTransactionsDescriptions                                                      |CountOfUniqueTransactionsTypes|OrderedUniqueTransactionDescriptions                                                |WentToMovies|
# +-------------+------------------------------------------------------------------------------------+------------------------------+------------------------------------------------------------------------------------+------------+
# |456-DEF-456  |[Electronics, Grocery Store, Books, Unknown, Park, Drug Store, Gas]                 |7                             |[Unknown, Park, Grocery Store, Gas, Electronics, Drug Store, Books]                 |false       |
# |333-XYZ-999  |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|6                             |[Some Totally Fake Long Description, Movies, Grocery Store, Gas, Electronics, Books]|true        |
# |987-CBA-321  |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |7                             |[Park, Movies, Grocery Store, Gas, Electronics, Drug Store, Books]                  |true        |
# |123-ABC-789  |[Electronics, Grocery Store, Unknown, Park, Drug Store, Movies]                     |6                             |[Unknown, Park, Movies, Grocery Store, Electronics, Drug Store]                     |true        |
# +-------------+------------------------------------------------------------------------------------+------------------------------+------------------------------------------------------------------------------------+------------+

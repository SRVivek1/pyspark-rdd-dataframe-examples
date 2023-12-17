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

    # using desc('column') API
    # finances_df.orderBy(desc('Amount'), ascending=False).show(5, truncate=False)

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
    date_demo_df.show(51, truncate=False)

# Command
# --------------------
# spark-submit --master yarn dataframe/curation/dsl/finance_data_analysis_dsl.py
#
# Output
# ---------------------
#
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
#
#
# *************************** Transforming Dates ***************************
#
# **************** finances_df.select('AccountNumber', 'Amount', 'Date').show()
# +-------------+------+---------+
# |AccountNumber|Amount|     Date|
# +-------------+------+---------+
# |  123-ABC-789|  1.23| 1/1/2015|
# |  456-DEF-456| 200.0| 1/3/2015|
# |  333-XYZ-999| 106.0| 1/4/2015|
# |  123-ABC-789|  2.36| 1/9/2015|
# |  456-DEF-456| 23.16|1/11/2015|
# |  123-ABC-789| 42.12|1/12/2015|
# |  456-DEF-456|  20.0|1/12/2015|
# |  333-XYZ-999| 52.13|1/17/2015|
# |  333-XYZ-999| 41.67|1/19/2015|
# |  333-XYZ-999| 56.37|1/21/2015|
# |  987-CBA-321| 63.84|1/23/2015|
# |  123-ABC-789|160.91|1/24/2015|
# |  456-DEF-456| 78.77|1/24/2015|
# |  333-XYZ-999| 86.24|1/29/2015|
# |  456-DEF-456| 93.71|1/31/2015|
# |  987-CBA-321|  2.29|1/31/2015|
# |  456-DEF-456|108.64|1/31/2015|
# |  456-DEF-456|116.11|1/31/2015|
# |  123-ABC-789| 27.19|2/10/2015|
# |  333-XYZ-999|131.04|2/11/2015|
# +-------------+------+---------+
# only showing top 20 rows
#
#
# **************** date_demo_df.withColumn('DateTS',unix_timestamp(col('Date'), 'MM/dd/yyyy').cast('timestamp')).show()
# +-------------+------+---------+-------------------+
# |AccountNumber|Amount|     Date|             DateTS|
# +-------------+------+---------+-------------------+
# |  123-ABC-789|  1.23| 1/1/2015|2015-01-01 00:00:00|
# |  456-DEF-456| 200.0| 1/3/2015|2015-01-03 00:00:00|
# |  333-XYZ-999| 106.0| 1/4/2015|2015-01-04 00:00:00|
# |  123-ABC-789|  2.36| 1/9/2015|2015-01-09 00:00:00|
# |  456-DEF-456| 23.16|1/11/2015|2015-01-11 00:00:00|
# |  123-ABC-789| 42.12|1/12/2015|2015-01-12 00:00:00|
# |  456-DEF-456|  20.0|1/12/2015|2015-01-12 00:00:00|
# |  333-XYZ-999| 52.13|1/17/2015|2015-01-17 00:00:00|
# |  333-XYZ-999| 41.67|1/19/2015|2015-01-19 00:00:00|
# |  333-XYZ-999| 56.37|1/21/2015|2015-01-21 00:00:00|
# |  987-CBA-321| 63.84|1/23/2015|2015-01-23 00:00:00|
# |  123-ABC-789|160.91|1/24/2015|2015-01-24 00:00:00|
# |  456-DEF-456| 78.77|1/24/2015|2015-01-24 00:00:00|
# |  333-XYZ-999| 86.24|1/29/2015|2015-01-29 00:00:00|
# |  456-DEF-456| 93.71|1/31/2015|2015-01-31 00:00:00|
# |  987-CBA-321|  2.29|1/31/2015|2015-01-31 00:00:00|
# |  456-DEF-456|108.64|1/31/2015|2015-01-31 00:00:00|
# |  456-DEF-456|116.11|1/31/2015|2015-01-31 00:00:00|
# |  123-ABC-789| 27.19|2/10/2015|2015-02-10 00:00:00|
# |  333-XYZ-999|131.04|2/11/2015|2015-02-11 00:00:00|
# +-------------+------+---------+-------------------+
# only showing top 20 rows
#
#
# **************** Different date transformations per column
# +-------------+------+---------+-------------------+----+-----------+----------------+-----+------------+--------------+---+--------------+-----------+-----------------+----------+-------+
# |AccountNumber|Amount|Date     |DateTS             |Year|day_of_year|date_format_yyyy|Month|day_of_month|date_format_MM|Day|date_format_dd|day_of_week|last_day_of_month|next_day  |quarter|
# +-------------+------+---------+-------------------+----+-----------+----------------+-----+------------+--------------+---+--------------+-----------+-----------------+----------+-------+
# |123-ABC-789  |1.23  |1/1/2015 |2015-01-01 00:00:00|2015|1          |2015            |1    |1           |01            |1  |01            |5          |2015-01-31       |2015-01-05|1      |
# |456-DEF-456  |200.0 |1/3/2015 |2015-01-03 00:00:00|2015|3          |2015            |1    |3           |01            |3  |03            |7          |2015-01-31       |2015-01-05|1      |
# |333-XYZ-999  |106.0 |1/4/2015 |2015-01-04 00:00:00|2015|4          |2015            |1    |4           |01            |4  |04            |1          |2015-01-31       |2015-01-05|1      |
# |123-ABC-789  |2.36  |1/9/2015 |2015-01-09 00:00:00|2015|9          |2015            |1    |9           |01            |9  |09            |6          |2015-01-31       |2015-01-12|1      |
# |456-DEF-456  |23.16 |1/11/2015|2015-01-11 00:00:00|2015|11         |2015            |1    |11          |01            |11 |11            |1          |2015-01-31       |2015-01-12|1      |
# |123-ABC-789  |42.12 |1/12/2015|2015-01-12 00:00:00|2015|12         |2015            |1    |12          |01            |12 |12            |2          |2015-01-31       |2015-01-19|1      |
# |456-DEF-456  |20.0  |1/12/2015|2015-01-12 00:00:00|2015|12         |2015            |1    |12          |01            |12 |12            |2          |2015-01-31       |2015-01-19|1      |
# |333-XYZ-999  |52.13 |1/17/2015|2015-01-17 00:00:00|2015|17         |2015            |1    |17          |01            |17 |17            |7          |2015-01-31       |2015-01-19|1      |
# |333-XYZ-999  |41.67 |1/19/2015|2015-01-19 00:00:00|2015|19         |2015            |1    |19          |01            |19 |19            |2          |2015-01-31       |2015-01-26|1      |
# |333-XYZ-999  |56.37 |1/21/2015|2015-01-21 00:00:00|2015|21         |2015            |1    |21          |01            |21 |21            |4          |2015-01-31       |2015-01-26|1      |
# |987-CBA-321  |63.84 |1/23/2015|2015-01-23 00:00:00|2015|23         |2015            |1    |23          |01            |23 |23            |6          |2015-01-31       |2015-01-26|1      |
# |123-ABC-789  |160.91|1/24/2015|2015-01-24 00:00:00|2015|24         |2015            |1    |24          |01            |24 |24            |7          |2015-01-31       |2015-01-26|1      |
# |456-DEF-456  |78.77 |1/24/2015|2015-01-24 00:00:00|2015|24         |2015            |1    |24          |01            |24 |24            |7          |2015-01-31       |2015-01-26|1      |
# |333-XYZ-999  |86.24 |1/29/2015|2015-01-29 00:00:00|2015|29         |2015            |1    |29          |01            |29 |29            |5          |2015-01-31       |2015-02-02|1      |
# |456-DEF-456  |93.71 |1/31/2015|2015-01-31 00:00:00|2015|31         |2015            |1    |31          |01            |31 |31            |7          |2015-01-31       |2015-02-02|1      |
# |987-CBA-321  |2.29  |1/31/2015|2015-01-31 00:00:00|2015|31         |2015            |1    |31          |01            |31 |31            |7          |2015-01-31       |2015-02-02|1      |
# |456-DEF-456  |108.64|1/31/2015|2015-01-31 00:00:00|2015|31         |2015            |1    |31          |01            |31 |31            |7          |2015-01-31       |2015-02-02|1      |
# |456-DEF-456  |116.11|1/31/2015|2015-01-31 00:00:00|2015|31         |2015            |1    |31          |01            |31 |31            |7          |2015-01-31       |2015-02-02|1      |
# |123-ABC-789  |27.19 |2/10/2015|2015-02-10 00:00:00|2015|41         |2015            |2    |10          |02            |10 |10            |3          |2015-02-28       |2015-02-16|1      |
# |333-XYZ-999  |131.04|2/11/2015|2015-02-11 00:00:00|2015|42         |2015            |2    |11          |02            |11 |11            |4          |2015-02-28       |2015-02-16|1      |
# |456-DEF-456  |18.99 |2/12/2015|2015-02-12 00:00:00|2015|43         |2015            |2    |12          |02            |12 |12            |5          |2015-02-28       |2015-02-16|1      |
# |123-ABC-789  |145.98|2/13/2015|2015-02-13 00:00:00|2015|44         |2015            |2    |13          |02            |13 |13            |6          |2015-02-28       |2015-02-16|1      |
# |123-ABC-789  |153.44|2/14/2015|2015-02-14 00:00:00|2015|45         |2015            |2    |14          |02            |14 |14            |7          |2015-02-28       |2015-02-16|1      |
# |123-ABC-789  |0.0   |2/14/2015|2015-02-14 00:00:00|2015|45         |2015            |2    |14          |02            |14 |14            |7          |2015-02-28       |2015-02-16|1      |
# |987-CBA-321  |168.38|2/14/2015|2015-02-14 00:00:00|2015|45         |2015            |2    |14          |02            |14 |14            |7          |2015-02-28       |2015-02-16|1      |
# |333-XYZ-999  |175.84|2/14/2015|2015-02-14 00:00:00|2015|45         |2015            |2    |14          |02            |14 |14            |7          |2015-02-28       |2015-02-16|1      |
# |333-XYZ-999  |43.12 |2/18/2015|2015-02-18 00:00:00|2015|49         |2015            |2    |18          |02            |18 |18            |4          |2015-02-28       |2015-02-23|1      |
# |987-CBA-321  |101.18|2/19/2015|2015-02-19 00:00:00|2015|50         |2015            |2    |19          |02            |19 |19            |5          |2015-02-28       |2015-02-23|1      |
# |456-DEF-456  |215.67|2/20/2015|2015-02-20 00:00:00|2015|51         |2015            |2    |20          |02            |20 |20            |6          |2015-02-28       |2015-02-23|1      |
# |123-ABC-789  |9.17  |2/20/2015|2015-02-20 00:00:00|2015|51         |2015            |2    |20          |02            |20 |20            |6          |2015-02-28       |2015-02-23|1      |
# |456-DEF-456  |71.31 |2/22/2015|2015-02-22 00:00:00|2015|53         |2015            |2    |22          |02            |22 |22            |1          |2015-02-28       |2015-02-23|1      |
# |456-DEF-456  |6.78  |2/23/2015|2015-02-23 00:00:00|2015|54         |2015            |2    |23          |02            |23 |23            |2          |2015-02-28       |2015-03-02|1      |
# |123-ABC-789  |138.51|2/24/2015|2015-02-24 00:00:00|2015|55         |2015            |2    |24          |02            |24 |24            |3          |2015-02-28       |2015-03-02|1      |
# |333-XYZ-999  |43.12 |2/26/2015|2015-02-26 00:00:00|2015|57         |2015            |2    |26          |02            |26 |26            |5          |2015-02-28       |2015-03-02|1      |
# |123-ABC-789  |123.58|2/26/2015|2015-02-26 00:00:00|2015|57         |2015            |2    |26          |02            |26 |26            |5          |2015-02-28       |2015-03-02|1      |
# |987-CBA-321  |150.33|2/27/2015|2015-02-27 00:00:00|2015|58         |2015            |2    |27          |02            |27 |27            |6          |2015-02-28       |2015-03-02|1      |
# |987-CBA-321  |63.12 |2/28/2015|2015-02-28 00:00:00|2015|59         |2015            |2    |28          |02            |28 |28            |7          |2015-02-28       |2015-03-02|1      |
# |123-ABC-789  |22.34 |3/1/2015 |2015-03-01 00:00:00|2015|60         |2015            |3    |1           |03            |1  |01            |1          |2015-03-31       |2015-03-02|1      |
# |456-DEF-456  |189.53|3/2/2015 |2015-03-02 00:00:00|2015|61         |2015            |3    |2           |03            |2  |02            |2          |2015-03-31       |2015-03-09|1      |
# |123-ABC-789  |4000.0|3/2/2015 |2015-03-02 00:00:00|2015|61         |2015            |3    |2           |03            |2  |02            |2          |2015-03-31       |2015-03-09|1      |
# |333-XYZ-999  |43.12 |3/2/2015 |2015-03-02 00:00:00|2015|61         |2015            |3    |2           |03            |2  |02            |2          |2015-03-31       |2015-03-09|1      |
# |333-XYZ-999  |228.73|3/3/2015 |2015-03-03 00:00:00|2015|62         |2015            |3    |3           |03            |3  |03            |3          |2015-03-31       |2015-03-09|1      |
# |333-XYZ-999  |241.8 |3/4/2015 |2015-03-04 00:00:00|2015|63         |2015            |3    |4           |03            |4  |04            |4          |2015-03-31       |2015-03-09|1      |
# |123-ABC-789  |254.87|3/4/2015 |2015-03-04 00:00:00|2015|63         |2015            |3    |4           |03            |4  |04            |4          |2015-03-31       |2015-03-09|1      |
# |987-CBA-321  |12.42 |3/4/2015 |2015-03-04 00:00:00|2015|63         |2015            |3    |4           |03            |4  |04            |4          |2015-03-31       |2015-03-09|1      |
# |456-DEF-456  |281.0 |3/9/2015 |2015-03-09 00:00:00|2015|68         |2015            |3    |9           |03            |9  |09            |2          |2015-03-31       |2015-03-16|1      |
# |987-CBA-321  |42.42 |3/10/2015|2015-03-10 00:00:00|2015|69         |2015            |3    |10          |03            |10 |10            |3          |2015-03-31       |2015-03-16|1      |
# |987-CBA-321  |267.93|3/11/2015|2015-03-11 00:00:00|2015|70         |2015            |3    |11          |03            |11 |11            |4          |2015-03-31       |2015-03-16|1      |
# |456-DEF-456  |42.85 |3/12/2015|2015-03-12 00:00:00|2015|71         |2015            |3    |12          |03            |12 |12            |5          |2015-03-31       |2015-03-16|1      |
# +-------------+------+---------+-------------------+----+-----------+----------------+-----+------------+--------------+---+--------------+-----------+-----------------+----------+-------+

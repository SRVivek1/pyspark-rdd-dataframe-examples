"""
This program demonstrates data curation in spark SQL dataframe object using SQL query.
"""


from pyspark.sql import SparkSession
import constants.app_constants as app_const

if __name__ == '__main__':
    """
    Driver program.
    """

    sparkSession = SparkSession\
        .builder\
        .appName('data-curation-using-sql')\
        .master('local[*]')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    print('\n***************** Data curation using SQL *****************\n')

    # Setup data source
    finances_small_parquet = app_const.file_read_path + app_const.finances_small_parquet
    print('\n************** Source file : ' + finances_small_parquet)

    # Load data
    # Sample query -  "SELECT * FROM parquet.`/home/viveksingh/project-data/sidharth/data/finances-small`"
    sql_query = "select * from parquet.`{}`".format(finances_small_parquet)
    print('\n********************** SQL Query to load data : ' + sql_query)
    finances_small_df = sparkSession.sql(sql_query)

    # print schema and sample data
    print('\n********************** Schema of data : ')
    finances_small_df.printSchema()

    print('\n********************** Sample data : ')
    finances_small_df.show(5, False)

    # Create a temp table/view to perform query
    finances_small_df.createOrReplaceTempView('finances')

    # Query data in table/view using provided name
    print('\n********************** sparkSession.sql(\'select * from finances order by amount\').show(5, False)')
    sparkSession\
        .sql('select * from finances order by amount')\
        .show(5, False)

    # SQL query to concatenate 2 columns
    sql_query_transform_1 = 'select concat_ws(\' - \', AccountNumber, Description) as AccountDetails from finances'
    print('\n********************** sql_query_transform_1 : ' + sql_query_transform_1)
    print('\n********************** sparkSession.sql(sql_query_transform_1)')
    sparkSession\
        .sql(sql_query_transform_1)\
        .show(5, False)

    # Group by function using SQL and DSL functions
    sql_query_transform_2 = """
        select
            AccountNumber,
            sum(Amount) as TotalTransaction,
            count(Amount) as NumberOfTransaction,
            max(Amount) as MaxTransaction,
            min(Amount) as MinTransaction,
            collect_set(Description) as UniqueTransactionDescriptions
        from
            finances
        group by
            AccountNumber
    """
    print('\n*************** Aggregation using SQL - sql_query_transform_2 : ' + sql_query_transform_2)

    print('\n*************** sparkSession.sql(sql_query_transform_2).show(5, False) : ')
    agg_finance_df = sparkSession.sql(sql_query_transform_2)
    agg_finance_df.show(5, False)

    # Create view of new dataframe with aggregated data
    agg_finance_df.createOrReplaceTempView('agg_finances')

    sql_query_transform_3 = """
        select
            AccountNumber,
            UniqueTransactionDescriptions,
            sort_array(UniqueTransactionDescriptions, false) as OrderedUniqueTransactionDescriptions,
            size(UniqueTransactionDescriptions) as CountOfUniqueTransactionTypes,
            array_contains(UniqueTransactionDescriptions, 'Movies') as WentToMovies
        from
            agg_finances
    """
    print('\n***************** Array function using SQL : ' + sql_query_transform_3)
    print('\n***************** sparkSession.sql(sql_query_transform_3).show(5, False)')
    sparkSession.sql(sql_query_transform_3).show(5, False)

    # Remove temp table/view
    sparkSession.catalog.dropTempView('finances')

# Command
# -----------------
# spark-submit dataframe/curation/sql/finance_data_analysis_sql.py
#
# Output
# -----------------
# ***************** Data curation using SQL *****************
#
#
# ************** Source file : /home/viveksingh/project-data/sidharth/data/finances-small
#
# ********************** SQL Query to load data : select * from parquet.`/home/viveksingh/project-data/sidharth/data/finances-small`
#
# ********************** Schema of data :
# root
#  |-- AccountNumber: string (nullable = true)
#  |-- Amount: double (nullable = true)
#  |-- Date: string (nullable = true)
#  |-- Description: string (nullable = true)
#
#
# ********************** Sample data :
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
# ********************** sparkSession.sql('select * from finances order by amount').show(5, False)
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
# ********************** sql_query_transform_1 : select concat_ws(' - ', AccountNumber, Description) as AccountDetails from finances
#
# ********************** sparkSession.sql(sql_query_transform_1)
# +---------------------------+
# |AccountDetails             |
# +---------------------------+
# |123-ABC-789 - Drug Store   |
# |456-DEF-456 - Electronics  |
# |333-XYZ-999 - Gas          |
# |123-ABC-789 - Grocery Store|
# |456-DEF-456 - Unknown      |
# +---------------------------+
# only showing top 5 rows
#
#
# *************** Aggregation using SQL - sql_query_transform_2 :
#         select
#             AccountNumber,
#             sum(Amount) as TotalTransaction,
#             count(Amount) as NumberOfTransaction,
#             max(Amount) as MaxTransaction,
#             min(Amount) as MinTransaction,
#             collect_set(Description) as UniqueTransactionDescriptions
#         from
#             finances
#         group by
#             AccountNumber
#
#
# *************** sparkSession.sql(sql_query_transform_2).show(5, False) :
# +-------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+
# |AccountNumber|TotalTransaction  |NumberOfTransaction|MaxTransaction|MinTransaction|UniqueTransactionDescriptions                                                       |
# +-------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+
# |456-DEF-456  |1466.5199999999998|14                 |281.0         |6.78          |[Electronics, Grocery Store, Books, Unknown, Park, Drug Store, Gas]                 |
# |333-XYZ-999  |1249.18           |12                 |241.8         |41.67         |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|
# |987-CBA-321  |871.9099999999999 |9                  |267.93        |2.29          |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |
# |123-ABC-789  |5081.7            |14                 |4000.0        |0.0           |[Electronics, Grocery Store, Unknown, Park, Drug Store, Movies]                     |
# +-------------+------------------+-------------------+--------------+--------------+------------------------------------------------------------------------------------+
#
#
# ***************** Array function using SQL :
#         select
#             AccountNumber,
#             UniqueTransactionDescriptions,
#             sort_array(UniqueTransactionDescriptions, false) as OrderedUniqueTransactionDescriptions,
#             size(UniqueTransactionDescriptions) as CountOfUniqueTransactionTypes,
#             array_contains(UniqueTransactionDescriptions, 'Movies') as WentToMovies
#         from
#             agg_finances
#
#
# ***************** sparkSession.sql(sql_query_transform_3).show(5, False)
# +-------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+-----------------------------+------------+
# |AccountNumber|UniqueTransactionDescriptions                                                       |OrderedUniqueTransactionDescriptions                                                |CountOfUniqueTransactionTypes|WentToMovies|
# +-------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+-----------------------------+------------+
# |456-DEF-456  |[Electronics, Grocery Store, Books, Unknown, Park, Drug Store, Gas]                 |[Unknown, Park, Grocery Store, Gas, Electronics, Drug Store, Books]                 |7                            |false       |
# |333-XYZ-999  |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|[Some Totally Fake Long Description, Movies, Grocery Store, Gas, Electronics, Books]|6                            |true        |
# |987-CBA-321  |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |[Park, Movies, Grocery Store, Gas, Electronics, Drug Store, Books]                  |7                            |true        |
# |123-ABC-789  |[Electronics, Grocery Store, Unknown, Park, Drug Store, Movies]                     |[Unknown, Park, Movies, Grocery Store, Electronics, Drug Store]                     |6                            |true        |
# +-------------+------------------------------------------------------------------------------------+------------------------------------------------------------------------------------+-----------------------------+------------+

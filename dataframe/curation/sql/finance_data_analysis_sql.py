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
    sql_query = "select * from parquet. '{}'".format(finances_small_parquet)
    print('\n********************** SQL Query to load data : ' + sql_query)
    finances_small_df = sparkSession.sql(sql_query)

    # print schema and sample data
    print('\n********************** Schema of data : ')
    finances_small_df.printSchema()

    print('\n********************** Sample data : ')
    finances_small_df.show(5, False)




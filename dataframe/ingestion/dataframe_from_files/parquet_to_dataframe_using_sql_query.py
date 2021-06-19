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

    print('\n***************** Data ingestion using SQL *****************\n')

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

# Command
# ----------------------
# spark-submit dataframe/ingestion/dataframe_from_files/parquet_to_dataframe_using_sql_query.py
#
# Output
# ----------------------
# ***************** Data ingestion using SQL *****************
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

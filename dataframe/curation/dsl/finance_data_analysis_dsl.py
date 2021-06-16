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

    print("\n***************************** Data curation using DSL *****************************\n")

    # File path of AWS S3
    file_path = app_const.file_read_path + app_const.finances_small_parquet
    print('\n************************ S3 URL : ' + file_path)

    # Read data in Data frame
    finances_df = sparkSession.read.parquet(file_path)

    # Print schema and sample records
    finances_df.printSchema()
    print('\n******************* Sample records : finances_df.show(5, truncate=False)\n')
    finances_df.show(5, truncate=False)

    # Sort the data using 'Amount' column
    print('\n******************* Sample records : finances_df.orderBy(col(\'Amount\')).show(5, truncate=False)\n')
    finances_df.orderBy(col('Amount')).show(5, truncate=False)

# Command
# --------------------
# spark-submit
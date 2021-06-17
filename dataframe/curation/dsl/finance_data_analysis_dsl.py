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
                size('UniqueTransactionsDescriptions').alias('CountOfUniqueTransactionsTypes'))\
        .show(5, False)


# Command
# --------------------
# spark-submit dataframe/curation/dsl/finance_data_analysis_dsl.py
#
# Output
# ---------------------
#

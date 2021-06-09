"""
This program demonstrate how we can read CSV data into DataFrame.
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, DoubleType
from constants import app_constants as appConstrants

if __name__ == '__main__':
    print('\nRead CSV file into DataFrame')

    sparkSession = SparkSession \
        .builder \
        .appName('csv-to-dataframe') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel('ERROR')

    finances_csv_schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('has_debt', BooleanType(), True),
        StructField('has_financial_dependents', BooleanType(), True),
        StructField('has_student_loans', BooleanType(), True),
        StructField('income', DoubleType(), True)
    ])

    # Create DataFrame using SparkSession.read.load().
    print('\n************ Create DataFrame using SparkSession.read.load() .')
    finances_df = sparkSession.read \
        .option('header', 'false') \
        .option('delimiter', ',') \
        .format('csv') \
        .schema(finances_csv_schema) \
        .load(appConstrants.finances_csv_file)

    print('\nFinances DataFrame Schema : finances_df.printSchema()')
    finances_df.printSchema()

    print('\n************ : finances_df.show()')
    finances_df.show()

    # Read same file using SparkSession.read.csv() method
    print('\n************ : ')
    finances_df = sparkSession.read \
        .option('header', True) \
        .option('delimiter', ',') \
        .schema(finances_csv_schema) \
        .csv(appConstrants.finances_csv_file)

    print('\nFinances DataFrame Schema : finances_df.printSchema()')
    finances_df.printSchema()

    print('\n************ : finances_df.show()')
    finances_df.show()

    # To get the DataFrame with new column names
    finances_df = finances_df \
        .toDF('id', 'has_debt_', 'has_financial_dependents', 'has_student_loan', 'income_')

    print('\n************ : finances_df.show()')
    finances_df.show()

    print('\n************ : # of partitions - ' + str(finances_df.rdd.getNumPartitions()))
    # Repartition and save data to file system

# Command
# --------------------
# spark-submit dataframe/ingestion/dataframe_from_files/csv_to_dataframe.py
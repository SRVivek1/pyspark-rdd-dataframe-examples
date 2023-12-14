"""
This program demonstrate how we can read CSV data into DataFrame.
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, DoubleType
from constants import app_constants as app_const

if __name__ == '__main__':
    print('\nRead CSV file into DataFrame')

    spark = SparkSession \
        .builder \
        .appName('csv-to-dataframe') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    finances_csv_schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('has_debt', BooleanType(), True),
        StructField('has_financial_dependents', BooleanType(), True),
        StructField('has_student_loans', BooleanType(), True),
        StructField('income', DoubleType(), True)
    ])

    # Create DataFrame using SparkSession.read.load().
    print('\n************ Create DataFrame using SparkSession.read.load() .')
    finances_df = spark.read \
        .option('header', 'false') \
        .option('delimiter', ',') \
        .format('csv') \
        .schema(finances_csv_schema) \
        .load(app_const.finances_csv_file)

    print('\nFinances DataFrame Schema : finances_df.printSchema()')
    finances_df.printSchema()

    print('\n************ : finances_df.show()')
    finances_df.show()

    # Read same file using SparkSession.read.csv() method
    print('\n************ : SparkSession.read.csv()')
    finances_df = spark.read \
        .option('header', 'false') \
        .option('delimiter', ',') \
        .schema(finances_csv_schema) \
        .csv(app_const.finances_csv_file)

    print('\nFinances DataFrame Schema : finances_df.printSchema()')
    finances_df.printSchema()

    print('\n************ : finances_df.show()')
    finances_df.show()

    # To get the DataFrame with new column names
    finances_df = finances_df \
        .toDF('id', 'has_debt_', 'has_financial_dependents_', 'has_student_loan_', 'income_')

    print('\n************ : # of partitions - ' + str(finances_df.rdd.getNumPartitions()))

    print('\n************ : finances_df.show()')
    finances_df.show()



    # Repartition based on has_student_loan_ column and write to FileSystem

    file_write_path = app_const.file_write_path + '/finances_generated'

    finances_df.repartition(2).write \
        .partitionBy('has_student_loan_') \
        .mode('overwrite') \
        .option('header', 'true') \
        .option('delimiter', '~') \
        .csv(file_write_path)

    print('\n************ File content repartitioned to 2 on column \'has_student_loan_\' is written to : ' + file_write_path)

    # Stop application
    spark.stop()


# Command
# --------------------
# spark-submit dataframe/ingestion/dataframe_from_files/csv_to_dataframe.py
#
# Output
# ----------------
# ************ Create DataFrame using SparkSession.read.load() .
#
# Finances DataFrame Schema : finances_df.printSchema()
# root
#  |-- id: integer (nullable = true)
#  |-- has_debt: boolean (nullable = true)
#  |-- has_financial_dependents: boolean (nullable = true)
#  |-- has_student_loans: boolean (nullable = true)
#  |-- income: double (nullable = true)
#
#
# ************ : finances_df.show()
# +---+--------+------------------------+-----------------+-------+
# | id|has_debt|has_financial_dependents|has_student_loans| income|
# +---+--------+------------------------+-----------------+-------+
# |101|   false|                    true|             true|60000.0|
# |102|    true|                    true|            false|50000.0|
# |103|    true|                    true|            false|55000.0|
# |104|    true|                   false|            false|65000.0|
# +---+--------+------------------------+-----------------+-------+
#
#
# ************ : SparkSession.read.csv()
#
# Finances DataFrame Schema : finances_df.printSchema()
# root
#  |-- id: integer (nullable = true)
#  |-- has_debt: boolean (nullable = true)
#  |-- has_financial_dependents: boolean (nullable = true)
#  |-- has_student_loans: boolean (nullable = true)
#  |-- income: double (nullable = true)
#
#
# ************ : finances_df.show()
# +---+--------+------------------------+-----------------+-------+
# | id|has_debt|has_financial_dependents|has_student_loans| income|
# +---+--------+------------------------+-----------------+-------+
# |101|   false|                    true|             true|60000.0|
# |102|    true|                    true|            false|50000.0|
# |103|    true|                    true|            false|55000.0|
# |104|    true|                   false|            false|65000.0|
# +---+--------+------------------------+-----------------+-------+
#
#
# ************ : # of partitions - 1
#
# ************ : finances_df.show()
# +---+---------+-------------------------+-----------------+-------+
# | id|has_debt_|has_financial_dependents_|has_student_loan_|income_|
# +---+---------+-------------------------+-----------------+-------+
# |101|    false|                     true|             true|60000.0|
# |102|     true|                     true|            false|50000.0|
# |103|     true|                     true|            false|55000.0|
# |104|     true|                    false|            false|65000.0|
# +---+---------+-------------------------+-----------------+-------+
#
#
# ************ File content repartitioned to 2 on column 'has_student_loan_' is written to : /home/viveksingh/project-data/app-generated/finances_generated

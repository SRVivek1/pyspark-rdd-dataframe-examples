"""
This program demonstrates how to read a JSON file and create DataFrame object.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from constants import app_constants as appConstants

if __name__ == '__main__':
    print('\n **************************** App read JSON in DataFrame ****************************')

    sparkSession = SparkSession.builder.appName('json-to-dataframe').getOrCreate()

    sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel('ERROR')

    # Read JSON data
    companies_df = sparkSession.read\
        .json(appConstants.company_json)

    print('\n****************** companies_df.printSchema()')
    companies_df.printSchema()

    print('\n****************** companies_df.show(5, False)')
    companies_df.show(5, False)

    # Select specific columns from Data frame
    print('\n****************** companies_df.select(col(\'company\')).show(5, False)')
    companies_df.select(col('company')).show(5, False)

    print("\n****************** companies_df.select(companies_df['employees']).show(5, False)")
    companies_df.select(companies_df['employees']).show(5, False)

    # Flatten records using explode(...) function
    print("\n****************** companies_df.select(col('company'), "
          "explode(col('employees')).alias('employee')).show(100, False)")
    flattened_com_df = companies_df.select(col('company'), explode(col('employees')).alias('employee'))
    flattened_com_df.show(5, False)

    # Rename employee.firstName to emp_name
    print('\n****************** Fetch company, employee & firstName (as emp_name) columns')
    flattened_com_df \
        .select(col('company'), col('employee'), col('employee.firstName').alias('emp_name')) \
        .show(truncate=False)

# Command
# -----------------
#   spark-submit dataframe/ingestion/dataframe_from_files/json_to_dataframe.py
#
# Output
# -----------------
#
# ****************** companies_df.printSchema()
# root
#  |-- company: string (nullable = true)
#  |-- employees: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- firstName: string (nullable = true)
#  |    |    |-- lastName: string (nullable = true)
#
#
# ****************** companies_df.show(5, False)
# +--------+-------------------------------------+
# |company |employees                            |
# +--------+-------------------------------------+
# |NewCo   |[{Sidhartha, Ray}, {Pratik, Solanki}]|
# |FamilyCo|[{Jiten, Gupta}, {Pallavi, Gupta}]   |
# |OldCo   |[{Vivek, Garg}, {Nitin, Gupta}]      |
# |ClosedCo|[]                                   |
# +--------+-------------------------------------+
#
#
# ****************** companies_df.select(col('company')).show(5, False)
# +--------+
# |company |
# +--------+
# |NewCo   |
# |FamilyCo|
# |OldCo   |
# |ClosedCo|
# +--------+
#
#
# ****************** companies_df.select(companies_df['employees']).show(5, False)
# +-------------------------------------+
# |employees                            |
# +-------------------------------------+
# |[{Sidhartha, Ray}, {Pratik, Solanki}]|
# |[{Jiten, Gupta}, {Pallavi, Gupta}]   |
# |[{Vivek, Garg}, {Nitin, Gupta}]      |
# |[]                                   |
# +-------------------------------------+
#
#
# ****************** companies_df.select(col('company'), explode(col('employees')).alias('employee')).show(100, False)
# +--------+-----------------+
# |company |employee         |
# +--------+-----------------+
# |NewCo   |{Sidhartha, Ray} |
# |NewCo   |{Pratik, Solanki}|
# |FamilyCo|{Jiten, Gupta}   |
# |FamilyCo|{Pallavi, Gupta} |
# |OldCo   |{Vivek, Garg}    |
# +--------+-----------------+
# only showing top 5 rows
#
#
# ****************** Fetch company, employee & firstName (as emp_name) columns
# +--------+-----------------+---------+
# |company |employee         |emp_name |
# +--------+-----------------+---------+
# |NewCo   |{Sidhartha, Ray} |Sidhartha|
# |NewCo   |{Pratik, Solanki}|Pratik   |
# |FamilyCo|{Jiten, Gupta}   |Jiten    |
# |FamilyCo|{Pallavi, Gupta} |Pallavi  |
# |OldCo   |{Vivek, Garg}    |Vivek    |
# |OldCo   |{Nitin, Gupta}   |Nitin    |
# +--------+-----------------+---------+

"""
This program demonstrates how to read a JSON file and create DataFrame object.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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

    print('\n****************** companies_df.select(companies_df[\'company\']).show(5, False)')
    companies_df.select(companies_df['employees']).show(5, False)



# Command
# -----------------
#   spark-submit dataframe/ingestion/dataframe_from_files/json_to_dataframe.py
#
# Output
# -----------------
#
#
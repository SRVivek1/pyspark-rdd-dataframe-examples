"""
This program demonstrates how to read a JSON file and create DataFrame object.
"""

from pyspark.sql import SparkSession
from constants import app_constants as appConstants

if __name__ == '__main__':
    print('\n **************************** App read JSON in DataFrame ****************************')

    sparkSession = SparkSession.builder.appName('json-to-dataframe').getOrCreate()

    sparkContext = sparkSession.sparkContext
    sparkContext.setLogLevel('ERROR')

    companies_df = sparkSession.read.json(appConstants.company_json)

    print('\n****************** companies_df.printSchema()')
    companies_df.printSchema()

    print('\n****************** companies_df.show()')
    companies_df.show()

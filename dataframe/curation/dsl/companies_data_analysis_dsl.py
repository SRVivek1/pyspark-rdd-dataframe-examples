"""
This program demonstrates different transformation operations/actions on DataFrame instance.
"""


from pyspark.sql import SparkSession
import constants.app_constants as app_const

if __name__ == '__main__':
    """
    Driver program
    """

    sparkSession = SparkSession\
        .builder\
        .appName('dataframe-curation-examples')\
        .master('local[*]')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    print('\n**************** Data Curation using DSL ****************\n')

    # Read company.json file
    print('\n**************** Data source : ' + app_const.company_json)
    company_df = sparkSession.read.json(app_const.company_json)

    # Print schema and sample records
    print('\n**************** company_df.printSchema()')
    company_df.printSchema()

    print('\n**************** company_df.show(5, False)')
    company_df.show(5, False)

    # Split Employees object as one object per row
    company_df_temp = company_df.select('company')
    company_df_temp.show(5, False)


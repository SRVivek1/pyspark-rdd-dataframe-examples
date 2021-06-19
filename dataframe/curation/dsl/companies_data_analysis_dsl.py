"""
This program demonstrates different transformation operations/actions on DataFrame instance.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, posexplode, expr
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

    # Show different records
    print('\n**************** company_df.select(\'company\').show(5, False)')
    company_df.select('company').show(5, False)

    print('\n**************** company_df.select(explode(\'employees\').alias(\'employee\')).show(5, False)')
    company_df.select(explode('employees').alias('employee')).show(5, False)

    # Explode data with their position in their container array/map instance
    print('\n**************** company_df.select(\'company\', '
          'posexplode(\'employees\')).show(5, False)')
    company_df.select('company', posexplode('employees').alias('employee_pos', 'employee')).show(5, False)

    # Split Employees object as one object per row
    print('\n**************** Normalize records in dataframe : '
          'company_df.select(\'company\', explode(\'employees\').alias(\'employee\'))')
    company_df_temp = company_df.select('company', explode('employees').alias('employee'))
    company_df_temp.show(5, False)

    # Using expressions in select function
    print('\n**************** company_df_temp.select(\'company\', expr(\'employee.firstName as FirstName\'))')
    company_df_temp.select('company', expr('employee.firstName as FirstName'))


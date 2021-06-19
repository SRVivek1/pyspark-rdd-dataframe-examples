"""
This program demonstrates different transformation operations/actions on DataFrame instance using SQL queries.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode,
    posexplode,
    expr,
    when,
    col
)
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

    # Create table/view to transform using SQL query
    company_df.createOrReplaceTempView('companies')

    # explode employees column
    sql_explode_query = 'select company, explode(employees) as employee from companies'
    print('\n************* SQL Explode query : ' + sql_explode_query)

    employees_df_temp = sparkSession.sql(sql_explode_query)

    print('Exploded dataframe : employees_df_temp.show(5, False)')
    employees_df_temp.show(5, False)

    employees_df_temp.createOrReplaceTempView('employees')

    sql_posexplode_query = 'select company, posexplode(employees) as (employee_pos, employee) from companies'
    print('\n************* SQL PosExplode query : ' + sql_posexplode_query)
    sparkSession.sql(sql_posexplode_query)
# Command
# -----------------
#
#
# Output
# ---------------------
#

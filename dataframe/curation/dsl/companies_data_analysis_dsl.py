"""
This program demonstrates different transformation operations/actions on DataFrame instance.
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
    company_df_temp\
        .select('company', expr('employee.firstName as FirstName'))\
        .show(truncate=False)

    print('\n**************** company_df_temp.select(\'company\', '
          'expr(\'employee.firstName as FirstName\'), expr(\'employee.lastName as LastName\'))')
    company_df_temp \
        .select('company', expr('employee.firstName as FirstName'), expr('employee.lastName as LastName')) \
        .show(truncate=False)

    # Perform if-else condition checks
    company_df_temp = company_df_temp\
        .select('*',
                when(col('company') == 'NewCo', 'Start-up')
                .when(col('company') == 'OldCo', 'Legacy')
                .otherwise('Standard').alias('Tier'))

    print('\n*************** Demonstrate functions -> when(...).otherwise(...)')
    company_df_temp.show(truncate=False)

    print('\n*************** Company dataframe transformed using select(..., expr(...)) methods : ')
    company_df_temp \
        .select('company', 'Tier',
                expr('employee.firstName as First_Name'),
                expr('employee.lastName as Last_Name')) \
        .show(truncate=False)

# Command
# -----------------
#  spark-submit dataframe/curation/dsl/companies_data_analysis_dsl.py
#
# Output
# ---------------------
# **************** Data Curation using DSL ****************
#
#
# **************** Data source : /home/viveksingh/project-data/sidharth/data/company.json
#
# **************** company_df.printSchema()
# root
#  |-- company: string (nullable = true)
#  |-- employees: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- firstName: string (nullable = true)
#  |    |    |-- lastName: string (nullable = true)
#
#
# **************** company_df.show(5, False)
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
# **************** company_df.select('company').show(5, False)
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
# **************** company_df.select(explode('employees').alias('employee')).show(5, False)
# +-----------------+
# |employee         |
# +-----------------+
# |{Sidhartha, Ray} |
# |{Pratik, Solanki}|
# |{Jiten, Gupta}   |
# |{Pallavi, Gupta} |
# |{Vivek, Garg}    |
# +-----------------+
# only showing top 5 rows
#
#
# **************** company_df.select('company', posexplode('employees')).show(5, False)
# +--------+------------+-----------------+
# |company |employee_pos|employee         |
# +--------+------------+-----------------+
# |NewCo   |0           |{Sidhartha, Ray} |
# |NewCo   |1           |{Pratik, Solanki}|
# |FamilyCo|0           |{Jiten, Gupta}   |
# |FamilyCo|1           |{Pallavi, Gupta} |
# |OldCo   |0           |{Vivek, Garg}    |
# +--------+------------+-----------------+
# only showing top 5 rows
#
#
# **************** Normalize records in dataframe : company_df.select('company', explode('employees').alias('employee'))
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
# **************** company_df_temp.select('company', expr('employee.firstName as FirstName'))
# +--------+---------+
# |company |FirstName|
# +--------+---------+
# |NewCo   |Sidhartha|
# |NewCo   |Pratik   |
# |FamilyCo|Jiten    |
# |FamilyCo|Pallavi  |
# |OldCo   |Vivek    |
# |OldCo   |Nitin    |
# +--------+---------+
#
#
# **************** company_df_temp.select('company', expr('employee.firstName as FirstName'), expr('employee.lastName as LastName'))
# +--------+---------+--------+
# |company |FirstName|LastName|
# +--------+---------+--------+
# |NewCo   |Sidhartha|Ray     |
# |NewCo   |Pratik   |Solanki |
# |FamilyCo|Jiten    |Gupta   |
# |FamilyCo|Pallavi  |Gupta   |
# |OldCo   |Vivek    |Garg    |
# |OldCo   |Nitin    |Gupta   |
# +--------+---------+--------+
#
#
# *************** Demonstrate functions -> when(...).otherwise(...)
# +--------+-----------------+--------+
# |company |employee         |Tier    |
# +--------+-----------------+--------+
# |NewCo   |{Sidhartha, Ray} |Start-up|
# |NewCo   |{Pratik, Solanki}|Start-up|
# |FamilyCo|{Jiten, Gupta}   |Standard|
# |FamilyCo|{Pallavi, Gupta} |Standard|
# |OldCo   |{Vivek, Garg}    |Legacy  |
# |OldCo   |{Nitin, Gupta}   |Legacy  |
# +--------+-----------------+--------+
#
#
# *************** Company dataframe transformed using select(..., expr(...)) methods :
# +--------+--------+----------+---------+
# |company |Tier    |First_Name|Last_Name|
# +--------+--------+----------+---------+
# |NewCo   |Start-up|Sidhartha |Ray      |
# |NewCo   |Start-up|Pratik    |Solanki  |
# |FamilyCo|Standard|Jiten     |Gupta    |
# |FamilyCo|Standard|Pallavi   |Gupta    |
# |OldCo   |Legacy  |Vivek     |Garg     |
# |OldCo   |Legacy  |Nitin     |Gupta    |
# +--------+--------+----------+---------+

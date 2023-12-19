"""
This program demonstrates different transformation operations/actions on DataFrame instance using SQL queries.
"""


from pyspark.sql import SparkSession
import constants.app_constants as app_const
import os.path
import yaml


if __name__ == '__main__':
    """
    Driver program
    """

    sparkSession = SparkSession\
        .builder\
        .appName('dataframe-curation-examples')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    print('\n**************** Data Curation using SQL ****************\n')

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

    print('\n************* sparkSession.sql(sql_posexplode_query).show(5, False)')
    sparkSession.sql(sql_posexplode_query).show(5, False)

    # Load application.yml
    app_conf = yaml.load(
        open(
            os.path.abspath(
                os.path.abspath(os.path.dirname(__file__)) +
                '/../../../' +
                'application.yml')),
        Loader=yaml.FullLoader)

    # SQL - Case-When query
    sql_case_when_query = app_conf['spark_sql_demo']['case_when_demo']
    print('\n************** SQL Case-When Query : ' + sql_case_when_query)

    print('\n************** sparkSession.sql(sql_case_when_query).show(5, False) : ')
    sparkSession.sql(sql_case_when_query)\
        .show(5, False)

# Command
# -----------------
# spark-submit dataframe/curation/sql/companies_data_analysis_sql.py
#
# Output
# ---------------------
# **************** Data Curation using SQL ****************
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
# ************* SQL Explode query : select company, explode(employees) as employee from companies
# Exploded dataframe : employees_df_temp.show(5, False)
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
# ************* SQL PosExplode query : select company, posexplode(employees) as (employee_pos, employee) from companies
#
# ************* sparkSession.sql(sql_posexplode_query).show(5, False)
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
# ************** SQL Case-When Query : select
#     company,
#     employee.firstName as firstName,
#     case
#         when company = 'FamilyCo' then 'Premium'
#         when company = 'OldCo' then 'Legacy'
#         else 'Standard'
#     end as Tier
# from
#     employees
#
#
# ************** sparkSession.sql(sql_case_when_query).show(5, False) :
# +--------+---------+--------+
# |company |firstName|Tier    |
# +--------+---------+--------+
# |NewCo   |Sidhartha|Standard|
# |NewCo   |Pratik   |Standard|
# |FamilyCo|Jiten    |Premium |
# |FamilyCo|Pallavi  |Premium |
# |OldCo   |Vivek    |Legacy  |
# +--------+---------+--------+
# only showing top 5 rows

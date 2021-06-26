"""
This program demonstrates the use of SPARK Joins.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType
)
from pyspark.sql.functions import col

if __name__ == '__main__':
    """
    Driver program
    """

    sparkSession = SparkSession\
        .builder\
        .appName('spark-join-demo-app')\
        .master('local[*]')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    print('\n*********************** Spark Joins Demo ***********************\n')

    # Employee table columns
    emp_columns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]
    emp_schema = StructType([
        StructField("emp_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("superior_emp_id", IntegerType(), False),
        StructField("year_joined", StringType(), False),
        StructField("emp_dept_id", IntegerType(), True),
        StructField("gender", StringType(), False),
        StructField("salary", IntegerType(), False)
    ])

    department_schema = StructType([
        StructField("dept_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("location", StringType(), False)
    ])

    # Prepare employee data
    employees = [
        [1, "Smith", -1, "2018", 10, "M", 3000],
        [2, "Rose", 1, "2010", 20, "M", 4000],
        [3, "Williams", 1, "2010", 10, "M", 1000],
        [4, "Jones", 2, "2005", 10, "F", 2000],
        [5, "Brown", 2, "2010", 40, "", -1],
        [6, "Jack", 2, "2010", 50, "", -1],
        [7, "Reddy", 2, "2010", 100, "F", 0],
        [8, "Philip", 2, "2010", 110, "M", 5000],
        [9, "Philip", 2, "2010", -1, "M", 5000]
    ]

    departments = [
        [10, "Department-10", "IND"],
        [20, "Department-20", "US"],
        [40, "Department-40", "UK"],
        [50, "Department-50", "SNG"],
        [60, "Department-60", "D60"],
        [70, "Department-70", "D70"],
        [80, "Department-80", ""],
        [90, "Department-90", ""]
    ]

    # Create Employees Dataframe
    employees_df = sparkSession.createDataFrame(employees, schema=emp_schema)

    print('\n************** employees_df.printSchema()')
    employees_df.printSchema()

    print('\n************** employees_df.show(truncate=False)')
    employees_df.show(truncate=False)

    # Create Department dataframe
    department_df = sparkSession.createDataFrame(departments, schema=department_schema)

    print('\n************** department_df.printSchema()')
    department_df.printSchema()

    print('\n************** department_df.show(truncate=False)')
    department_df.show(truncate=False)

    # Start : Inner join
    # default is 'inner join only'
    print('\n***************************** Dataframe inner join *****************************')
    innerjoin_df = employees_df\
        .join(department_df,
              employees_df.emp_dept_id == department_df.dept_id,
              how='inner')

    print('\n************** employees_df.join(department_df, '
          'employees_df.emp_dept_id == department_df.department_id, how=\'inner\')')
    print('\n************** innerjoin_df.sort(innerjoin_df.emp_id).show(10, False)')
    innerjoin_df.sort(innerjoin_df.emp_id).show(10, False)
    # End : Inner join

    # Start : Full outer join
    # outer a.k.a full, full_outer
    print('\n***************************** Dataframe outer/full_outer join *****************************')
    full_outer_join_df = employees_df\
        .join(department_df,
              employees_df.emp_dept_id == department_df.dept_id,
              how='full_outer')

    print("""\n************** full_outer_join_df = employees_df\
        .join(department_df,
              employees_df.emp_dept_id == department_df.dept_id,
              how='full_outer')""")
    print('\n**************** full_outer_join_df.sort(full_outer_join_df.emp_id).show(truncate=False)')
    full_outer_join_df.sort(full_outer_join_df.emp_id).show(truncate=False)
    # End : Full outer join

    # Start : Left outer join
    # left a.k.a left_outer
    print('\n***************************** Dataframe Left/Left outer join *****************************')
    # Employees left_outer join
    employees_left_outer_df = employees_df\
        .join(department_df,
              employees_df.emp_id == department_df.dept_id,
              'left_outer')
    print('\n*************** employees_left_outer_df.sort(employees_left_outer_df.emp_id).show(truncate=False)')
    employees_left_outer_df.sort(employees_left_outer_df.emp_id).show(truncate=False)

    # Department left_outer join
    department_left_outer_df = department_df\
        .join(employees_df,
              department_df.dept_id == employees_df.emd_dept_id,
              'left_outer')
    print('\n*************** department_left_outer_df.sort(department_left_outer_df.dept_id).show(truncate=False)')
    department_left_outer_df.sort(department_left_outer_df.dept_id).show(truncate=False)
    # End : Left outer join

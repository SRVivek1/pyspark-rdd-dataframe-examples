"""
This program demonstrates the use of SPARK Joins.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType
)


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

    # Prepare employee data
    employees = [
        [1, "Smith", -1, "2018", 10, "M", 3000],
        [2, "Rose", 1, "2010", 20, "M", 4000],
        [3, "Williams", 1, "2010", 10, "M", 1000],
        [4, "Jones", 2, "2005", 10, "F", 2000],
        [5, "Brown", 2, "2010", 40, "", -1],
        [6, "Brown", 2, "2010", 50, "", -1]
    ]

    print('\n************** type(employees): {0}'.format(type(employees)))
    print('\n************** type(employees[0]): {0}'.format(type(employees[0])))

    # Create Employees Dataframe
    employees_df = sparkSession.createDataFrame(employees, schema=emp_schema)

    print('\n************** employees_df.printSchema()')
    employees_df.printSchema()

    print('\n************** employees_df.show(5, truncate=False)')
    employees_df.show(5, truncate=False)




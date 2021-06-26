"""
This program demonstrates the implementation of 'Broadcast View'.
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, StructField, IntegerType, StructType
)

if __name__ == '__main__':
    """
    Driver function
    """

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
        StructField("name", StringType(), False)
    ])

    # Prepare employee data
    employees = [
        [1, "Smith", -1, "2018", 10, "M", 3000],
        [2, "Rose", 1, "2010", 20, "M", 4000],
        [3, "Williams", 1, "2010", 10, "M", 1000],
        [4, "Jones", 2, "2005", 10, "F", 2000],
        [5, "Brown", 2, "2010", 20, "", -1],
        [6, "Jack", 2, "2010", 10, "", -1],
        [7, "Reddy", 2, "2010", 20, "F", 0],
        [8, "Philip", 2, "2010", 20, "M", 5000],
        [9, "Alex", 2, "2010", -1, "M", 5000]
    ]
    # Departments data
    departments = [
        [10, "HR"],
        [20, "FINANCE"],
        [40, "IT"]
    ]

    sparkSession = SparkSession\
        .builder\
        .appName('spark-broadcast-join-app')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    # Print schema & sample data
    print('\n********************************** Broadcast join **********************************\n')

    employees_df = sparkSession.createDataFrame(employees, schema=emp_schema)
    departments_df = sparkSession.createDataFrame(departments, schema=department_schema)

    print('\n******************* employees_df.printSchema()')
    employees_df.printSchema()

    print('\n******************* employees_df.show(truncate=False)')
    employees_df.show(truncate=False)

    print('\n******************* departments_df.printSchema()')
    departments_df.printSchema()

    print('\n******************* departments_df.show(truncate=False)')
    departments_df.show(truncate=False)


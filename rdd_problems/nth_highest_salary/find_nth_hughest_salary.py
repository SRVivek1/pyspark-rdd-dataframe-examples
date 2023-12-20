"""
    Requirement
    ------------------
        >> Write a program to find the nth highest salary department wise.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('POC - Find nth highest salary') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # Create new df using collection
    employees_df = sc.parallelize([("James", "Sales", 3000),
                                   ("Michael", "Sales", 4600),
                                   ("Robert", "Sales", 4100),
                                   ("Maria", "Finance", 3000),
                                   ("James", "Sales", 3000),
                                   ("Scott", "Finance", 3300),
                                   ("Jen", "Finance", 3900),
                                   ("Jeff", "Marketing", 3000),
                                   ("Kumar", "Marketing", 2000),
                                   ("Kumar", "Marketing", 4000),
                                   ("Kumar", "Marketing", 5000),
                                   ("Saif", "Sales", 4100)]) \
        .toDF(['emp_name', 'department', 'salary'])

    employees_df.printSchema()
    employees_df.orderBy('department', 'salary').show()

    # Solution
    # ------------------------
    # Partition them column wise
    # sort them salary wise
    # Rank them
    # first nth rank field

    window_spec = Window.partitionBy('department').orderBy(col('salary').desc())

    # Overview of rank() and dense_rank() functions
    employees_df \
        .withColumn('salary_rank', rank().over(window_spec)) \
        .withColumn('salary_dense_rank', dense_rank().over(window_spec)) \
        .show()

    # Highest paid per department
    print("*************** Highest paid salary per department")
    employees_df \
        .withColumn('salary_dense_rank', dense_rank().over(window_spec)) \
        .filter("salary_dense_rank == 1") \
        .show()

    # 2nd highest paid per department
    print("*************** 2nd highest paid salary per department")
    employees_df \
        .withColumn('salary_dense_rank', dense_rank().over(window_spec)) \
        .filter("salary_dense_rank == 2") \
        .show()

    # 3rd highest paid per department
    print("*************** 3rd highest paid salary per department")
    employees_df \
        .withColumn('salary_dense_rank', dense_rank().over(window_spec)) \
        .filter("salary_dense_rank == 3") \
        .show()

#
# DataBricks CE cluster
#
# output
# ------------------
# root
#  |-- emp_name: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: long (nullable = true)
#
# +--------+----------+------+
# |emp_name|department|salary|
# +--------+----------+------+
# |   Maria|   Finance|  3000|
# |   Scott|   Finance|  3300|
# |     Jen|   Finance|  3900|
# |   Kumar| Marketing|  2000|
# |    Jeff| Marketing|  3000|
# |   Kumar| Marketing|  4000|
# |   Kumar| Marketing|  5000|
# |   James|     Sales|  3000|
# |   James|     Sales|  3000|
# |  Robert|     Sales|  4100|
# |    Saif|     Sales|  4100|
# | Michael|     Sales|  4600|
# +--------+----------+------+
#
# +--------+----------+------+-----------+-----------------+
# |emp_name|department|salary|salary_rank|salary_dense_rank|
# +--------+----------+------+-----------+-----------------+
# |     Jen|   Finance|  3900|          1|                1|
# |   Scott|   Finance|  3300|          2|                2|
# |   Maria|   Finance|  3000|          3|                3|
# |   Kumar| Marketing|  5000|          1|                1|
# |   Kumar| Marketing|  4000|          2|                2|
# |    Jeff| Marketing|  3000|          3|                3|
# |   Kumar| Marketing|  2000|          4|                4|
# | Michael|     Sales|  4600|          1|                1|
# |  Robert|     Sales|  4100|          2|                2|
# |    Saif|     Sales|  4100|          2|                2|
# |   James|     Sales|  3000|          4|                3|
# |   James|     Sales|  3000|          4|                3|
# +--------+----------+------+-----------+-----------------+
#
# *************** Highest paid salary per department
# +--------+----------+------+-----------------+
# |emp_name|department|salary|salary_dense_rank|
# +--------+----------+------+-----------------+
# |     Jen|   Finance|  3900|                1|
# |   Kumar| Marketing|  5000|                1|
# | Michael|     Sales|  4600|                1|
# +--------+----------+------+-----------------+
#
# *************** 2nd highest paid salary per department
# +--------+----------+------+-----------------+
# |emp_name|department|salary|salary_dense_rank|
# +--------+----------+------+-----------------+
# |   Scott|   Finance|  3300|                2|
# |   Kumar| Marketing|  4000|                2|
# |  Robert|     Sales|  4100|                2|
# |    Saif|     Sales|  4100|                2|
# +--------+----------+------+-----------------+
#
# *************** 3rd highest paid salary per department
# +--------+----------+------+-----------------+
# |emp_name|department|salary|salary_dense_rank|
# +--------+----------+------+-----------------+
# |   Maria|   Finance|  3000|                3|
# |    Jeff| Marketing|  3000|                3|
# |   James|     Sales|  3000|                3|
# |   James|     Sales|  3000|                3|
# +--------+----------+------+-----------------+
#
#

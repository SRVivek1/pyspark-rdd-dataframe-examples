"""
    Requirement
    -------------------
        --> Find the running sum (prefix sum) department wise in the given data set.
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

    # WindowSpec
    win_spec = Window.partitionBy('department') \
        .orderBy(col('salary').desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Running sum
    result_df = employees_df.withColumn('running_sum', sum('salary').over(win_spec))
    result_df.show()

#
# Output
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
# +--------+----------+------+-----------+
# |emp_name|department|salary|running_sum|
# +--------+----------+------+-----------+
# |   Maria|   Finance|  3000|       3000|
# |   Scott|   Finance|  3300|       6300|
# |     Jen|   Finance|  3900|      10200|
# |   Kumar| Marketing|  2000|       2000|
# |    Jeff| Marketing|  3000|       5000|
# |   Kumar| Marketing|  4000|       9000|
# |   Kumar| Marketing|  5000|      14000|
# |   James|     Sales|  3000|       3000|
# |   James|     Sales|  3000|       6000|
# |  Robert|     Sales|  4100|      10100|
# |    Saif|     Sales|  4100|      14200|
# | Michael|     Sales|  4600|      18800|
# +--------+----------+------+-----------+
#

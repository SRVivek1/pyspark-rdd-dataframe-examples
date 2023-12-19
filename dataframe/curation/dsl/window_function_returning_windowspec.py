"""
    Requirement
    -----------------
        --> Demonstrate the Window function used to create window spec.
        --> Demonstrate following properties:
            >> Window.unboundedPreceding
            >> Window.unboundedFollowing
            >> Window.currentRow
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('POC - WindowSpec builder functions') \
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
                               ("Saif", "Sales", 4100)]).toDF(['emp_name', 'department', 'salary'])

    employees_df.printSchema()
    employees_df.show()

    # Window.partitionBy - moves matching records to same partition
    # WindowSpec.orderBy - ordering column
    window_spec = Window.partitionBy('department').orderBy('salary')

    # For each department assign a number based on their salary & order them based on salary
    print("*************** df.withColumn('row_number', row_number().over(window_spec))"
          + ".withColumn('salary_rank', rank().over(window_spec))")
    result_df = employees_df\
        .withColumn('row_number', row_number().over(window_spec)) \
        .withColumn('salary_rank', rank().over(window_spec))
    result_df.show()

    # Show highest salary paid employees
    print("*************** df.filter(result_df.salary_rank == 1)")
    result_df = result_df.filter(result_df.salary_rank == 1)
    result_df.show()

    # Window.rowsBetween(s, e) - operate on rows/records in the given range
    # Considers current and current + 0 following rows for avg(..) aggregate function
    print("*************** window_spec.rowsBetween(Window.currentRow, 0)")
    first_x_rows_spec = window_spec.rowsBetween(Window.currentRow, 0)
    result_df = employees_df.withColumn('avg_salary', avg('salary').over(first_x_rows_spec))
    result_df.show()

    # Rows between, current row + next row
    # Considers current and current + 1 following rows for avg(..) aggregate function
    # It will consider all records in that partition where the value is in range of
    #   --> (Window.currentRow + 900)
    # For same value on Window.currentRow it will simply consider previously calculated
    # value for the preceding (previous) row.
    # Window.rangeBetween(s, e) - Find all records in the range of current_row + value of the column
    print("*************** window_spec.rowsBetween(Window.currentRow, 900)")
    print("*************** df.withColumn('avg_salary_range_between', sum('salary').over(x_values_in_range_spec))")
    x_values_in_range_spec = Window.partitionBy('department').orderBy('salary').rangeBetween(Window.currentRow, 900)
    result_df = employees_df.withColumn('avg_salary_range_between', sum('salary').over(x_values_in_range_spec))
    result_df.show()

#
# command
# ----------------
# spark-submit --master yarn
#
# Platform : Ddatabricks CE server
#
# Output
# ----------------
# root
#  |-- emp_name: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: long (nullable = true)
#
# +--------+----------+------+
# |emp_name|department|salary|
# +--------+----------+------+
# |   James|     Sales|  3000|
# | Michael|     Sales|  4600|
# |  Robert|     Sales|  4100|
# |   Maria|   Finance|  3000|
# |   James|     Sales|  3000|
# |   Scott|   Finance|  3300|
# |     Jen|   Finance|  3900|
# |    Jeff| Marketing|  3000|
# |   Kumar| Marketing|  2000|
# |    Saif|     Sales|  4100|
# +--------+----------+------+
#
# *************** df.withColumn('row_number', row_number().over(window_spec)).withColumn('salary_rank', rank().over(window_spec))
# +--------+----------+------+----------+-----------+
# |emp_name|department|salary|row_number|salary_rank|
# +--------+----------+------+----------+-----------+
# |   Maria|   Finance|  3000|         1|          1|
# |   Scott|   Finance|  3300|         2|          2|
# |     Jen|   Finance|  3900|         3|          3|
# |   Kumar| Marketing|  2000|         1|          1|
# |    Jeff| Marketing|  3000|         2|          2|
# |   James|     Sales|  3000|         1|          1|
# |   James|     Sales|  3000|         2|          1|
# |  Robert|     Sales|  4100|         3|          3|
# |    Saif|     Sales|  4100|         4|          3|
# | Michael|     Sales|  4600|         5|          5|
# +--------+----------+------+----------+-----------+
#
# *************** df.filter(result_df.salary_rank == 1)
# +--------+----------+------+----------+-----------+
# |emp_name|department|salary|row_number|salary_rank|
# +--------+----------+------+----------+-----------+
# |   Maria|   Finance|  3000|         1|          1|
# |   Kumar| Marketing|  2000|         1|          1|
# |   James|     Sales|  3000|         1|          1|
# |   James|     Sales|  3000|         2|          1|
# +--------+----------+------+----------+-----------+
#
# *************** window_spec.rowsBetween(Window.currentRow, 0)
# +--------+----------+------+----------+
# |emp_name|department|salary|avg_salary|
# +--------+----------+------+----------+
# |   Maria|   Finance|  3000|    3000.0|
# |   Scott|   Finance|  3300|    3300.0|
# |     Jen|   Finance|  3900|    3900.0|
# |   Kumar| Marketing|  2000|    2000.0|
# |    Jeff| Marketing|  3000|    3000.0|
# |   James|     Sales|  3000|    3000.0|
# |   James|     Sales|  3000|    3000.0|
# |  Robert|     Sales|  4100|    4100.0|
# |    Saif|     Sales|  4100|    4100.0|
# | Michael|     Sales|  4600|    4600.0|
# +--------+----------+------+----------+
#
# *************** window_spec.rowsBetween(Window.currentRow, 900)
# *************** df.withColumn('avg_salary_range_between', sum('salary').over(x_values_in_range_spec))
# +--------+----------+------+------------------------+
# |emp_name|department|salary|avg_salary_range_between|
# +--------+----------+------+------------------------+
# |   Maria|   Finance|  3000|                   10200|
# |   Scott|   Finance|  3300|                    7200|
# |     Jen|   Finance|  3900|                    3900|
# |   Kumar| Marketing|  2000|                    2000|
# |    Jeff| Marketing|  3000|                    3000|
# |   James|     Sales|  3000|                    6000|
# |   James|     Sales|  3000|                    6000|
# |  Robert|     Sales|  4100|                   12800|
# |    Saif|     Sales|  4100|                   12800|
# | Michael|     Sales|  4600|                    4600|
# +--------+----------+------+------------------------+
#
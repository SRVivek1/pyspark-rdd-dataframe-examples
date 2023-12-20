"""
    Requirement
    -----------------
        --> Write a program to calculate rolling average from below data set.
        --> New Columns:
            >> rolling_avg_full
            >> rolling_avg_last_4_records
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import bround, avg


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
                                   ("Lifeng", "Finance", 4100),
                                   ("Winnie", "Finance", 4900),
                                   ("Jeff", "Marketing", 3000),
                                   ("Kumar", "Marketing", 2000),
                                   ("Nisha", "Marketing", 3300),
                                   ("Rahul", "Marketing", 3900),
                                   ("Chwei", "Marketing", 4200),
                                   ("Umakanth", "Marketing", 4900),
                                   ("Saif", "Sales", 4100)]) \
        .toDF(['emp_name', 'department', 'salary'])

    employees_df.printSchema()
    employees_df.orderBy('department', 'salary').show()

    # partition by department, order by salary
    win_spec = Window.partitionBy('department').orderBy('salary')

    result_df = employees_df \
        .withColumn('rolling_avg', bround(avg('salary').over(win_spec), 2)) \
        .withColumn('r_avg_prev_0_rows', avg('salary')
                    .over(win_spec.rowsBetween(Window.currentRow, 0))) \
        .withColumn('r_avg_next_1_rows', avg('salary')
                    .over(win_spec.rowsBetween(Window.currentRow, 1))) \
        .withColumn('r_avg_prev_-1_rows', avg('salary')
                    .over(win_spec.rowsBetween(-1, Window.currentRow))) \
        .withColumn('r_avg_next_2_rows', bround(avg('salary')
                    .over(win_spec.rowsBetween(Window.currentRow, 2)), 2)) \
        .withColumn('r_avg_prev_-2_rows', bround(avg('salary')
                    .over(win_spec.rowsBetween(-2, Window.currentRow)), 2)) \
        .withColumn('r_avg_next_4_rows', bround(avg('salary')
                    .over(win_spec.rowsBetween(Window.currentRow, 4)), 2)) \
        .withColumn('r_avg_prev_-4_rows', bround(avg('salary')
                    .over(win_spec.rowsBetween(-4, Window.currentRow)), 2))

    result_df.show()

#
# Output
# -------------
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
# |  Lifeng|   Finance|  4100|
# |  Winnie|   Finance|  4900|
# |   Kumar| Marketing|  2000|
# |    Jeff| Marketing|  3000|
# |   Nisha| Marketing|  3300|
# |   Rahul| Marketing|  3900|
# |   Chwei| Marketing|  4200|
# |Umakanth| Marketing|  4900|
# |   James|     Sales|  3000|
# |   James|     Sales|  3000|
# |    Saif|     Sales|  4100|
# |  Robert|     Sales|  4100|
# | Michael|     Sales|  4600|
# +--------+----------+------+
#
# +--------+----------+------+-----------+-----------------+-----------------+------------------+-----------------+------------------+-----------------+------------------+
# |emp_name|department|salary|rolling_avg|r_avg_prev_0_rows|r_avg_next_1_rows|r_avg_prev_-1_rows|r_avg_next_2_rows|r_avg_prev_-2_rows|r_avg_next_4_rows|r_avg_prev_-4_rows|
# +--------+----------+------+-----------+-----------------+-----------------+------------------+-----------------+------------------+-----------------+------------------+
# |   Maria|   Finance|  3000|     3000.0|           3000.0|           3150.0|            3000.0|           3400.0|            3000.0|           3840.0|            3000.0|
# |   Scott|   Finance|  3300|     3150.0|           3300.0|           3600.0|            3150.0|          3766.67|            3150.0|           4050.0|            3150.0|
# |     Jen|   Finance|  3900|     3400.0|           3900.0|           4000.0|            3600.0|           4300.0|            3400.0|           4300.0|            3400.0|
# |  Lifeng|   Finance|  4100|     3575.0|           4100.0|           4500.0|            4000.0|           4500.0|           3766.67|           4500.0|            3575.0|
# |  Winnie|   Finance|  4900|     3840.0|           4900.0|           4900.0|            4500.0|           4900.0|            4300.0|           4900.0|            3840.0|
# |   Kumar| Marketing|  2000|     2000.0|           2000.0|           2500.0|            2000.0|          2766.67|            2000.0|           3280.0|            2000.0|
# |    Jeff| Marketing|  3000|     2500.0|           3000.0|           3150.0|            2500.0|           3400.0|            2500.0|           3860.0|            2500.0|
# |   Nisha| Marketing|  3300|    2766.67|           3300.0|           3600.0|            3150.0|           3800.0|           2766.67|           4075.0|           2766.67|
# |   Rahul| Marketing|  3900|     3050.0|           3900.0|           4050.0|            3600.0|          4333.33|            3400.0|          4333.33|            3050.0|
# |   Chwei| Marketing|  4200|     3280.0|           4200.0|           4550.0|            4050.0|           4550.0|            3800.0|           4550.0|            3280.0|
# |Umakanth| Marketing|  4900|     3550.0|           4900.0|           4900.0|            4550.0|           4900.0|           4333.33|           4900.0|            3860.0|
# |   James|     Sales|  3000|     3000.0|           3000.0|           3000.0|            3000.0|          3366.67|            3000.0|           3760.0|            3000.0|
# |   James|     Sales|  3000|     3000.0|           3000.0|           3550.0|            3000.0|          3733.33|            3000.0|           3950.0|            3000.0|
# |  Robert|     Sales|  4100|     3550.0|           4100.0|           4100.0|            3550.0|          4266.67|           3366.67|          4266.67|           3366.67|
# |    Saif|     Sales|  4100|     3550.0|           4100.0|           4350.0|            4100.0|           4350.0|           3733.33|           4350.0|            3550.0|
# | Michael|     Sales|  4600|     3760.0|           4600.0|           4600.0|            4350.0|           4600.0|           4266.67|           4600.0|            3760.0|
# +--------+----------+------+-----------+-----------------+-----------------+------------------+-----------------+------------------+-----------------+------------------+
#

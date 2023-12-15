"""
    Requirement
    -------------
        --> This program demonstrates window function usages with DataFrame created using parquet files.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, ntile
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.window import Window, WindowSpec
import os
import yaml

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('POC - AWS S3 read write parquet files') \
        .config('spark.sql.legacy.parquet.int96RebaseModeInRead', 'CORRECTED') \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    # READ Config
    cur_dir = os.path.abspath(os.path.abspath(__file__))
    app_conf = yaml.load(open(cur_dir + '../../../../../' + 'application.yml'), Loader=yaml.FullLoader)
    secrets = yaml.load(open(cur_dir + '../../../../../../' + '.secrets'), Loader=yaml.FullLoader)

    # AWS authentication
    hdp_cnf = sc._jsc.hadoopConfiguration()
    hdp_cnf.set('fs.s3a.access.key', secrets['s3_conf']['access_key'])
    hdp_cnf.set('fs.s3a.secret.key', secrets['s3_conf']['secret_access_key'])

    # Note -this DF object is not used as it has complex dataset
    # Read parquet files
    nyc_omo_df = spark.read \
        .parquet('s3a://' + app_conf['s3_conf']['s3_bucket'] + '/NYC_OMO/')

    # Using employee data from collection
    employees = [("James", "Sales", 3000),
           ("Michael", "Sales", 4600),
           ("Robert", "Sales", 4100),
           ("Maria", "Finance", 3000),
           ("James", "Sales", 3000),
           ("Scott", "Finance", 3300),
           ("Jen", "Finance", 3900),
           ("Jeff", "Marketing", 3000),
           ("Kumar", "Marketing", 2000),
           ("Saif", "Sales", 4100)]

    employees_rdd = sc.parallelize(employees) \
        .map(lambda rec: (rec[0], rec[1], float(rec[2])))

    for rec in employees_rdd.take(10):
        print(rec)

    # Schema
    employees_schema = StructType() \
        .add('name', StringType(), False) \
        .add('department', StringType(), True) \
        .add('salary', DoubleType(), True)

    print('****************** employees_df cretaed')
    employee_df = spark.createDataFrame(employees_rdd, schema=employees_schema)
    employee_df.printSchema()
    employee_df.show(10)

    # Window function - row_number
    # Group by department and order by salary
    print("***************** window_specs = Window.partitionBy('department').orderBy('salary')")
    window_specs = Window.partitionBy('department').orderBy('salary')

    # Ranking function
    print('***************** window function - row_number')
    temp_df = employee_df.withColumn('row_number', row_number().over(window_specs))
    temp_df.printSchema()
    temp_df.show()

    # rank(..)
    print('***************** window function- rank')
    temp_df = employee_df.withColumn('rank', rank().over(window_specs))
    temp_df.printSchema()
    temp_df.show()

    # Window function - dense_rank, percent_rank & ntile
    temp_df = employee_df \
        .withColumn('dense_rank', dense_rank().over(window_specs)) \
        .withColumn('percent_rank', percent_rank().over(window_specs)) \
        .withColumn('ntile', ntile(2).over(window_specs))

    temp_df.printSchema()
    temp_df.show()
    
#
# command
#
# spark-submit --packages 'org.apache.hadoop:hadoop-aws:2.7.4' --master yarn ./program.py
#
# Cluster - DataBricks
# ('James', 'Sales', 3000.0)
# ('Michael', 'Sales', 4600.0)
# ('Robert', 'Sales', 4100.0)
# ('Maria', 'Finance', 3000.0)
# ('James', 'Sales', 3000.0)
# ('Scott', 'Finance', 3300.0)
# ('Jen', 'Finance', 3900.0)
# ('Jeff', 'Marketing', 3000.0)
# ('Kumar', 'Marketing', 2000.0)
# ('Saif', 'Sales', 4100.0)
# ****************** employees_df cretaed
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#
# +-------+----------+------+
# |   name|department|salary|
# +-------+----------+------+
# |  James|     Sales|3000.0|
# |Michael|     Sales|4600.0|
# | Robert|     Sales|4100.0|
# |  Maria|   Finance|3000.0|
# |  James|     Sales|3000.0|
# |  Scott|   Finance|3300.0|
# |    Jen|   Finance|3900.0|
# |   Jeff| Marketing|3000.0|
# |  Kumar| Marketing|2000.0|
# |   Saif|     Sales|4100.0|
# +-------+----------+------+
#
# ***************** window_specs = Window.partitionBy('department').orderBy('salary')
# ***************** window function - row_number
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#  |-- row_number: integer (nullable = false)
#
# +-------+----------+------+----------+
# |   name|department|salary|row_number|
# +-------+----------+------+----------+
# |  Maria|   Finance|3000.0|         1|
# |  Scott|   Finance|3300.0|         2|
# |    Jen|   Finance|3900.0|         3|
# |  Kumar| Marketing|2000.0|         1|
# |   Jeff| Marketing|3000.0|         2|
# |  James|     Sales|3000.0|         1|
# |  James|     Sales|3000.0|         2|
# | Robert|     Sales|4100.0|         3|
# |   Saif|     Sales|4100.0|         4|
# |Michael|     Sales|4600.0|         5|
# +-------+----------+------+----------+
#
# ***************** window function- rank
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#  |-- rank: integer (nullable = false)
#
# +-------+----------+------+----+
# |   name|department|salary|rank|
# +-------+----------+------+----+
# |  Maria|   Finance|3000.0|   1|
# |  Scott|   Finance|3300.0|   2|
# |    Jen|   Finance|3900.0|   3|
# |  Kumar| Marketing|2000.0|   1|
# |   Jeff| Marketing|3000.0|   2|
# |  James|     Sales|3000.0|   1|
# |  James|     Sales|3000.0|   1|
# | Robert|     Sales|4100.0|   3|
# |   Saif|     Sales|4100.0|   3|
# |Michael|     Sales|4600.0|   5|
# +-------+----------+------+----+
#
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#  |-- dense_rank: integer (nullable = false)
#  |-- percent_rank: double (nullable = false)
#  |-- ntile: integer (nullable = false)
#
# +-------+----------+------+----------+------------+-----+
# |   name|department|salary|dense_rank|percent_rank|ntile|
# +-------+----------+------+----------+------------+-----+
# |  Maria|   Finance|3000.0|         1|         0.0|    1|
# |  Scott|   Finance|3300.0|         2|         0.5|    1|
# |    Jen|   Finance|3900.0|         3|         1.0|    2|
# |  Kumar| Marketing|2000.0|         1|         0.0|    1|
# |   Jeff| Marketing|3000.0|         2|         1.0|    2|
# |  James|     Sales|3000.0|         1|         0.0|    1|
# |  James|     Sales|3000.0|         1|         0.0|    1|
# | Robert|     Sales|4100.0|         2|         0.5|    1|
# |   Saif|     Sales|4100.0|         2|         0.5|    2|
# |Michael|     Sales|4600.0|         3|         1.0|    2|
# +-------+----------+------+----------+------------+-----+
#

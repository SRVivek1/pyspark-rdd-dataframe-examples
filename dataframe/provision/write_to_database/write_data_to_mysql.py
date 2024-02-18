"""
	Write data to MySQL
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StringType, DoubleType
from constants import app_constants

import pyspark.sql.functions as sqf

if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName('provision-to-mysql-db') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    schema = StructType() \
        .add('first_name', StringType(), False) \
        .add('last_name', StringType(), False) \
        .add('location', StringType(), False) \
        .add('department', StringType(), False) \
        .add('salary', StringType(), False)

    df = spark.read.csv(path=app_constants.employee_details_min_dup_csv, schema=schema, sep=',', header='true')

    df.show(10)

    # remove $ from salary
    df = df.withColumn('salary', sqf.regexp_replace('salary', '[$]', '').cast(DoubleType()))

    df.printSchema()
    df.show(10)

    print("Writing data to my sql database...")
    df.write \
        .option('driver', 'com.mysql.cj.jdbc.Driver') \
        .option('user', 'dummy-user') \
        .option('password', 'dummy-password') \
        .jdbc(url='jdbc:mysql://localhost:32769/devdb?useSSL=false', table='employee_details_dup_salary', mode='overwrite')

#
# Command
# ----------------
# export PYTHONPATH=$PYTHONPATH:.
#
# spark-submit --packages 'mysql:mysql-connector-java:8.0.13' --master 'local[*]' dataframe/provision/write_to_database/write_data_to_mysql.py
#
# Output
# ----------------
# +----------+---------+--------------+----------+---------+
# |first_name|last_name|      location|department|   salary|
# +----------+---------+--------------+----------+---------+
# |    Angela|   Mangam|     Żółkiewka|Accounting|$35416.88|
# |Willabella|  Sweeten|       Aborlan|Accounting|$21114.83|
# |  Ashleigh| Chezelle|         Jinhe|Accounting|$21114.83|
# |      Iain|  Dunnico|       Komenda|Accounting|$20268.08|
# |  Jeanette| Gallaway|     Bailizhou|Accounting| $3551.12|
# |    Michal|  Hampton| Dos Quebradas|Accounting|$29618.68|
# |  Sinclare|  Jillitt|   Ban Chalong|Accounting|$29618.68|
# |    Portia| Jephcote|    Kobiernice|Accounting|$14571.30|
# |      Zeke|   Testin|Telêmaco Borba|Accounting| $3582.71|
# |   Willard|Checketts|         Tantu|Accounting|$29997.88|
# +----------+---------+--------------+----------+---------+
# only showing top 10 rows
#
# root
#  |-- first_name: string (nullable = true)
#  |-- last_name: string (nullable = true)
#  |-- location: string (nullable = true)
#  |-- department: string (nullable = true)
#  |-- salary: double (nullable = true)
#
# +----------+---------+--------------+----------+--------+
# |first_name|last_name|      location|department|  salary|
# +----------+---------+--------------+----------+--------+
# |    Angela|   Mangam|     Żółkiewka|Accounting|35416.88|
# |Willabella|  Sweeten|       Aborlan|Accounting|21114.83|
# |  Ashleigh| Chezelle|         Jinhe|Accounting|21114.83|
# |      Iain|  Dunnico|       Komenda|Accounting|20268.08|
# |  Jeanette| Gallaway|     Bailizhou|Accounting| 3551.12|
# |    Michal|  Hampton| Dos Quebradas|Accounting|29618.68|
# |  Sinclare|  Jillitt|   Ban Chalong|Accounting|29618.68|
# |    Portia| Jephcote|    Kobiernice|Accounting| 14571.3|
# |      Zeke|   Testin|Telêmaco Borba|Accounting| 3582.71|
# |   Willard|Checketts|         Tantu|Accounting|29997.88|
# +----------+---------+--------------+----------+--------+
# only showing top 10 rows
#
# Writing data to my sql database...
#

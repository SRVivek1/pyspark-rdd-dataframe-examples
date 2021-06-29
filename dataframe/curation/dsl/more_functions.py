"""
This program demonstrates other functions from spark APIs.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    first, last, trim, lower, col, initcap,
    ltrim, format_string, coalesce, lit
)
from model.Person import Person


if __name__ == '__main__':
    """
    Driver program.
    """

    sparkSession = SparkSession\
        .builder\
        .appName('spark-api-functions-demo')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    print('\n**************************** SPARK pyspark.sql.functions method demo ****************************\n')
    # Create dataframe from person list.
    people_df = sparkSession.createDataFrame([
        Person("Sidhartha", "Ray", 32, None, "Programmer"),
        Person("Pratik", "Solanki", 22, 176.7, None),
        Person("Ashok ", "Pradhan", 62, None, None),
        Person(" ashok", "Pradhan", 42, 125.3, "Chemical Engineer"),
        Person("Pratik", "Solanki", 22, 222.2, "Teacher")
    ])

    print('\n**************** people_df.printSchema()')
    people_df.printSchema()

    print('\n**************** people_df.show(truncate=False)')
    people_df.show(truncate=False)

    # first(...) method.
    print('\n**************** people_df.groupBy(\'firstName\').agg(first(\'weightInLbs\')).show(truncate=False)')
    people_df\
        .groupBy('firstName')\
        .agg(first('weightInLbs'))\
        .show(truncate=False)

    # last(...) method
    print('\n**************** people_df.groupBy(\'firstName\').agg(last(\'weightInLbs\')).show(truncate=False)')
    people_df\
        .groupBy('firstName')\
        .agg(last('weightInLbs'))\
        .show(truncate=False)

    # lower(...) & trim(...) methods
    print("\n***************** people_df"
          ".groupBy(trim(lower(col('firstName')))).agg(first('weightInLbs')).show(truncate=False)")
    people_df\
        .groupBy(trim(lower(col('firstName'))))\
        .agg(first('weightInLbs'))\
        .show(truncate=False)

    # lower(...) & trim(...) methods
    print("\n***************** people_df"
          ".groupBy(trim(lower(col('firstName')))).agg(last('weightInLbs')).show(truncate=False)")
    people_df\
        .groupBy(trim(lower(col('firstName'))))\
        .agg(last('weightInLbs'))\
        .show(truncate=False)

    # last(...) ignore nulls
    print("\n***************** people_df"
          ".groupBy(trim(lower(col('firstName')))).agg(last('weightInLbs', True)).show(truncate=False)")
    people_df\
        .groupBy(trim(lower(col('firstName'))))\
        .agg(last('weightInLbs', True))\
        .show(truncate=False)

    # Sort weightInLbs column in desc order then find records
    print("\n************* people_df.sort(col('weightInLbs').desc())"
          ".groupBy(trim(lower(col('firstName')))).agg(first('weightInLbs', True)).show(truncate=False)")
    people_df\
        .sort(col('weightInLbs').desc())\
        .groupBy(trim(lower(col('firstName'))))\
        .agg(first('weightInLbs', True))\
        .show(truncate=False)

    # Sort weightInLbs column in asc order where null appear in last
    print("\n************** people_df.sort(col('weighInLbs').asc_nulls_last())"
          ".groupBy(trim(lower(col('firstName')))).agg(first(col('weightInLbs'))).show(truncate=False)")
    people_df.sort(col('weightInLbs').asc_nulls_last())\
        .groupBy(trim(lower(col('firstName'))))\
        .agg(first(col('weightInLbs')))\
        .show(truncate=False)

    # Data cleaning / correction - using withColumn(...), initcap(..), ltrim(...) & trim(...)
    print('\n************ Data cleaning / correction - using withColumn(...), initcap(..), ltrim(...) & trim(...)')

    corrected_people_df = people_df.withColumn('firstName', initcap('firstName'))
    print("\n************ corrected_people_df = people_df.withColumn('firstName', initcap('firstName'))")
    corrected_people_df.show(truncate=False)

    corrected_people_df = corrected_people_df.withColumn('firstName', ltrim(initcap('firstName')))
    print("\n************ corrected_people_df = "
          "corrected_people_df.withColumn('firstName', ltrim(initcap('firstName')))")
    corrected_people_df.show(truncate=False)

    corrected_people_df = corrected_people_df.withColumn('firstName', trim(initcap('firstName')))
    print("\n************ corrected_people_df = corrected_people_df.withColumn('firstName', trim(initcap('firstName')))")
    corrected_people_df.show(truncate=False)

    # Using format_string(...)
    corrected_people_df = corrected_people_df\
        .withColumn('fullName', format_string('%s %s', 'firstName', 'lastName'))

    print("\n************* corrected_people_df.withColumn('fullName', format_string('%s %s', 'firstName', 'lastName'))")
    corrected_people_df.show(truncate=False)

    # convert weightInLbs column to numeric values
    print("\n************* corrected_people_df.withColumn('weightInLbs', coalesce('WeightInLbs', lit(0)))")
    corrected_people_df = corrected_people_df\
        .withColumn('weightInLbs', coalesce('WeightInLbs', lit(0)))
    corrected_people_df.show(truncate=False)

    # Find people with job type as engineering. - contains(...)
    print("\n************ corrected_people_df.filter(lower(col('jobType')).contains('engineer')).show()")
    corrected_people_df\
        .filter(lower(col('jobType'))
                .contains('engineer'))\
        .show()

    # Search with list - isin(["..", "...", ...])
    print("\n************ corrected_people_df.filter(lower(col('jobType'))"
          ".isin(['chemical engineer', 'abc', 'teacher'])).show()")
    corrected_people_df\
        .filter(lower(col('jobType'))
                .isin(['chemical engineer', 'abc', 'teacher']))\
        .show()

    # Search without list - isin("..", "...", "...", ....)
    print("\n************ corrected_people_df"
          ".filter(lower(col('jobType')).isin('chemical engineer', 'abc', 'teacher')).show()")
    corrected_people_df\
        .filter(lower(col('jobType'))
                .isin('chemical engineer', 'abc', 'teacher'))\
        .show()

    # Exclusion - using tilda '~'
    print("\n************* Exclusion - using tilda '~'")
    print("\n************* corrected_people_df.filter(~lower(col('jobType'))"
          ".isin('chemical engineer', 'abc', 'teacher')).show()")
    corrected_people_df\
        .filter(~lower(col('jobType'))
                .isin('chemical engineer', 'abc', 'teacher'))\
        .show()

# Command
# -------------------
#   spark-submit dataframe/curation/dsl/more_functions.py
#
# Output
# -------------------
#
# **************************** SPARK pyspark.sql.functions method demo ****************************
#
#
# **************** people_df.printSchema()
# root
#  |-- age: long (nullable = true)
#  |-- firstName: string (nullable = true)
#  |-- jobType: string (nullable = true)
#  |-- lastName: string (nullable = true)
#  |-- weightInLbs: double (nullable = true)
#
#
# **************** people_df.show(truncate=False)
# +---+---------+-----------------+--------+-----------+
# |age|firstName|jobType          |lastName|weightInLbs|
# +---+---------+-----------------+--------+-----------+
# |32 |Sidhartha|Programmer       |Ray     |null       |
# |22 |Pratik   |null             |Solanki |176.7      |
# |62 |Ashok    |null             |Pradhan |null       |
# |42 | ashok   |Chemical Engineer|Pradhan |125.3      |
# |22 |Pratik   |Teacher          |Solanki |222.2      |
# +---+---------+-----------------+--------+-----------+
#
#
# **************** people_df.groupBy('firstName').agg(first('weightInLbs')).show(truncate=False)
# +---------+------------------+
# |firstName|first(weightInLbs)|
# +---------+------------------+
# |Ashok    |null              |
# |Sidhartha|null              |
# |Pratik   |176.7             |
# | ashok   |125.3             |
# +---------+------------------+
#
#
# **************** people_df.groupBy('firstName').agg(last('weightInLbs')).show(truncate=False)
# +---------+-----------------+
# |firstName|last(weightInLbs)|
# +---------+-----------------+
# |Ashok    |null             |
# |Sidhartha|null             |
# |Pratik   |222.2            |
# | ashok   |125.3            |
# +---------+-----------------+
#
#
# ***************** people_df.groupBy(trim(lower(col('firstName')))).agg(first('weightInLbs')).show(truncate=False)
# +----------------------+------------------+
# |trim(lower(firstName))|first(weightInLbs)|
# +----------------------+------------------+
# |sidhartha             |null              |
# |pratik                |176.7             |
# |ashok                 |null              |
# +----------------------+------------------+
#
#
# ***************** people_df.groupBy(trim(lower(col('firstName')))).agg(last('weightInLbs')).show(truncate=False)
# +----------------------+-----------------+
# |trim(lower(firstName))|last(weightInLbs)|
# +----------------------+-----------------+
# |sidhartha             |null             |
# |pratik                |222.2            |
# |ashok                 |125.3            |
# +----------------------+-----------------+
#
#
# ***************** people_df.groupBy(trim(lower(col('firstName')))).agg(last('weightInLbs', True)).show(truncate=False)
# +----------------------+-----------------+
# |trim(lower(firstName))|last(weightInLbs)|
# +----------------------+-----------------+
# |sidhartha             |null             |
# |pratik                |222.2            |
# |ashok                 |125.3            |
# +----------------------+-----------------+
#
#
# ************* people_df.sort(col('weightInLbs').desc()).groupBy(trim(lower(col('firstName')))).agg(first('weightInLbs', True)).show(truncate=False)
# +----------------------+------------------+
# |trim(lower(firstName))|first(weightInLbs)|
# +----------------------+------------------+
# |sidhartha             |null              |
# |pratik                |222.2             |
# |ashok                 |125.3             |
# +----------------------+------------------+
#
#
# ************** people_df.sort(col('weighInLbs').asc_nulls_last()).groupBy(trim(lower(col('firstName')))).agg(first(col('weightInLbs'))).show(truncate=False)
# +----------------------+------------------+
# |trim(lower(firstName))|first(weightInLbs)|
# +----------------------+------------------+
# |sidhartha             |null              |
# |pratik                |176.7             |
# |ashok                 |125.3             |
# +----------------------+------------------+
#
#
# ************ Data cleaning / correction - using withColumn(...), initcap(..), ltrim(...) & trim(...)
#
# ************ corrected_people_df = people_df.withColumn('firstName', initcap('firstName'))
# +---+---------+-----------------+--------+-----------+
# |age|firstName|jobType          |lastName|weightInLbs|
# +---+---------+-----------------+--------+-----------+
# |32 |Sidhartha|Programmer       |Ray     |null       |
# |22 |Pratik   |null             |Solanki |176.7      |
# |62 |Ashok    |null             |Pradhan |null       |
# |42 | Ashok   |Chemical Engineer|Pradhan |125.3      |
# |22 |Pratik   |Teacher          |Solanki |222.2      |
# +---+---------+-----------------+--------+-----------+
#
#
# ************ corrected_people_df = corrected_people_df.withColumn('firstName', ltrim(initcap('firstName')))
# +---+---------+-----------------+--------+-----------+
# |age|firstName|jobType          |lastName|weightInLbs|
# +---+---------+-----------------+--------+-----------+
# |32 |Sidhartha|Programmer       |Ray     |null       |
# |22 |Pratik   |null             |Solanki |176.7      |
# |62 |Ashok    |null             |Pradhan |null       |
# |42 |Ashok    |Chemical Engineer|Pradhan |125.3      |
# |22 |Pratik   |Teacher          |Solanki |222.2      |
# +---+---------+-----------------+--------+-----------+
#
#
# ************ corrected_people_df = corrected_people_df.withColumn('firstName', trim(initcap('firstName')))
# +---+---------+-----------------+--------+-----------+
# |age|firstName|jobType          |lastName|weightInLbs|
# +---+---------+-----------------+--------+-----------+
# |32 |Sidhartha|Programmer       |Ray     |null       |
# |22 |Pratik   |null             |Solanki |176.7      |
# |62 |Ashok    |null             |Pradhan |null       |
# |42 |Ashok    |Chemical Engineer|Pradhan |125.3      |
# |22 |Pratik   |Teacher          |Solanki |222.2      |
# +---+---------+-----------------+--------+-----------+
#
#
# ************* corrected_people_df.withColumn('fullName', format_string('%s %s', 'firstName', 'lastName'))
# +---+---------+-----------------+--------+-----------+--------------+
# |age|firstName|jobType          |lastName|weightInLbs|fullName      |
# +---+---------+-----------------+--------+-----------+--------------+
# |32 |Sidhartha|Programmer       |Ray     |null       |Sidhartha Ray |
# |22 |Pratik   |null             |Solanki |176.7      |Pratik Solanki|
# |62 |Ashok    |null             |Pradhan |null       |Ashok Pradhan |
# |42 |Ashok    |Chemical Engineer|Pradhan |125.3      |Ashok Pradhan |
# |22 |Pratik   |Teacher          |Solanki |222.2      |Pratik Solanki|
# +---+---------+-----------------+--------+-----------+--------------+
#
#
# ************* corrected_people_df.withColumn('weightInLbs', coalesce('WeightInLbs', lit(0)))
# +---+---------+-----------------+--------+-----------+--------------+
# |age|firstName|jobType          |lastName|weightInLbs|fullName      |
# +---+---------+-----------------+--------+-----------+--------------+
# |32 |Sidhartha|Programmer       |Ray     |0.0        |Sidhartha Ray |
# |22 |Pratik   |null             |Solanki |176.7      |Pratik Solanki|
# |62 |Ashok    |null             |Pradhan |0.0        |Ashok Pradhan |
# |42 |Ashok    |Chemical Engineer|Pradhan |125.3      |Ashok Pradhan |
# |22 |Pratik   |Teacher          |Solanki |222.2      |Pratik Solanki|
# +---+---------+-----------------+--------+-----------+--------------+
#
#
# ************ corrected_people_df.filter(lower(col('jobType')).contains('engineer')).show()
# +---+---------+-----------------+--------+-----------+-------------+
# |age|firstName|          jobType|lastName|weightInLbs|     fullName|
# +---+---------+-----------------+--------+-----------+-------------+
# | 42|    Ashok|Chemical Engineer| Pradhan|      125.3|Ashok Pradhan|
# +---+---------+-----------------+--------+-----------+-------------+
#
#
# ************ corrected_people_df.filter(lower(col('jobType')).isin(['chemical engineer', 'abc', 'teacher'])).show()
# +---+---------+-----------------+--------+-----------+--------------+
# |age|firstName|          jobType|lastName|weightInLbs|      fullName|
# +---+---------+-----------------+--------+-----------+--------------+
# | 42|    Ashok|Chemical Engineer| Pradhan|      125.3| Ashok Pradhan|
# | 22|   Pratik|          Teacher| Solanki|      222.2|Pratik Solanki|
# +---+---------+-----------------+--------+-----------+--------------+
#
#
# ************ corrected_people_df.filter(lower(col('jobType')).isin('chemical engineer', 'abc', 'teacher')).show()
# +---+---------+-----------------+--------+-----------+--------------+
# |age|firstName|          jobType|lastName|weightInLbs|      fullName|
# +---+---------+-----------------+--------+-----------+--------------+
# | 42|    Ashok|Chemical Engineer| Pradhan|      125.3| Ashok Pradhan|
# | 22|   Pratik|          Teacher| Solanki|      222.2|Pratik Solanki|
# +---+---------+-----------------+--------+-----------+--------------+
#
#
# ************* Exclusion - using tilda '~'
#
# ************* corrected_people_df.filter(~lower(col('jobType')).isin('chemical engineer', 'abc', 'teacher')).show()
# +---+---------+----------+--------+-----------+-------------+
# |age|firstName|   jobType|lastName|weightInLbs|     fullName|
# +---+---------+----------+--------+-----------+-------------+
# | 32|Sidhartha|Programmer|     Ray|        0.0|Sidhartha Ray|
# +---+---------+----------+--------+-----------+-------------+

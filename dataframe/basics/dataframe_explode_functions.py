"""
This program demonstrates the use of different explode functions available in 'pyspark.sql.functions' API.
"""

from pyspark.sql import (
    SparkSession,
    Row
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    ArrayType,
    MapType,
    StringType
)
from pyspark.sql.functions import (
    explode,
    posexplode,
    explode_outer,
    posexplode_outer
)

if __name__ == '__main__':
    """
    Driver program.
    """

    sparkSession = SparkSession\
        .builder\
        .appName('dataframe-explode-functions-demo')\
        .master('local[*]')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    print('\n*************************** DataFrame select function ***************************\n')

    # Define schema for dataframe
    schema = StructType([
        StructField('col', IntegerType(), False),
        StructField('inlist', ArrayType(IntegerType(), containsNull=True), False),
        StructField('mapField', MapType(StringType(), StringType(), valueContainsNull=True), False)
    ])

    # Create dataframe
    data = [Row(a=1,
                inlist=[None, 1, 2, 3, None],
                mapField={'a': 'apple', 'b': 'ball', 'c': None})]

    test_df = sparkSession.createDataFrame(data, schema)

    # Print dataframe schema and sample data
    print('\n**************** : test_df.printSchema()')
    test_df.printSchema()

    print('\n**************** : test_df.show(truncate=False)')
    test_df.show(truncate=False)

    # Explode data - using explode(...) function
    print('\n**************** explode(...) List : test_df.select(explode(test_df.inlist)).show(truncate=False)')
    test_df.select(explode(test_df.inlist).alias('exploded_col_inlist')).show(truncate=False)

    print('\n**************** explode(...) Map : test_df.select(explode(test_df.mapField)).show(truncate=False)')
    test_df.select(explode(test_df.mapField)).show(truncate=False)

    # Explode data with their position in their container array/map
    print('\n**************** posexplode(...) List : test_df.select(posexplode(\'inlist\')).show(truncate=False)')
    test_df.select(posexplode('inlist')).show(truncate=False)

    print('\n**************** posexplode(...) Map : test_df.select(posexplode(\'mapField\')).show(truncate=False)')
    test_df.select(posexplode('mapField')).show(truncate=False)

    # Explode data - show null & empty fields
    print('\n**************** explode_outer(...) List : test_df.select(explode_outer(\'inList\')).show(truncate=False)')
    test_df.select(explode_outer('inList')).show(truncate=False)

    print('\n**************** explode_outer(...) Map : test_df.select(explode_outer(\'mapField\')).show(truncate=False)')
    test_df.select(explode_outer('mapField')).show(truncate=False)

    # Explode data - show null & empty fields with position
    print('\n**************** posexplode_outer(...) List : test_df.select(explode_outer(\'inList\')).show(truncate=False)')
    test_df.select(posexplode_outer('inList')).show(truncate=False)

    print('\n**************** posexplode_outer(...) Map : test_df.select(explode_outer(\'mapField\')).show(truncate=False)')
    test_df.select(posexplode_outer('mapField')).show(truncate=False)

# Command
# -------------------
# spark-submit dataframe/basics/dataframe_explode_functions.py
#
# Output
# -------------------
#
# *************************** DataFrame select function ***************************
#
#
# **************** : test_df.printSchema()
# root
#  |-- col: integer (nullable = false)
#  |-- inlist: array (nullable = false)
#  |    |-- element: integer (containsNull = true)
#  |-- mapField: map (nullable = false)
#  |    |-- key: string
#  |    |-- value: string (valueContainsNull = true)
#
#
# **************** : test_df.show(truncate=False)
# +---+---------------------+----------------------------------+
# |col|inlist               |mapField                          |
# +---+---------------------+----------------------------------+
# |1  |[null, 1, 2, 3, null]|{a -> apple, b -> ball, c -> null}|
# +---+---------------------+----------------------------------+
#
#
# **************** explode(...) List : test_df.select(explode(test_df.inlist)).show(truncate=False)
# +-------------------+
# |exploded_col_inlist|
# +-------------------+
# |null               |
# |1                  |
# |2                  |
# |3                  |
# |null               |
# +-------------------+
#
#
# **************** explode(...) Map : test_df.select(explode(test_df.mapField)).show(truncate=False)
# +---+-----+
# |key|value|
# +---+-----+
# |a  |apple|
# |b  |ball |
# |c  |null |
# +---+-----+
#
#
# **************** posexplode(...) List : test_df.select(posexplode('inlist')).show(truncate=False)
# +---+----+
# |pos|col |
# +---+----+
# |0  |null|
# |1  |1   |
# |2  |2   |
# |3  |3   |
# |4  |null|
# +---+----+
#
#
# **************** posexplode(...) Map : test_df.select(posexplode('mapField')).show(truncate=False)
# +---+---+-----+
# |pos|key|value|
# +---+---+-----+
# |0  |a  |apple|
# |1  |b  |ball |
# |2  |c  |null |
# +---+---+-----+
#
#
# **************** explode_outer(...) List : test_df.select(explode_outer('inList')).show(truncate=False)
# +----+
# |col |
# +----+
# |null|
# |1   |
# |2   |
# |3   |
# |null|
# +----+
#
#
# **************** explode_outer(...) Map : test_df.select(explode_outer('mapField')).show(truncate=False)
# +---+-----+
# |key|value|
# +---+-----+
# |a  |apple|
# |b  |ball |
# |c  |null |
# +---+-----+
#
#
# **************** posexplode_outer(...) List : test_df.select(explode_outer('inList')).show(truncate=False)
# +---+----+
# |pos|col |
# +---+----+
# |0  |null|
# |1  |1   |
# |2  |2   |
# |3  |3   |
# |4  |null|
# +---+----+
#
#
# **************** posexplode_outer(...) Map : test_df.select(explode_outer('mapField')).show(truncate=False)
# +---+---+-----+
# |pos|key|value|
# +---+---+-----+
# |0  |a  |apple|
# |1  |b  |ball |
# |2  |c  |null |
# +---+---+-----+

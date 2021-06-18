"""
This program demonstrates the use of different explode functions available in 'pyspark.sql.functions' API.
"""

from pyspark.sql import (
    SparkSession,
    Row
)
from pyspark.sql.functions import explode, posexplode, explode_outer, posexplode_outer


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

    # Create dataframe
    test_df = sparkSession.createDataFrame([Row(a=1, inlist=[1, 2, 3], mapField={'a': 'apple', 'b': 'ball'})])

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

    print('\n**************** posexplode(...) List : test_df.select(posexplode(\'inlist\')).show(truncate=False)')
    test_df.select(posexplode('inlist')).show(truncate=False)

    print('\n**************** posexplode(...) Map : test_df.select(posexplode(\'mapField\')).show(truncate=False)')
    test_df.select(posexplode('mapField')).show(truncate=False)

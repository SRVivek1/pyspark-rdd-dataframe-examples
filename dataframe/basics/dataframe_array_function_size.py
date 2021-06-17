"""
This program demonstrates the array size(...) function.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == '__main__':
    """
    Driver program.
    """

    sparkSession = SparkSession\
        .builder\
        .appName('demo-array-fun-size')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    # DataFrame with 3 elements
    df = sparkSession.createDataFrame([([1, 2, 3],), ([1],), ([],)], ['data'])

    print('\n***************** DataFrame data :')
    df.show(5, False)

    # Find size of elements in each record
    print('\n***************** df.select(size(df.data)).show(5, False)')
    df.select(size(df.data)).show(5, False)

# Command
# -----------------------
# spark-submit dataframe/basics/dataframe_array_function_size.py
#
# Output
# -----------------------
#
# ***************** DataFrame data :
# +---------+
# |data     |
# +---------+
# |[1, 2, 3]|
# |[1]      |
# |[]       |
# +---------+
#
#
# ***************** df.select(size(df.data)).show(5, False)
# +----------+
# |size(data)|
# +----------+
# |3         |
# |1         |
# |0         |
# +----------+

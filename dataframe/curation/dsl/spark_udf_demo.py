"""
This program demonstrate different ways of creating/registering UDFs with spark.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


if __name__ == '__main__':
    """
    Driver program.
    """

    spark = SparkSession\
        .builder\
        .appName('spark-udf-demo')\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Define a function to capitalize all the words in a sentence.
    def initcap(line: str) -> str:
        lst = line.split(' ')
        return ' '.join(list(map(str.capitalize, lst)))

    # Register UDF - Method 1
    initcap_udf_1 = spark.udf.register('initicap', initcap, StringType())

    # Register UDF - Method 2
    initcap_udf_2 = udf(initcap, StringType())

    # Register UDF - Method 3 - Inline method declaration
    initcap_udf_3 = spark.udf\
        .register('initicap',
                  lambda line: ' '.join(list(map(str.capitalize, line.split(' ')))),
                  StringType())
    # Register UDF - Method 4 - Inline method declaration
    initcap_udf_4 = spark.udf\
        .register('initicap',
                  lambda line, delimiter: ' '.join(list(map(str.capitalize, line.split(delimiter)))),
                  StringType())

    initcap_udf_5 = udf(lambda line, delimiter: ' '.join(list(map(str.capitalize, line.split(delimiter)))),
                        StringType())

    # Create a simple Dataframe
    sample_df = spark.createDataFrame(
        [(1, 'some data'),
         (2, 'some more data'),
         (3, 'even more data')
         ]
    ).toDF('id', 'text')

    # Using UDF functions
    sample_df.select('id',
                     initcap_udf_1('text').alias('udf_1'),
                     initcap_udf_1('text').alias('udf_2'),
                     initcap_udf_1('text').alias('udf_3'),
                     initcap_udf_1('text').alias('udf_4'),
                     initcap_udf_1('text').alias('udf_5')
                     )\
        .show(5, False)

# Command
# -------------------
# spark-submit dataframe/curation/dsl/spark_udf_demo.py
#
# Output
# -------------------
# +---+--------------+--------------+--------------+--------------+--------------+
# |id |udf_1         |udf_2         |udf_3         |udf_4         |udf_5         |
# +---+--------------+--------------+--------------+--------------+--------------+
# |1  |Some Data     |Some Data     |Some Data     |Some Data     |Some Data     |
# |2  |Some More Data|Some More Data|Some More Data|Some More Data|Some More Data|
# |3  |Even More Data|Even More Data|Even More Data|Even More Data|Even More Data|
# +---+--------------+--------------+--------------+--------------+--------------+

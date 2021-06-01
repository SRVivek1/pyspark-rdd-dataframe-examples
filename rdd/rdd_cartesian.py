"""
This program demonstrates the functionality of 'cartesian()' method.

Definition
----------------
    RDD.cartesian(other)
        Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs
        of elements (a, b) where a is in self and b is in other.

        # >>> rdd = sc.parallelize([1, 2])
        # >>> sorted(rdd.cartesian(rdd).collect())
            -> [(1, 1), (1, 2), (2, 1), (2, 2)]
"""


from pyspark.sql import SparkSession


if __name__ == '__main__':
    sparkSession = SparkSession \
        .builder \
        .appName('rdd-cartesian-demo') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext

    sparkContext.setLogLevel("ERROR")

    num_rdd = sparkContext.parallelize([1, 2])
    char_rdd = sparkContext.parallelize(['a', 'b'])

    # print source data
    print('\n********* num_rdd : {0}'.format(num_rdd.collect()))
    print('\n********* char_rdd : {0}'.format(char_rdd.collect()))

    # Calculate cartesian product
    num_cartesian_result = num_rdd.cartesian(char_rdd)

    char_cartesian_result = char_rdd.cartesian(num_rdd)

    # Print results
    print('\n********** num_rdd.cartesian(char_rdd)')
    num_cartesian_result.foreach(print)

    print('\n********** char_rdd.cartesian(num_rdd)')
    char_cartesian_result.foreach(print)

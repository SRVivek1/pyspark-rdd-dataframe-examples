"""
This program demonstrates the function of rdd join method

From Documentation:
    pyspark.RDD.join
    RDD.join(other, numPartitions=None)[source]
    Return an RDD containing all pairs of elements with matching keys in self and other.

    Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in self and (k, v2) is in other.

    Performs a hash join across the cluster.

    Example
        # >>> x = sc.parallelize([("a", 1), ("b", 4)])
        # >>> y = sc.parallelize([("a", 2), ("a", 3)])
        # >>> sorted(x.join(y).collect())
            [('a', (1, 2)), ('a', (1, 3))]
"""


from pyspark.sql import SparkSession


if __name__ == '__main__':
    sparkSession = SparkSession \
        .builder \
        .appName('rdd-join-app') \
        .getOrCreate()

    sparkContext = sparkSession.sparkContext

    rdd_1 = sparkContext.parallelize([('a', 1), ('b', 2)])
    rdd_2 = sparkContext.parallelize([('a', 11), ('a', 12)])

    print('********** RDD 1 : {0}'.format(rdd_1.take(2)))
    print('********** RDD 2 : {0}'.format(rdd_2.take(2)))
    # Perform join on rdd_1 to rdd_2
    joined_rdd = rdd_1.join(rdd_2)

    # Print RDD content
    print('********** joined_rdd : {0}'.format(joined_rdd.collect()))

    print('********** Example - RDD with more then 2 values')
    rdd_3 = sparkContext.parallelize([('a', 1, 'apple'), ('b', 2, 'banana'), ('m', 3, 'mango')])
    rdd_4 = sparkContext.parallelize([('a', 101, 'ant'), ('b', 102, 'burro'), ('m', 103, 'monkey')])

    print('********** RDD 3 : {0}'.format(rdd_3.take(3)))
    print('********** RDD 4 : {0}'.format(rdd_4.take(3)))

    joined_rdd_2 = rdd_3.join(rdd_4)

    # Print RDD content
    print('********** joined_rdd_2 : {0}'.format(joined_rdd_2.collect()))

    # Trying with different set of data
    rdd_5 = sparkContext.parallelize([('a', 'ant', 101), ('b', 'burro', 102), ('m', 'monkey', 103)])

    print('*********** RDD 3 : {0}'.format(rdd_3))
    print('*********** RDD 5 : {0}'.format(rdd_5))

    joined_rdd_3 = rdd_3.join(rdd_5)

    print('*********** joined_rdd_3 : {0}'.format(joined_rdd_3))

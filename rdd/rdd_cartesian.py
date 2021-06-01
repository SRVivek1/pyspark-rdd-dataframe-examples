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
from distutils.util import strtobool

if __name__ == '__main__':
    sparkSession = SparkSession \
        .builder \
        .master('local[1]') \
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

    num_rdd_2 = sparkContext.parallelize([1, 2, 3])
    char_rdd_2 = sparkContext.parallelize(['a', 'b', 'c', 'd'])

    # print source data
    print('\n********* num_rdd_2 : {0}'.format(num_rdd_2.collect()))
    print('\n********* char_rdd_2 : {0}'.format(char_rdd_2.collect()))

    # Calculate cartesian product
    num_cartesian_result_2 = num_rdd_2.cartesian(char_rdd_2)

    char_cartesian_result_2 = char_rdd_2.cartesian(num_rdd_2)

    # Print results
    print('\n********** num_rdd_2.cartesian(char_rdd_2) - sorted result')
    num_cartesian_result_2.sortByKey().foreach(print)

    print('\n********** char_rdd_2.cartesian(num_rdd_2) - sorted result')
    char_cartesian_result_2.sortByKey().foreach(print)

    # Cartesian product on a pair rdd
    demographic_rdd = sparkContext.parallelize([[101, 'vivek', 'vivek@test.com'], [102, 'Rohit', 'rohit@test.com']])
    finance_rdd = sparkContext.parallelize([[101, 100000, 'icici', 'false'], [102, 50000, 'hdfc', 'true']])

    # print source data
    print('\n********** Demographic data : {0}'.format(demographic_rdd.collect()))
    print('\n********** Finance data : {0}'.format(finance_rdd.collect()))

    # Create paired rdd
    demographic_pair_rdd = demographic_rdd \
        .map(lambda lst: (lst[0], (lst[1], lst[2])))

    finance_pair_rdd = finance_rdd \
        .map(lambda lst: (lst[0], (lst[1], lst[2], strtobool(lst[3]))))

    # print paired RDD
    print('\n*********** Demographic paired RDD')
    demographic_pair_rdd.foreach(print)

    print('\n*********** Finance paired RDD')
    finance_pair_rdd.foreach(print)

    # Perform Cartesian product
    cartesian_result = demographic_pair_rdd.cartesian(finance_pair_rdd)

    print('\n*********** demographic_pair_rdd.cartesian(finance_pair_rdd)')
    cartesian_result.foreach(print)


# Command
#   spark-submit rdd/rdd_cartesian.py


# Result
#
#  ********* num_rdd : [1, 2]
#
# ********* char_rdd : ['a', 'b']
#
# ********** num_rdd.cartesian(char_rdd)
# (1, 'a')
# (1, 'b')
# (2, 'a')
# (2, 'b')
#
# ********** char_rdd.cartesian(num_rdd)
# ('a', 1)
# ('a', 2)
# ('b', 2)
# ('b', 1)

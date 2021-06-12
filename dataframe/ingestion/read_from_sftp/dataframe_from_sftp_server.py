"""
This program demonstrates how to read data from SFTP servers.
"""


from pyspark.sql import SparkSession
import os.path
import yaml


if __name__ == '__main__':
    print('\n******************************* Read data from SFTP *******************************')

    sparkSession = SparkSession\
        .builder\
        .appName('Read data from SFTP server')\
        .config('spark.jars.packages', 'com.springml:spark-sftp_2.11:1.1.1')\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    # Start : Read configuration
    current_dir = os.path.abspath(os.path.dirname(__file__))
    project_root_dir = os.path.abspath(current_dir + '../../../..')

    app_config_file = os.path.abspath(project_root_dir + '/application.yml')
    app_secrets_file = os.path.abspath(project_root_dir + '/.secrets')

    config_file = open(app_config_file)
    app_config = yaml.load(config_file, Loader=yaml.FullLoader)

    secrets_file = open(app_secrets_file)
    app_secrets = yaml.load(secrets_file, Loader=yaml.FullLoader)
    # End : Read configuration

    # Start : SFTP configuration
    sftp_connection_config = {
        'host': app_secrets['sftp_conf']['hostname'],
        'port': app_secrets['sftp_conf']['port'],
        'username': app_secrets['sftp_conf']['username'],
        'pem': os.path.abspath(project_root_dir + '/' + app_secrets['sftp_conf']['pem'])
    }
    # End : SFTP configuration

    # Start : File information
    sftp_file_details = {
        'filetype': app_config['sftp_conf']['filetype'],
        'delimiter': app_config['sftp_conf']['delimiter']
    }
    # End : File information

    # Start : Read data from SFTP
    '''receipts_df = sparkSession.read\
        .format('com.springml.spark.sftp')\
        .options(**sftp_connection_config)\
        .options(**sftp_file_details)\
        .load(app_config['sftp_conf']['directory'] + '/receipts_delta_GBR_14_10_2017.csv')'''

    receipts_df = sparkSession.read\
        .format("com.springml.spark.sftp")\
        .option("host", 'ec2-34-247-32-96.eu-west-1.compute.amazonaws.com')\
        .option("port", '22')\
        .option("username", 'ubuntu')\
        .option("pem", '/home/viveksingh/spark-projects/rdd-dataframe-examples/ec2-pem-1.pem')\
        .option("fileType", "csv")\
        .option("delimiter", "|")\
        .load('/home/ubuntu/sftp/receipts_delta_GBR_14_10_2017.csv')
    # End : Read data from SFTP

    # Show data
    print('\n********************** Sample data read from SFTP : receipts_df.show(10, truncate=False)')
    receipts_df.show(10, truncate=False)

# Command
# ---------------
# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1,org.scala-lang:scala-library:2.11.11,org.scala-lang:scala-compiler:2.11.11,org.scala-lang:scala-reflect:2.11.11" dataframe/ingestion/read_from_sftp/dataframe_from_sftp_server.py
#
#
# Output
# ------------------
# Check EC2 program, local one getting error
# 21/06/11 09:41:33 INFO SharedState: Warehouse path is 'file:/home/viveksingh/spark-projects/rdd-dataframe-examples/spark-warehouse'.
# Traceback (most recent call last):
#   File "/home/viveksingh/spark-projects/rdd-dataframe-examples/dataframe/ingestion/read_from_sftp/dataframe_from_sftp_server.py", line 59, in <module>
#     receipts_df = sparkSession.read\
#   File "/home/viveksingh/spark-tools/spark/python/lib/pyspark.zip/pyspark/sql/readwriter.py", line 204, in load
#   File "/home/viveksingh/spark-tools/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py", line 1304, in __call__
#   File "/home/viveksingh/spark-tools/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 111, in deco
#   File "/home/viveksingh/spark-tools/spark/python/lib/py4j-0.10.9-src.zip/py4j/protocol.py", line 326, in get_return_value
# py4j.protocol.Py4JJavaError: An error occurred while calling o42.load.
# : java.lang.NoClassDefFoundError: scala/Product$class
# 	at com.springml.spark.sftp.DatasetRelation.<init>(DatasetRelation.scala:20)
# 	at com.springml.spark.sftp.DefaultSource.createRelation(DefaultSource.scala:83)
# 	at com.springml.spark.sftp.DefaultSource.createRelation(DefaultSource.scala:38)
# 	at org.apache.spark.sql.execution.datasources.DataSource.resolveRelation(DataSource.scala:354)
# 	at org.apache.spark.sql.DataFrameReader.loadV1Source(DataFrameReader.scala:326)
# 	at org.apache.spark.sql.DataFrameReader.$anonfun$load$3(DataFrameReader.scala:308)
# 	at scala.Option.getOrElse(Option.scala:189)
# 	at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:308)
# 	at org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:240)
# 	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
# 	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
# 	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
# 	at java.lang.reflect.Method.invoke(Method.java:498)
# 	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
# 	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
# 	at py4j.Gateway.invoke(Gateway.java:282)
# 	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
# 	at py4j.commands.CallCommand.execute(CallCommand.java:79)
# 	at py4j.GatewayConnection.run(GatewayConnection.java:238)
# 	at java.lang.Thread.run(Thread.java:748)
# Caused by: java.lang.ClassNotFoundException: scala.Product$class
# 	at java.net.URLClassLoader.findClass(URLClassLoader.java:382)
# 	at java.lang.ClassLoader.loadClass(ClassLoader.java:418)
# 	at java.lang.ClassLoader.loadClass(ClassLoader.java:351)
# 	... 20 more

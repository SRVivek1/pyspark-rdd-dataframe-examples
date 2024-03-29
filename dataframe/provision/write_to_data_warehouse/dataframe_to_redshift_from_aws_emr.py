"""
This program demonstrates how to provision data to AWS Redshift data warehouse.
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit
import os.path
import yaml

current_dir = os.path.abspath(os.path.dirname(__file__))
app_config_file = os.path.abspath(current_dir + '../../../..' + '/application.yml')
app_secret_file = os.path.abspath(current_dir + '../../../..' + '/.secrets')

app_config = yaml.load(open(app_config_file), Loader=yaml.FullLoader)
app_secret = yaml.load(open(app_secret_file), Loader=yaml.FullLoader)


def get_redshift_jdbc_url() -> str:
    """
        Construct RedShift JDBC URL.
            format: jdbc:redshift://$hostname:$port/$database?user=$username&password=$password

    :return: string
    """
    redshift_conf = app_secret['redshift_conf']
    host = redshift_conf['host']
    port = redshift_conf['port']
    database = redshift_conf['database']
    username = redshift_conf['username']
    password = redshift_conf['password']

    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


if __name__ == '__main__':
    """
        Driver program
    """

    # Environment variable config
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"\
         --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    # Create spark session
    sparkSession = SparkSession\
        .builder\
        .appName('Provision data to AWS RedShift')\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('ERROR')

    # Setup Spark to use S3 - for temp location
    hadoopConfig = sparkSession.sparkContext._jsc.hadoopConfiguration()
    hadoopConfig.set('fs.s3a.access.key', app_secret['s3_conf']['access_key'])
    hadoopConfig.set('fs.s3a.secret.key', app_secret['s3_conf']['secret_access_key'])

    # Get Redshift JDBC URL
    redshift_jdbc_url = get_redshift_jdbc_url()
    print('\n************** RedShift - JDBC URL : ' + redshift_jdbc_url)

    # Read data from AWS RedShift
    txn_df = sparkSession\
        .read\
        .format('io.github.spark_redshift_community.spark.redshift')\
        .option('url', redshift_jdbc_url)\
        .option('query', app_config['redshift_conf']['query'])\
        .option('forward_spark_s3_credentials', 'true')\
        .option('tempdir', 's3a://' + app_config['s3_conf']['s3_bucket'] + '/temp')\
        .load()

    # Show sample records
    print('\n*********************** Data read from RedShift')
    txn_df.show(5, False)

    # Update data
    txn_df_updated = txn_df.withColumn('ingestion_date', lit(current_date().cast('string')))

    # Show sample records
    print('\n*********************** Data writing to RedShift')
    txn_df_updated.show(5, False)

    # Write data to AWS RedShift
    # RedShift connections are costly/Limited, so reducing DF partition to 1
    # This will result in creating 1 connection with Redshift to write data.
    txn_df_updated \
        .coalesce(1)\
        .write\
        .format('io.github.spark_redshift_community.spark.redshift')\
        .option('url', redshift_jdbc_url)\
        .option('forward_spark_s3_credentials', 'true') \
        .option('tempdir', 's3a://' + app_config['s3_conf']['s3_bucket'] + '/temp')\
        .option('dbtable', app_config['redshift_conf']['newdbtable'])\
        .mode('overwrite')\
        .save()

    print('\n*********************** Data provision Completed')

# Commands
# ----------------------
# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/read_from_data_warehouse/dataframe_from_redshift_from_aws_emr.py
#
# Output
# ----------------------
# ************** RedShift - JDBC URL : jdbc:redshift://rs-cluster-test1.cqehfwirgng7.eu-west-1.redshift.amazonaws.com:5439/dev?user=master&password=Passw0rd
#
# *********************** Data read from RedShift
# +----------+-----------+------+-----------+
# |txn_id    |create_time|amount|cust_id    |
# +----------+-----------+------+-----------+
# |-785038820|20190101   |292   |-1433967778|
# |-785006564|20190101   |199   |-1433965090|
# |-784974308|20190101   |885   |-1433962402|
# |-784942052|20190101   |119   |-1433959714|
# |-784909796|20190101   |169   |-1433957026|
# +----------+-----------+------+-----------+
# only showing top 5 rows
#
#
# *********************** Data writing to RedShift
# +----------+-----------+------+-----------+--------------+
# |txn_id    |create_time|amount|cust_id    |ingestion_date|
# +----------+-----------+------+-----------+--------------+
# |-785038820|20190101   |292   |-1433967778|2021-06-14    |
# |-785006564|20190101   |199   |-1433965090|2021-06-14    |
# |-784974308|20190101   |885   |-1433962402|2021-06-14    |
# |-784942052|20190101   |119   |-1433959714|2021-06-14    |
# |-784909796|20190101   |169   |-1433957026|2021-06-14    |
# +----------+-----------+------+-----------+--------------+
# only showing top 5 rows

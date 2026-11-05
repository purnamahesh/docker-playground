from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics

def getThroughputRate(metrics: dict):
    elapsed_seconds = metrics['elapsedTime'] / 1000.0

    read_rps = metrics['recordsRead'] / elapsed_seconds
    write_rps = metrics['recordsWritten'] / elapsed_seconds

    print(f"\n\nRead: {read_rps:,.2f} records/sec")
    print(f"Write: {write_rps:,.2f} records/sec\n\n")

def createSparkSession(appName: str):
    jars_path = '/home/ubuntu/work_dir/jars/'
    jars = (
        'postgresql-42.7.13.jar',
        'hadoop-aws-3.5.0.jar',
        'analyticsaccelerator-s3-1.3.1.jar',
        'aws-java-sdk-bundle-2.55.16.jar',
        'delta-spark_2.13-4.4.1.jar',
        'spark-measure_2.13-0.28.jar'
    )
    spark_jars = ','.join(jars_path + jar for jar in jars)

    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .config('spark.eventLog.enabled','true')
        .config('spark.eventLog.dir','/tmp/spark-events')
        .config('spark.jars',spark_jars)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://rustfs:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin") \
        .appName(appName)
        .getOrCreate()
    )
    stagemetrics = StageMetrics(spark)

    return spark, stagemetrics

def read_csv_from_s3(spark, source_path: str, corrupt_record_name: str = 'bad_record', options: dict = {}, schema = None, header=True):
    return (
        spark.read
        .option('columnNameOfCorruptRecord', corrupt_record_name)
        .options(**options)
        .csv(source_path, header=header, schema=schema)
    )

def write_postgres_table(df, table_name: str, mode: str = 'overwrite', options: dict = {}):
    df.write \
    .format('jdbc') \
    .option("url", "jdbc:postgresql://instacart:5432/instacart") \
    .option("dbtable", table_name) \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .options(**options) \
    .mode(mode) \
    .save()
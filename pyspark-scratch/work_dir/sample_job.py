from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

df = spark.range(1000)
df.show()

spark.stop()

# docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client work_dir/sample_job.py 
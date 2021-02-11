from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
static = spark.read.json("activity-data/")
dataSchema = static.schema
streaming =spark.readStream.schema(dataSchema).option("maxFilesPerTrigger",
    1).json("activity-data")

from pyspark.sql.functions import expr

simpleTransform = streaming.withColumn("bike", expr("gt like '%bike%'"))\
    .filter("bike")\
    .filter("gt is not null")\
    .select("gt", "model", "arrival_time", "creation_time","device")\
    .writeStream\
    .queryName("simple_transform")\
    .format("memory")\
    .outputMode("append")\
    .start()


from time import sleep

for x in range(20):
    spark.sql("SELECT * FROM simple_Transform").show(1000)
    sleep(15)

activityQuery.awaitTermination()

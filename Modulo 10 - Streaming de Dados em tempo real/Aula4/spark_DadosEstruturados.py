from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
static = spark.read.json("activity-data/")
dataSchema = static.schema
streaming =spark.readStream.schema(dataSchema).option("maxFilesPerTrigger",
    1).json("activity-data")

activityCounts = streaming.groupBy("gt").count()
spark.conf.set("spark.sql.shuffle.partitions", 5)

activityQuery =activityCounts.writeStream.queryName("activity_counts").format("memory").outputMode("complete").start()


from time import sleep

for x in range(10):
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(5)

activityQuery.awaitTermination()

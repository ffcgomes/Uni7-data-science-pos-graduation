from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
static = spark.read.json("activity-data/")
dataSchema = static.schema
streaming =spark.readStream.schema(dataSchema).option("maxFilesPerTrigger",
    1).json("activity-data")

from pyspark.sql.functions import expr

deviceModelStats = streaming.cube("gt", "model").avg()\
.drop("avg(Arrival_time)")\
.drop("avg(Creation_Time)")\
.drop("avg(Index)")\
.writeStream.queryName("device_counts").format("memory")\
.outputMode("complete")\
.start()


from time import sleep

for x in range(20):
    spark.sql("SELECT * FROM device_counts").show(1000)
    sleep(15)

activityQuery.awaitTermination()

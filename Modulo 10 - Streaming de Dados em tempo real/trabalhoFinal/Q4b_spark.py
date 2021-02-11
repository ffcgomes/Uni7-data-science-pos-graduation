from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.option("multiline","true").json("file:///home/flavio/Disc10/trabalhoFinal/ans.json")


df.groupBy('municipio_ibge').sum('valor').show(truncate=False)


spark.stop()
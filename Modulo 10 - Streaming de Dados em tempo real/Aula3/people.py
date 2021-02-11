from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.json("file:////home/flavio/Disc10/Aula3/people.json")
df.filter(df['age']>21).show()
#df.show()
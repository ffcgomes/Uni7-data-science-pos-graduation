from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.option("multiline","true").json("file:///home/flavio/Disc10/trabalhoFinal/ans.json")

df_mun = df.groupBy('municipio_ibge').sum('valor')
df_mun = df_mun.withColumnRenamed('municipio_ibge','municipio').withColumnRenamed('sum(valor)','soma')
df_mun.orderBy(df_mun.soma.desc()).show()

#maximo = df_mun.agg({'soma':'max'}).collect()[0][0]
#print("Maximo = ",maximo)
spark.stop()
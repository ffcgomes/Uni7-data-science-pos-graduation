from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.option("multiline","true").json("file:///home/flavio/Disc10/trabalhoFinal/ans.json")

filtro = df.filter((df['ano']==2010) | (df['ano']==2011))

n = filtro.count()
soma = filtro.agg(F.sum('valor')).collect()[0][0]
media = soma/n
print("Media dos Gastos em 2010 e 2011 = ",media)

spark.stop()
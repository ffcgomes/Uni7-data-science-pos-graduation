from pyspark.sql import SparkSession

if __name__ == "__main__":
 spark = SparkSession.builder.appName("App").getOrCreate()
 voos14 = spark.read.option("inferSchema","true").option("header",True).csv("file:///home/flaviolg/spark/aula3/voos-2014.csv")
 voos15 = spark.read.option("inferSchema","true").option("header",True).csv("file:///home/flaviolg/spark/aula3/voos-2015.csv")
 voos14 = voos14.withColumnRenamed("DEST_COUNTRY_NAME","Destino")
 voos15 = voos15.withColumnRenamed("DEST_COUNTRY_NAME","Destino")
 voos14 = voos14.withColumnRenamed("ORIGIN_COUNTRY_NAME","Origem")
 voos15 = voos15.withColumnRenamed("ORIGIN_COUNTRY_NAME","Origem")
 #voos14.show()
 
 #Mostra os 3 primeiros registros
 #voos15.show(3)
 
 #Ordena e obtem os 3 primeiros registros
 #voos15.sort("destino").show(3)

 #Obter a quant. maxima de voos entre dois destinos
 from pyspark.sql.functions import max
 #voos15.select(max("count")).show(1)
 
 #Obter os 5 voos mais frequentes
 from pyspark.sql.functions import desc
 #voos15.sort(desc("count")).limit(5).show()
 
 #Obter a media dos voos em 2015
 from pyspark.sql.functions import sum
 #voos15.select(sum("count")/365).show(1)

 #Uniao
 #voos_concat = voos14.union(voos15)
 #voos_concat.show(5)
 
 #Intersecao
 #voos_int = voos14.intersect(voos15)
 #voos_int.show(5)

 #Quais os 5 paises destino que mais receberam voos em 215
 
 temp = voos15.groupBy("Destino").sum("count").limit(5)
 temp.show()
 temp.sort(desc("sum(count)")).show()

 #Exemplo acima usando sql
 #voos15.createOrReplaceTempView("voos")
 #spark.sql("SELECT Destino,SUM(count) FROM voos GROUP BY Destino ORDER BY SUM(count) DESC LIMIT 5" ).show()

 #Diferenca entreo n. total de voos entre 2014 e 2015
 #tot15 = voos15.select(sum("count"))
 #tot14 = voos14.select(sum("count"))
 
 #voos15.select(sum("count")).subtract(voos14.select(sum("count"))).show(1)

 

 spark.stop()
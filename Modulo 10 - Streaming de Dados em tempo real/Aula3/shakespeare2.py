#-*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf

#Cria a app com o nome WordCount
conf = SparkConf().setAppName("WordCount")

#Instacia o SparkContext
sc = SparkContext.getOrCreate()

#Cria o RDD com o conteúdo do shakespeare.txt
contentRDD = sc.textFile("/home/flavio/Google Drive/Data_Science/Pos-Graducao-Uni7/Disc-10 - Streaming de Dados/spark/shakespeare.txt")
print(contentRDD)
#Elimina as linha em branco
filter_empty_lines = contentRDD.filter(lambda x: len(x) > 0)

#Splita as palavras pelo espaço em branco entre elas
words = filter_empty_lines.flatMap(lambda x: x.split(' '))

#Map-Reduce da contagem das palavras
wordcount = words.map(lambda x:(x,1)) \
.reduceByKey(lambda x, y: x + y) \
.map(lambda x: (x[1], x[0])).sortByKey(False)

#Imprime o resultado
for word in wordcount.collect():
    print(word)

#Salva o resultado no HDFS dentro da pasta /user/toti/textos/Wordcount/
wordcount.saveAsTextFile("/home/flavio/Google Drive/Data_Science/Pos-Graducao-Uni7/Disc-10 - Streaming de Dados/spark/Wordcount")
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("MyApp")
sc = SparkContext(conf=conf)

students = sc.textFile("file:////home/flaviolg/spark/students.csv")
students.take(4)

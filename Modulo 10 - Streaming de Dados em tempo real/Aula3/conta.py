from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("MyApp")
sc = SparkContext(conf=conf)

text_file = sc.textFile("file:///home/flaviolg/spark/aula2/shakespeare.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
for x in counts.collect():
    print x
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("pi_estimation")
sc = SparkContext(conf=conf)

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

count = sc.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
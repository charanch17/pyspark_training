from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("ratings_counter")
sc = SparkContext(conf=conf)
lines = sc.textFile(r"D:\spark_training\ml-100k\u.data")
ratings = lines.map(lambda x : int(x.split()[2]))
print(ratings.top(3))
ratings = ratings.countByValue()


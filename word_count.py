from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster('local').setAppName("word_count")
sc = SparkContext(conf=conf).getOrCreate()
rdd = sc.textFile(r"D:\spark_training\Book").flatMap(lambda x:x.split()).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
print(rdd.take(4))

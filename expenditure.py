from pyspark import SparkConf,SparkContext
conf = SparkConf().setAppName("expenditure").setMaster("local")
sc = SparkContext(conf=conf)
rdd = sc.textFile(r"D:\spark_training\customer-orders.csv")
rdd = rdd.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))
rdd = rdd.reduceByKey(lambda x,y:x+y)
rdd = rdd.sortBy(lambda x:x[1])
print(rdd.take(5))
from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("average_friends")
sc = SparkContext.getOrCreate(conf=conf)
print(sc)
def parse_lines(line):
    meta = line.split(",")
    return (int(meta[2]),int(meta[3]))
lines = sc.textFile(r"D:\spark_training\fakefriends.csv")
age_rdd = lines.map(parse_lines)
print(age_rdd.top(3))
age_rdd = age_rdd.mapValues(lambda x:(x,1))
print(age_rdd.top(3))
age_rdd = age_rdd.reduceByKey(lambda x,y:(x[0]+y[0],y[1]+x[1]))
print(age_rdd.top(3))
age_rdd = age_rdd.mapValues(lambda x:x[0]/x[1])
print(age_rdd.top(3))
age_rdd = age_rdd.sortBy(lambda x:x[1],ascending=False).take(3)
print(age_rdd)
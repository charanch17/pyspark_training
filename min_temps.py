from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster("local").setAppName("min_temps")
sc = SparkContext(conf=conf).getOrCreate()
def parse_lines(line):
    x = line.split(",")
    return (x[0],x[2],x[3])
rdd = sc.textFile(r"D:\spark_training\1800.csv")
rdd = rdd.map(parse_lines)
filtered_rdd = rdd.filter(lambda x:'TMIN' in x[1])
print(set(filtered_rdd.keys().collect()))
filtered_rdd = filtered_rdd.map(lambda x:(x[0],x[2]))
filtered_rdd = filtered_rdd.reduceByKey(lambda x,y: x if x<y else y)
print(filtered_rdd.collect())

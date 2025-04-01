from pyspark.sql import SparkSession,Row,functions as funcs

ss = SparkSession.builder.appName("fake_friendsdf").getOrCreate()

df = ss.read.csv(r"D:\spark_training\fakefriends-header.csv",header =True,sep=',',inferSchema=True)
df.show()
print(df.describe())
df.printSchema()
df= df.select(['age','friends'])
df = df.groupBy(['age']).agg(funcs.round(funcs.avg("friends"),3).alias("average_friends")).sort(["average_friends"],ascending=False)
df.show()

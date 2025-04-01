from pyspark.sql import SparkSession, Row, functions as funcs
ss = SparkSession.builder.appName("reader").getOrCreate()
rdd = ss.sparkContext.textFile(r"fakefriends.csv")


def parse_lines(line):
    l = line.split(",")
    return Row(id=l[0], name=l[1], age=int(l[2]), friends=int(l[3]))


df = rdd.map(parse_lines).toDF()
df.createOrReplaceTempView("data")
final_df = ss.sql("select * from data")
final_df.show()
test_df = df.select(["age", "name"]).where(
    "age>10 and name !='Will' ").limit(1).show()
test_df = df.select(["age", "friends"]).groupBy(["age"]).agg(funcs.sum("friends").alias("total_friends")).filter(
    "total_friends>10").select(["age", "total_friends"]).orderBy("total_friends", ascending=False).show()
print(type(df))

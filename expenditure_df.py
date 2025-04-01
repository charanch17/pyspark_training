from pyspark.sql import SparkSession, Row, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

ss = SparkSession.builder.appName("identify_expenditure").getOrCreate()
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("price", FloatType(), True)
])
df = ss.read.csv(r"customer-orders.csv", schema=schema, header=False, sep=",")
df.printSchema()
df.show()
df = df.groupBy(["customer_id"]).agg(func.sum("price").alias("expenditure")).sort(["expenditure"], ascending=False).select(["customer_id","expenditure"])
df.withColumn("expenditure",func.round("expenditure",2)).show()
df.show()
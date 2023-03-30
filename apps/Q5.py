from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import count, desc

spark = SparkSession.builder.appName("sample").getOrCreate()

business_schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("full_address", StringType(), True),
    StructField("categories", StringType(), True)
])
review_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True),
    StructField("stars", StringType(), True)
])

user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("url", StringType(), True)
])

business_df = spark.read.csv("/opt/spark-data/business.csv", header=False, schema=business_schema, sep="::")
review_df = spark.read.csv("/opt/spark-data/review.csv", header=False, schema=review_schema, sep="::")
user_df = spark.read.csv("/opt/spark-data/user.csv", header=False, schema=user_schema, sep="::")

user_review_count_df = review_df.groupBy("user_id").agg(count("review_id").alias("review_count"))
user_review_count_df = user_review_count_df.join(user_df, "user_id")
total_review_count = review_df.count()
user_review_count_df = user_review_count_df.withColumn("contribution", user_review_count_df["review_count"] / total_review_count * 100)
top_10_users = user_review_count_df.orderBy(desc("contribution")).limit(10).select("name", "contribution")
top_10_users.write.csv("/opt/spark-data/output/out5", header=True, mode="overwrite")

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, countDistinct, split, explode, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

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

business_review_join = business_df.join(review_df,["business_id"],"inner")

business_review_join = business_review_join.withColumn('categories', split(business_review_join.categories, ', '))\
        .select('*', explode('categories')\
        .alias('category'))

business_review_join = business_review_join.withColumn('category', regexp_replace('category', 'List\\(|\\)', ''))

category_user_agg = business_review_join.groupBy('category', 'user_id')\
        .agg(avg('stars').alias('average rating'))

category_agg = category_user_agg.groupBy('category')\
        .agg(countDistinct(category_user_agg.user_id)\
        .alias('no. of unique reviewers'),avg('average rating').alias("average rating"))
category_agg.coalesce(1).write.csv("/opt/spark-data/output/out4", header=True, mode="overwrite")

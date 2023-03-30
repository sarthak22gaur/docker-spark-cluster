from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Yelp Statistics').getOrCreate()

business_df = spark.read.csv('/opt/spark-data/business.csv', sep='::', header=None, inferSchema=True)
review_df = spark.read.csv('/opt/spark-data/review.csv', sep='::', header=None, inferSchema=True)
user_df = spark.read.csv('/opt/spark-data/user.csv', sep='::', header=None, inferSchema=True)

stanford_business_df = business_df.filter(business_df['_c1'].contains('Stanford'))
review_stanford_df = stanford_business_df.join(review_df, review_df['_c2'] == stanford_business_df['_c0'], 'inner')\
        .select(review_df['_c1'].alias('user_id'), '_c3', stanford_business_df['_c0'])

average_rating_df = review_stanford_df.groupBy('user_id').avg('_c3').alias('average_rating')

result_df = average_rating_df.join(user_df, average_rating_df['user_id'] == user_df['_c0'], 'inner')\
        .select(user_df['_c1'].alias('name'), 'average_rating.avg(_c3)')

result_df.coalesce(1).write.csv("/opt/spark-data/output/out3_sql", header=True, mode="overwrite")

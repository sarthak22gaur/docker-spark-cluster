from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)

def println(line):
    print(line)

users = sc.textFile("/opt/spark-data/user.csv")\
        .map(lambda line: line.split("::"))\
        .map(lambda x: (x[0], x[1]))

business = sc.textFile("/opt/spark-data/business.csv")\
        .map(lambda line: line.split("::"))

review = sc.textFile("/opt/spark-data/review.csv")\
        .map(lambda line: line.split("::"))\
        .map(lambda x: (x[2], (x[1], float(x[3]))))


def func(line):
    if "Stanford" in line[1]:
        return line

stanfordBusinesses = business.map(func).filter(lambda x: x != None)
joined = stanfordBusinesses.leftOuterJoin(review)\
        .map(lambda x: (x[0], x[1][1]))\
        .map(lambda x: (x[1][0], x[1][1]))\
        .mapValues(lambda x: (x, 1))\
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
        .mapValues(lambda x: x[0] / x[1])\
        .leftOuterJoin(users)\
        .map(lambda x: str(x[1][1]) + "\t" + str(x[1][0]))

joined.coalesce(1).saveAsTextFile("/opt/spark-data/output/out3")

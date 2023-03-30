from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)

friends = sc.textFile("/opt/spark-data/mutual.txt")\
        .map(lambda line: line.split("\t"))\
        .map(lambda x: (x[0], x[1].split(",")))

def createpair(line):
    tmp = []
    if len(line[1]) > 0:
        for i in line[1]:
            if (i < line[0]):
                tmp.append((i + "," + line[0], line[1]))
            else:
                tmp.append((line[0] + "," + i, line[1]))
        return tmp
    return

common = friends.flatMap(createpair)\
        .reduceByKey(lambda x, y: list(set(x).intersection(y)))\
        .map(lambda x: (x[0], len(x[1])))\
        .sortBy(lambda x: x[1], True)

# pairs with non-zero mutual friends
nonzero = common.filter(lambda x: x[1] != 0)
top = sc.parallelize(nonzero.take(10))\
        .map(lambda x: str(x[0].split(',')[0]) + ", " + str(x[0].split(',')[1]) + "\t" + str(x[1]))

top.saveAsTextFile("/opt/spark-data/output/out2")

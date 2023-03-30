from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)

friends = sc.textFile("/opt/spark-data/mutual.txt").map(lambda line: line.split("\t")).map(lambda x: (x[0], x[1].strip().split(",")))

def createPair(line):
    tmp = []
    if len(line[1]) > 0:
        for i in line[1]:
            if (i < line[0]):
                tmp.append((i + "," + line[0], line[1]))
            else:
                tmp.append((line[0] + "," + i, line[1]))
        return tmp
    return

def println(line):
    print(line)

friends.flatMap(createPair).reduceByKey(lambda x, y: list(set(x).intersection(y)))\
        .filter(lambda x: x[0][0] != ',')\
        .map(lambda x: str(x[0].split(',')[0]) + ", " + str(x[0].split(',')[1]) + "\t" + str(len(x[1])))\
        .saveAsTextFile("/opt/spark-data/output/out1")

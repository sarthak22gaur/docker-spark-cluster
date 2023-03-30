from pyspark.mllib.linalg import Matrices
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("sample")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

matrix1 = sc.textFile("/opt/spark-data/matrix1_small.txt").map(lambda x: x.strip().split(" "))
matrix2 = sc.textFile("/opt/spark-data/matrix2_small.txt").map(lambda x: x.strip().split(" "))

rowcount_matrix1 = matrix1.count()
matrix1_rows = matrix1.collect()

rowcount_matrix2 = matrix2.count()
matrix2_rows = matrix2.collect()

rows_list = []

def lst_func(z):
    row_list=[]
    for x in z:
        row_list.append(int(x))
    return row_list


for i in range(rowcount_matrix1):
    rows_list.append(IndexedRow(i,[i] + lst_func(matrix1_rows[i])))

matrix1_Indexedrows = sc.parallelize(rows_list)
guest_feature_matrix = IndexedRowMatrix(matrix1_Indexedrows)

item_feature_vector_length = 0
for x in matrix2_rows[0]:
    item_feature_vector_length = item_feature_vector_length + 1

item_feature_array = [1] + [0] * rowcount_matrix2

for i in range(item_feature_vector_length):
    item_feature_array+=[0]
    for j in range(rowcount_matrix2):
        item_feature_array = item_feature_array + [int(matrix2_rows[j][i])]

a = rowcount_matrix2 + 1
b = item_feature_vector_length + 1
item_feature_matrix = Matrices.dense(a, b, item_feature_array)

ratings_matrix = guest_feature_matrix.multiply(item_feature_matrix)
result = ratings_matrix.rows.map(lambda x: x).coalesce(1)

result.saveAsTextFile("/opt/spark-data/output/out6")

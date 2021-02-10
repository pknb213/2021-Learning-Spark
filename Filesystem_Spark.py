import pyspark, json, os, glob
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from Spark.spark.python.pyspark.context import xrange
from datetime import datetime

print(os.getcwd())
# for root, dir, files in os.walk(os.getcwd()):
file_list = glob.glob(os.getcwd() + "/*.log")
file_list = list(set(file_list))
print(file_list, type(file_list))

for i in file_list:
    with open(i, "r", encoding="UTF8") as f:
        with open("./20210207_dict.log", "a", encoding="UTF8") as c:  # Todo : 날짜 동적 업데이트
            for line in f.readlines():
                # Str to Json json.loads(line[line.find('{'):line.find('}')+1].replace("'", "\""))
                c.write(line[line.find('{'):line.find('}')+1].replace("'", "\"") + "\n")
li = list
with open("./20210207_dict.log", "r", encoding="UTF8") as f:
     li = f.readlines()

print(li, type(li), len(li))

spark = SparkSession.builder\
    .master("local")\
    .appName("word Count")\
    .config("spark.driver.memory", "5g")\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()

sc = spark.sparkContext

# conf = SparkConf().setAppName("test").setMaster("local")
# sc = SparkContext(conf=conf)
data = [{'a': x, 'b': x*x} for x in xrange(0, 100000)]
rdd = sc.parallelize(data)
st = datetime.now()
print(rdd)
print(rdd.collect())
print(rdd.count())
print(datetime.now() - st)


def div(v):
    v['b'] = v['b'] // 10
    return v


st = datetime.now()
ddd = rdd.map(div)
print(ddd.collect())
print(ddd.count())
print(datetime.now() - st)


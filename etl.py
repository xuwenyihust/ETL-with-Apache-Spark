from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext

# Define Spark configuration
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("MySQL_import")
#conf.set("spark.executor.memoryg")

# Initialize a SparkContext and SQLContext
sc = SparkContext(conf=conf)
sql_ctx = SQLContext(sc)

# Initialize hive context
hive_ctx = HiveContext(sc)

hive_ctx.sql("CREATE TABLE IF NOT EXISTS test ( \
				id int, \
				num int) \
			")
results = hive_ctx.sql("DESCRIBE test").collect()
print(results)

print('>'*80)

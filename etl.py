import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext

def main(sc, filename):
	# Initialize sparksql context
	sql_ctx = SQLContext(sc)
	# Initialize hive context
	hive_ctx = HiveContext(sc)
	hive_ctx.sql("DROP TABLE IF EXISTS UserMovieRatings")
	hive_ctx.sql(	"CREATE EXTERNAL TABLE IF NOT EXISTS UserMovieRatings ( \
						userId int, \
						movieId int, \
						rating int, \
						unixTimestamp bigint) \
					ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' \
					STORED AS TEXTFILE \
					LOCATION '/home/wenyixu/Python/ETL-with-Apache-Spark/data/'"
				)
	summary = hive_ctx.sql("DESCRIBE UserMovieRatings").collect()
	table_head = hive_ctx.sql("SELECT * FROM UserMovieRatings LIMIT 5").collect()
	print('>'*80)
	print('>>> Input file:')
	print(filename)
	print('>>> Table summary:')
	for x in summary:
		print(x)
	print('>>> Table head:')
	for x in table_head:
		print(x)
	print('>'*80)

if __name__=="__main__":
	# Define Spark configuration
	conf = SparkConf()
	conf.setMaster("local[4]")
	conf.setAppName("MySQL_import")
	# Initialize a SparkContext and SQLContext
	sc = SparkContext(conf=conf)
	filename = sys.argv[1]
	# Execute main function
	main(sc, filename)


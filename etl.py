import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext

def main(sc, file1, file2):
	# Initialize sparksql context
	sql_ctx = SQLContext(sc)
	# Initialize hive context
	hive_ctx = HiveContext(sc)
	############################################################################
	# Create Table Definitions Using Hive 
	hive_ctx.sql("DROP TABLE IF EXISTS UserMovieRatings")
	hive_ctx.sql("DROP TABLE IF EXISTS UserDetails")
	hive_ctx.sql("DROP TABLE IF EXISTS MovieDetails")
	hive_ctx.sql(	"CREATE EXTERNAL TABLE IF NOT EXISTS UserMovieRatings ( \
						userId int, \
						movieId int, \
						rating int, \
						unixTimestamp bigint) \
					ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' \
					STORED AS TEXTFILE \
					LOCATION "+file1
				)	
	table1_head = hive_ctx.sql("SELECT * FROM UserMovieRatings LIMIT 5").collect()
	hive_ctx.sql(	"CREATE EXTERNAL TABLE IF NOT EXISTS UserDetails ( \
						userId int, \
						age int, \
						gender varchar(1), \
						occupation string, \
						zipCode string) \
					ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' \
					STORED AS TEXTFILE \
					LOCATION "+file2
				)
	table2_head = hive_ctx.sql("SELECT * FROM UserDetails LIMIT 5").collect()
	hive_ctx.sql(	"CREATE EXTERNAL TABLE IF NOT EXISTS MovieDetails ( \
						movieId int, \
						title string, \
						time string, \
						website string) \
					ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' \
					STORED AS TEXTFILE \
					LOCATION "+file3
				)
	table3_head = hive_ctx.sql("SELECT * FROM MovieDetails LIMIT 5").collect()
	############################################################################
    # Transform The Data Using Hive
	'''num_5ratings = hive_ctx.sql("SELECT title, COUNT(*) AS numberOf5Ratings \
					FROM MovieDetails \
					JOIN UserMovieRatings \
					ON MovieDetails.movieId = UserMovieRatings.movieId \
					WHERE rating = 5 \
					GROUP BY title \
					ORDER BY numberOf5Ratings DESC LIMIT 5").collect()
	num_5ratings_m = hive_ctx.sql( \
				"SELECT gender, title, COUNT(*) AS numberOf5Ratings \
				FROM UserMovieRatings \
				JOIN UserDetails \
				ON UserMovieRatings.userId = UserDetails.userId \
				JOIN MovieDetails \
				ON UserMovieRatings.movieId = MovieDetails.movieId \
				WHERE rating = 5 \
				AND gender = 'M' \
				GROUP BY title, gender \
				ORDER BY numberOf5Ratings DESC LIMIT 5").collect()
	num_5ratings_f = hive_ctx.sql( \
				"SELECT gender, title, COUNT(*) AS numberOf5Ratings \
				FROM UserMovieRatings \
				JOIN UserDetails \
				ON UserMovieRatings.userId = UserDetails.userId \
				JOIN MovieDetails \
				ON UserMovieRatings.movieId = MovieDetails.movieId \
				WHERE rating = 5 \
				AND gender = 'F' \
				GROUP BY title, gender \
				ORDER BY numberOf5Ratings DESC LIMIT 5").collect()
	'''
	num_5ratings_mf = hive_ctx.sql( \
				"SELECT * FROM \
				(SELECT gender, title, COUNT(*) AS numberOf5Ratings \
				FROM UserMovieRatings \
				JOIN UserDetails \
				ON UserMovieRatings.userId = UserDetails.userId \
				JOIN MovieDetails \
				ON UserMovieRatings.movieId = MovieDetails.movieId \
				WHERE rating = 5 \
				AND gender = 'M' \
				GROUP BY title, gender \
				ORDER BY numberOf5Ratings DESC LIMIT 5) AS m \
				UNION \
				SELECT * FROM \
				(SELECT gender, title, COUNT(*) AS numberOf5Ratings \
				FROM UserMovieRatings \
				JOIN UserDetails \
				ON UserMovieRatings.userId = UserDetails.userId \
				JOIN MovieDetails \
				ON UserMovieRatings.movieId = MovieDetails.movieId \
				WHERE rating = 5 \
				AND gender = 'F' \
				GROUP BY title, gender \
				ORDER BY numberOf5Ratings DESC LIMIT 5) AS f\
				ORDER BY gender DESC").collect()

	print('>'*80)
	print('>>> Input file1:')
	print(file1)
	print('>>> Table1 head:')
	for x in table1_head:
		print(x)
	print('>>> Table2 head:')
	for x in table2_head:
		print(x)
	print('>>> Table3 head:')
	for x in table3_head:
		print(x)
	print('>>> Number of 5 ratings:')
	for x in num_5ratings:
		print(x)
	print('>>> Number of 5 ratings for male:')
	for x in num_5ratings_m:
		print(x)
	print('>>> Number of 5 ratings for female:')
	for x in num_5ratings_f:
		print(x)
	print('>>> Number of 5 ratings for male&female:')
	for x in num_5ratings_mf:
		print(x)
	print('>'*80)

if __name__=="__main__":
	# Define Spark configuration
	conf = SparkConf()
	conf.setMaster("local[4]")
	conf.setAppName("MySQL_import")
	# Initialize a SparkContext and SQLContext
	sc = SparkContext(conf=conf)
	file1 = sys.argv[1]
	file2 = sys.argv[2]
	file3 = sys.argv[3]
	# Execute main function
	main(sc, file1, file2)


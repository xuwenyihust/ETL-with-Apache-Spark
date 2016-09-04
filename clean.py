#!/usr/bin/python

genre_li = ['unknown', \
			'Action', \
			'Adventure', \
			'Animation', \
			"Children's", \
			'Comedy', \
			'Crime', \
			'Documentary', \
			'Drama', \
			'Fantasy', \
			'Film-Noir', \
			'Horror', \
			'Musical', \
			'Mystery', \
			'Romance', \
			'Sci-Fi', \
			'Thriller', \
			'War', \
			'Western']

raw = open("./data/movie_details/movie-details.txt", "r")
clean = open("./data/movie_details_clean/movie-details.txt", 'w')

for line in raw:
	cleaned_str = ''
	line = line.split('|')
	movieId = line[0]
	movieName = line[1]
	genre = line[5:]






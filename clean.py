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

raw = open("./data/movie_details/movie-details.txt", "r", encoding = "ISO-8859-1")
clean = open("./data/movie_details_clean/movie-details.txt", 'w')

for line in raw:
	cleaned_str = ''
	line = line.split('|')
	movieId = line[0]
	movieName = line[1]
	genre = line[5:]

	cleaned_str += str(movieId)
	cleaned_str += '#'
	cleaned_str += str(movieName)
	cleaned_str += '#'
	
	for i in range(len(genre)):
		if genre[i] == '1':
			cleaned_str += genre_li[i]
			cleaned_str += '|'
	cleaned_str = cleaned_str.rstrip('|')
	cleaned_str += '\n'
	#print(cleaned_str)
	clean.write(cleaned_str)

raw.close()
clean.close()


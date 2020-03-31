from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
	movieNames = {}
	with open("/home/cloudera/ml-100k/u.item",encoding = "ISO-8859-1") as f:
		for line in f:
			fields = line.split('|')
			movieNames[int(fields[0])] = fields[1]
	return movieNames

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///home/cloudera/temp").appName("PopularMovies").getOrCreate()

# Load up our movie ID -> name dictionary
nameDict = loadMovieNames()

# Get the raw data
lines = spark.sparkContext.textFile("file:///home/cloudera/ml-100k/u.data")
# Convert it to a RDD of Row objects
movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
# Convert that to a DataFrame
movieDataset = spark.createDataFrame(movies)

topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()

topMovieIDs.show()

# Grab the top 10
top10 = topMovieIDs.take(10)
total = []
total1 = []
# Print the results

print("\n")
for result in top10:
	total = ("%s" % (nameDict[result[0]]))
	total1.append(total)
file = open("/home/cloudera/result/result1.txt","w")
for item in total1:
	file.write("%s\n" % item)
file.close()

# Stop the session
spark.stop()


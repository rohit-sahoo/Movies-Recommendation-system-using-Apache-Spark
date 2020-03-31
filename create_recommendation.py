import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating

def loadMovieNames():
    movieNames = {}
    with open("/home/cloudera/ml-100k/u.item", encoding='ascii', errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendationsALS")
sc = SparkContext(conf = conf)
sc.setCheckpointDir('checkpoint')

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("file:///home/cloudera/ml-100k/u.data")

ratings = data.map(lambda l: l.split()).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()
#print(ratings)

# Build the recommendation model using Alternating Least Squares
print("\nTraining recommendation model...")
rank = 10
# Lowered numIterations to ensure it works on lower-end systems
numIterations = 6
model = ALS.train(ratings, rank, numIterations)

userID = int(sys.argv[1])

print("\nRatings for user ID " + str(userID) + ":")
userRatings = ratings.filter(lambda l: l[0] == userID)
current = []
current1 = []
for rating in userRatings.collect():
	current = nameDict[int(rating[1])] + ": " + str(rating[2])
	current1.append(current)
file = open("/home/cloudera/result/user_ratings.txt","w")
for mr in current1:
        file.write("%s\n" % mr)
file.close()


print("\nTop 10 recommendations:")
recommendations = model.recommendProducts(userID, 10)
total = []
total1 = []
for recommendation in recommendations:
    total = nameDict[int(recommendation[1])] #+ "\t--> " + \
		#" score: " + str(recommendation[2])
    total1.append(total)
file = open("/home/cloudera/result/recommendation.txt","w")
for item in total1:
	file.write("%s\n" % item)
file.close()



import sys
from pyspark import SparkConf, SparkContext
from math import sqrt
from pyspark.mllib.recommendation import ALS,MatrixFactorizationModel, Rating
conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendationsALS")
sc = SparkContext(conf = conf)
sc.setCheckpointDir('checkpoint')

movielens = sc.textFile("file:///home/cloudera/ml-100k/u.data")

clean_data = movielens.map(lambda x:x.split('\t'))

rate = clean_data.map(lambda y: int(y[2]))

users = clean_data.map(lambda y: int(y[0]))

mls = movielens.map(lambda l: l.split('\t'))
ratings = mls.map(lambda x: Rating(int(x[0]),\
    int(x[1]), float(x[2])))

train, test = ratings.randomSplit([0.7,0.3],7856)

train.count()
test.count()

#Need to cache the data to speed up training
train.cache()
test.cache()

rank = 5
numIterations = 10
model = ALS.train(train, rank, numIterations)

pred_input = train.map(lambda x:(x[0],x[1]))

pred = model.predictAll(pred_input) 

true_reorg = train.map(lambda x:((x[0],x[1]), x[2]))
pred_reorg = pred.map(lambda x:((x[0],x[1]), x[2]))

true_pred = true_reorg.join(pred_reorg)

from math import sqrt

MSE = true_pred.map(lambda r: (r[1][0] - r[1][1])**2).mean()
RMSE = sqrt(MSE)

file = open("/home/cloudera/result/accuracy.txt","w")
file.write("The RMSE vaue for the model is:%s\n" % RMSE)
file.close()


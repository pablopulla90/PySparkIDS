#!/usr/bin/env python
"""
Copyright Google Inc. 2016
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from __future__ import print_function

import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating


CLOUDSQL_INSTANCE_IP = sys.argv[1]
CLOUDSQL_DB_NAME = sys.argv[2]
CLOUDSQL_USER = sys.argv[3]
CLOUDSQL_PWD  = sys.argv[4]

conf = SparkConf().setAppName("app_collaborative")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

jdbcUrl    = 'jdbc:mysql://%s:3306/%s?user=%s&password=%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME, CLOUDSQL_USER, CLOUDSQL_PWD)

#[START how_far]
def howFarAreWe(model, against, sizeAgainst):
  # Ignore the rating column  
  againstNoRatings = against.map(lambda x: (int(x[0]), int(x[1])) )

  # Keep the rating to compare against
  againstWiRatings = against.map(lambda x: ((int(x[0]),int(x[1])), int(x[2])) )

  # Make a prediction and map it for later comparison
  # The map has to be ((user,product), rating) not ((product,user), rating)
  predictions = model.predictAll(againstNoRatings).map(lambda p: ( (p[0],p[1]), p[2]) )

  # Returns the pairs (prediction, rating)
  predictionsAndRatings = predictions.join(againstWiRatings).values()

  # Returns the variance
  return sqrt(predictionsAndRatings.map(lambda s: (s[0] - s[1]) ** 2).reduce(add) / float(sizeAgainst))
#[END how_far]

# Read the data from the Cloud SQL
# Create dataframes
dfRates = sqlContext.read.jdbc(url=jdbcUrl, table='Rating')

rddUserRatings = dfRates.filter(dfRates.userId == 0).rdd
print(rddUserRatings.count())

# Split the data in 3 different sets : training, validating, testing
# 60% 20% 20%
rddRates = dfRates.rdd
rddTraining, rddValidating, rddTesting = rddRates.randomSplit([6,2,2])

#Add user ratings in the training model
rddTraining.union(rddUserRatings)
nbValidating = rddValidating.count()
nbTesting    = rddTesting.count()
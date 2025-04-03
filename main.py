import sys
import argparse
if __name__ == '__main__':
    from pyspark.sql import SparkSession
    #from pyspark import SparkConf, SparkContext
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    from sparkFunctions.transformationFunc import *
    parser = argparse.ArgumentParser()
    parser.add_argument("--friends",type=str)
    parser.add_argument("--movie", type=str)
    args = parser.parse_args()
    friends = args.friends
    print(args)
    friend_df = readFileFunc(spark, friends)
    friend_df.show(truncate=False)
    movie_df = readFileFunc(spark, args.movie)
    movie_df.show(truncate=False)
    l = [10, 20, 30, 40, 200, 50, 60, 70, 80]
    print(max(l),l.index(max(l)))
    l1 = l.copy()
    min(l)
    print("sort : ",l1.sort())
    print("sort l : ",l)
    print("index and high ",l1[-1],l.index(l1[-1]))
    s = {(10,20):'durga'}
    x = 0
    for i in l:
        for j in l:
            if i < j:
               # print(x)
                x = j
    print(x,l.index(x))

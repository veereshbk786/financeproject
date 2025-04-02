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
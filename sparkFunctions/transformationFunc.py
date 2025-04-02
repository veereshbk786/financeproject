def readFileFunc(spark, path):
    friend_df = spark.read.format("csv").option("header", "true").load(path)
    return friend_df

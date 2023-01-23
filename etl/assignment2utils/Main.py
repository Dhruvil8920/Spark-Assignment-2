from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Spark-Assignment-2").getOrCreate()
path = "E:/CLG/py/Spark-Assignment-2/dataset/ghtorrent-logs.txt"
schema = StructType([StructField("Login_Level", StringType(), True), \
                     StructField("time", DateType(), True), \
                     StructField("downloader_id", StringType(), True)])


# Reading TextFile
def ReadText():
    rdd1 = spark.sparkContext.textFile(path)
    return rdd1


# Total Lines
def TotalLines():
    rdd1 = ReadText()
    return (f"Total lines in rdd is: {format(rdd1.count())}")


# Total Warnings
def TotalWarnings():
    rdd1 = ReadText()
    return ("Total warnings is: {}".format(rdd1.filter(lambda x: "WARN" in x).count()))


# Total Api
def TotalRepository():
    rdd1 = ReadText()
    return ("Total Repositories : {}".format(rdd1.filter(lambda x: "api_client" in x).count()))


# Creating Dataframe
def makeDf(spark, schema):
    dfA2 = spark.read.csv("E:/CLG/py/Spark-Assignment-2/dataset/ghtorrent-logs.txt", inferSchema=True, schema=schema)
    return dfA2


def ProperDf():
    dfA2 = makeDf(spark, schema)
    finalDF = dfA2.withColumn('id', split(col("downloader_id"), "--").getItem(0)) \
        .withColumn('clientInter', split(col("downloader_id"), "--").getItem(1)) \
        .withColumn('client', split(col("clientInter"), ":").getItem(0)) \
        .withColumn('jobInfo', split(col("clientInter"), ":").getItem(1))
    finalDF = finalDF.drop("clientInter", "downloader_id")

    return finalDF


# Most HTTP request
def MostHttpReq():
    finaldf = ProperDf()
    dfMostReq = finaldf.filter(finaldf.jobInfo.contains("request")).groupBy("id").count()
    df = dfMostReq.groupBy("id").max("count").orderBy(col("max(count)").desc())
    df = df.withColumn("sr_no", row_number().over(Window.orderBy(monotonically_increasing_id())))
    ans = df.filter(col("sr_no") == 1)
    ans = ans.withColumnRenamed("max(count)", "maxHttpRequest") \
        .drop("sr_no")

    return ans

# Most failed HTTP request
def failedRequest():
    finaldf = ProperDf()
    dfFailed = finaldf.filter(finaldf.jobInfo.contains("Failed")).groupBy("id").count().orderBy(col("count").desc())
    ans1 = dfFailed.withColumn("sr_no", row_number().over(Window.orderBy(monotonically_increasing_id())))
    ans1 = ans1.groupBy("id", "sr_no").max("count").filter(ans1.sr_no == 1).select("id", col("max(count)").alias(
        "maxFailedRequest"))
    return ans1

# Most active hour of the day
def ActiveHour():
    finaldf = ProperDf()
    df = finaldf.groupBy("time").count()
    df = df.withColumn("sr_no", row_number().over(Window.orderBy(monotonically_increasing_id()))).na.drop()
    df = df.groupBy("sr_no", "time").max("count").filter(df.sr_no == 1).select(
        col("time").alias("mostRepeatedDate"), col("max(count)").alias("noOfReps"))

    return df

# Most active repository
def MostActiveRepo():
    finaldf = ProperDf()
    df = finaldf.withColumn("MostActiveRepo", split(col("jobInfo"), " ").getItem(2))
    df = df.filter(df.jobInfo.contains("Repo")).filter(df.client.contains("ghtorrent.rb")).groupBy("id",
                                                                                                   "MostActiveRepo").agg(
        count("MostActiveRepo").alias("timesUsed")).sort(col("timesUsed").desc()) \
        .withColumn("sr_no", row_number().over(Window.orderBy(monotonically_increasing_id())))
    df = df.select("MostActiveRepo", "timesUsed").where(df.sr_no == 1)

    return df


from etl.assignment2utils.Main import *
import unittest

path = "E:/CLG/py/Spark-Assignment-2/dataset/ghtorrent-logs.txt"
Readrdd = spark.sparkContext.textFile(path)
schema = StructType([StructField("Login_Level",StringType(), True),\
                     StructField("time",DateType(), True),\
                     StructField("downloader_id", StringType(),True)])
chackdfa = spark.read.csv(path, inferSchema=True, schema=schema)

class TestMyFunc(unittest.TestCase):

    def testReadText(self):

        def read(self):
            Readrdd1 = Readrdd
            return Readrdd1
        self.assertTrue(ReadText(), read(self))

    def testTotalLines(self):
        self.assertTrue(TotalLines(), 281234)

    def testTotalWarnings(self):
        self.assertTrue(TotalWarnings(), 3811)

    def testTotalRepository(self):
        self.assertTrue(TotalRepository(), 37596)

    def testMostHTTPReq(self):
        data = [("ghtorrent-13", 2515)]
        schema = StructType([StructField("id", StringType(), True), StructField("maxHttpRequest", LongType(), True)])
        chackDF = spark.createDataFrame(data=data, schema=schema)
        self.assertTrue(MostHttpReq(), chackDF)

    def testMaxFailedRequests(self):
        data = [("ghtorrent-13", 2321)]
        schema = StructType([StructField("id", StringType(), True), \
                             StructField("maxFailedRequest", LongType(), True)])
        chackDf = spark.createDataFrame(data=data, schema=schema)
        self.assertTrue(failedRequest(), chackDf)

    def testActiveHour(self):
        data = [("2017-03-24", 30963)]
        schema = StructType([StructField("mostRepeatedDate", StringType(), True), \
                             StructField("noOfReps", LongType(), True)])
        chackDF = spark.createDataFrame(data=data, schema=schema)
        self.assertTrue(ActiveHour(), chackDF)

    def testMostActiveRepo(self):
        data = [("ovyx/HammerheadN", 657)]
        schema = StructType([StructField("MostActiveRepo", StringType(), True), \
                             StructField("timeUsed", LongType(), True)])
        chackDF = spark.createDataFrame(data=data, schema=schema)
        self.assertTrue(MostActiveRepo(), chackDF)


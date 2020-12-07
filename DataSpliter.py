import os
import sys
import pyhdfs
from pandas.core.algorithms import quantile
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType
import pandas as pd
import numpy as np
import PinkMST
from Point import Point

from pyspark.sql.session import SparkSession
from pyspark import SparkConf
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]").set("spark.cores.max","4"))

filePath = "test/data.txt"
hadoop_master ='hdfs://127.0.0.1:9000'


class DataSpliter():
    def __init__(self, inputFileName=filePath, numSplits=None, outputDir=None):

        self.inputFileName = inputFileName
        self.numSplits = numSplits
        self.outputDir = outputDir
        self.sc = sc
        self.data = None

    def getSparkConf(self):
        return self.sc

    def displayValue(points, size): 
        print("=" * 100)
        num = 0
        for p in points:
            print("====:" + p)
            p += 1
            if p > size:
                break
        print("=" * 100)

    def readFile(self):
        data = self.sc.sequenceFile(self.outputDir).map(lambda x:x[1]).collect()
        PinkMST.displayValue(data,None,len(data))

    def createPartitionFiles(self,fileLoc,numPartitions):
        self.delete(fileLoc) #删除原有文件
        idSubgraphs = list(range(int(numPartitions)))  
        self.sc.parallelize(idSubgraphs, numPartitions).saveAsTextFile("hdfs://127.0.0.1:9000" + fileLoc)

    def writeSequenceFile(self):
        self.sc.addPyFile('Point.py')
        data = self.loadData()
        numPoints = len(data)
        print("data number: ", numPoints)
        pointId = 0
        points = []
        for i in data:
            points.append(str(pointId) + ":" + str(i))
            pointId += 1
        
        self.displayValue(points, 5)
        self.saveAsSequenceFile(points)

    def displayValue(self,points, size):
        print("=" * 80)
        for i in range(size):
            print(points[i])
        print("=" * 80)


    def saveAsSequenceFile(self, points):
        self.delete(self.outputDir)
        pointsToWrite = self.sc.parallelize(points,self.numSplits)
        pointsPairToWrite = pointsToWrite.map(lambda x : x)
        pointsPairToWrite.saveAsTextFile(path=hadoop_master+self.outputDir)

    def loadData(self):
        data = self.sc.textFile(self.inputFileName)
        data = data.map(lambda line:line.split(" ")).map(lambda l:[float(i) for i in l])
        schema = StructType([
                StructField("f1", FloatType(), True),
                StructField("f2", FloatType(), True),
                StructField("f3", FloatType(), True),
                StructField("f4", FloatType(), True),
                StructField("f5", FloatType(), True),
#                 StructField("f6", FloatType(), True),
#                 StructField("f7", FloatType(), True),
#                 StructField("f8", FloatType(), True),
#                 StructField("f9", FloatType(), True),
#                 StructField("f10", FloatType(), True)
        ])
        data = SparkSession(self.sc).createDataFrame(data,schema)
        data = data.rdd.map(lambda line: [float(i) for i in line]).take(10000)
        return data

    def delete(self,file):
        fs = pyhdfs.HdfsClient(hosts='127.0.0.1,9000')
        if(fs.exists(file)):
            fs.delete(file,recursive=True)
        else:
            pass

    def writeSmallFiles(self):
        points = [Point(0, [0.0, 0.0]), Point(1, [1.0, 0.0]), Point(2, [0.0, 2.0]), Point(3, [3.0, 2.0])]
        self.saveAsSequenceFile_1(points, self.outputDir)

    @staticmethod
    def openSequenceFile(filename):
        return sc.sequenceFile(filename).collect()

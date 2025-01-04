import sys
from pyspark import SparkConf 
from pyspark.sql import SparkSession
from collections import namedtuple
from lib.logger import Log4j

SurveyRecord = namedtuple('SurveyRecord',['Age','Gender','Country','State'])

if __name__ == '__main__':

    conf = SparkConf()\
            .setMaster('local[3]')\
            .setAppName('HelloRDD')
    

    spark = SparkSession.builder.config(conf = conf).getOrCreate()
    sc = spark.sparkContext

    logger =   Log4j(spark)

    if len(sys.argv) !=2:
        logger.error('Usage: HelloSpark <filename>')
        sys.exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)


    colsRDD = partitionedRDD.map(lambda line: line.replace('"','').split(','))
    selectRDD = colsRDD.map(lambda col: SurveyRecord(col[1],col[2],col[3],col[4]))
    filteredRDD = selectRDD.filter(lambda r : r.Age<40)
    kvRDD = filteredRDD.map(lambda r:(r.Country,1))
    countRDD = kvRDD.reduceByKey(lambda v1,v2:v1+v2)

    colsList = countRDD.collect()

    for x in colsList:
        logger.info()
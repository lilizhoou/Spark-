import sys
from pyspark.sql import SparkSession 
from lib.logger import Log4j


spark = SparkSession\
        .builder\
        .master('local[3]')\
        .appName('HelloSparkSQL')\
        .getOrCreate()
logger = Log4j(spark)


if len(sys.argv) !=2:
    logger.error('usage: HelloSpark <filename>')
    sys.exit(-1)


surveydf = spark.read\
                .option('header','true')\
                .option('inferSchema','true')\
                .csv(sys.argv[1])

surveydf.createOrReplaceTempView('survey_tbl')
countdf = spark.sql('select Country,count(1) as count from survey_tbl where Age<40 group by 1')

countdf.show()


from pyspark.sql import *


if __name__ == '__main__':


    spark = SparkSession.builder\
                        .appName('Hello Spark') \
                        .master('local[3]') \
                        .getOrCreate()
    
    data_list = [('Lili','2024'),
                 ('Lili','2023'),
                 ('Lili','2022')]
    
    df = spark.createDataFrame(data_list).toDF('Name','Year')

    df.show()
    
#from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntergerType

if __name__ == "__main__":

    spark = (SparkSession.builder \
        .master("local[3]") \
        .appName("estudando-dataframes") \
        .getOrCreate())

    schema = (StructType([StructField("regiao",StructType(),True),
        StructField("estado",StructType(),True),
        StructField("data",StructType(),True),
        StructField("casosNovos",StructType(),True),
        StructField("casosAcumulados",StructType(),True),
        StructField("obitosNovos",StructType(),True),
        StructField("obitosAcumulados",StructType(),True)]))

    df = (spark
        .read
        .format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load("..\PySpark(Iniciantes)\arquivo_geral.csv", schema=schema)
        )

    df.printSchema()
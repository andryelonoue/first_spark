from pyspark import SparkConf
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

    lista = ["um","dois","tres","quatro","cinco","seis"]

    df = (spark
        .read
        .format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load("..\PySpark(Iniciantes)\arquivo_geral.csv").toDF(*lista))

    df = df.select(F.col("_c0").alias(lista[0]),
                    F.col("_c1").alias(lista[1]),
                    F.col("_c2").alias(lista[2]),
                    F.col("_c3").alias(lista[3]),
                    F.col("_c4").alias(lista[4]),
                    F.col("_c5").alias(lista[5]),
                    F.col("_c6").alias("sete"))
                    
    df.printSchema()

    
package training

import org.apache.spark.sql.{DataFrame, SparkSession}
import training.system.Parameters

object pgTest1_2 extends App {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("pgSQL")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  Parameters.initTables
  val adresses: DataFrame = spark.table("adress")

  adresses.write
    .mode("overwrite")
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/cinema")
    .option("dbtable", "public.adress_cinema")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("truncate", "true")
    .save()
}

package training

import org.apache.spark.sql.SparkSession

object pgTest1 extends App {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("pgSQL")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/cinema")
    .option("dbtable", "adress_cinema")
    .option("user", "postgres")
    .option("password", "postgres")
    .load()

  jdbcDF.show()
}

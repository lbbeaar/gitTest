package training
import org.apache.spark.sql.{DataFrame, SparkSession}
import training.pgTest1.jdbcDF
import training.system.Parameters

import java.util.Properties

object pgTest1_1 extends App {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("pgSQL")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  Parameters.initTables
  val table: DataFrame = spark.table("client")

  val connectionProperties = new Properties()
  connectionProperties.put("user", "postgres")
  connectionProperties.put("password", "postgres")
  table.write
    .jdbc("jdbc:postgresql://localhost:5432/cinema", "client", connectionProperties)

}

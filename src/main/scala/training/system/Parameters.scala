package training.system

import training.domain._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Parameters {
  val POPULATION_DATASET_PATH = ""
  val EXAMPLE_OUTPUT_PATH = "./output/"


  val path_client = "./dataset/client/*"
  val path_adress = "./dataset/adress/*"

  val table_client = "client"
  val table_adress = "adress"

  private def createTable(name: String, structType: StructType, path: String, delimiter: String = ";")
                         (implicit spark: SparkSession): Unit = {
    spark.read
      .format("com.databricks.spark.csv")
        //.option("inferSchema", "true")
      .options(
        Map(
          "delimiter" -> delimiter,
          "nullValue" -> "\\N"
        )
      ).schema(structType).load(path).createOrReplaceTempView(name)
  }

  def initTables(implicit spark: SparkSession): Unit = {
    createTable(Parameters.table_client, Client.structType, Parameters.path_client)
    createTable(Parameters.table_adress, Adress.structType, Parameters.path_adress)
  }
}

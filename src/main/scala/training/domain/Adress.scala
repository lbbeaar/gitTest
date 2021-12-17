package training.domain
import org.apache.spark.sql.types._

object Adress extends Enumeration{
  private val DELIMITER = ","

  val ID, CITY_ID, ADRESS, PHONE = Value

  val structType = StructType(
    Seq(
      StructField(ID.toString, IntegerType),
      StructField(CITY_ID.toString, IntegerType),
      StructField(ADRESS.toString, StringType),
      StructField(PHONE.toString, LongType)
    )
  )
}

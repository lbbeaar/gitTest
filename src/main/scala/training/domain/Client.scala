package training.domain
import org.apache.spark.sql.types._

object Client extends Enumeration{
  private val DELIMITER = ","

  val ID, GENDER, LName = Value

  val structType = StructType(
    Seq(
      StructField(ID.toString, IntegerType),
      StructField(GENDER.toString, StringType),
      StructField(LName.toString, StringType)
    )
  )
}

//case class Laptop_Case(code: Int,
//                   model: String,
//                   speed: Int,
//                   ram: Int,
//                   hd: Int,
//                   price: Double,
//                   screen: Int)
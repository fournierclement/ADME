import lib.Session
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import lib.Cleanser


import scala.annotation.tailrec
object Laboratory extends App with Session {


  override def runner(spark: SparkSession) {
    val dataframe = spark.read.json("./resources/data-students.json")


    //Cleansing tests
    val cleanser = Cleanser(dataframe)
    val dataframe2 = cleanser.handleInterests().drop("interests")
    dataframe2.show()
  }

}
import lib.Session
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import lib.Cleanser


import scala.annotation.tailrec
object Laboratory extends App with Session {


  override def runner(spark: SparkSession) {
    val dataframe = spark.read.json("./resources/data-students.json")


    val dataframetest = dataframe
      .drop("type")
      .drop("network")
      .drop("media")
      .drop("impid")
      .drop("publisher")
      // .drop("exchange")



    //Cleansing tests
    val dataFrame1 = Cleanser(dataframetest)
    .handleAppOrSite()
    .handleLabel()
    .handleOS()
    .handleInterests()
    .handleExchange()
    .dataFrame
    .show(10)
  }

}
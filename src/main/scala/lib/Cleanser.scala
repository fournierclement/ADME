package lib
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.annotation.tailrec


case class Cleanser(dataFrame: DataFrame) {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def handleLabel(): Cleanser = {
    Cleanser(
      this.dataFrame.withColumn("label", when($"label".endsWith("true"), 1).otherwise(0))
    )
  }

  def handleAppOrSite(): Cleanser = {
    Cleanser(
      this.dataFrame.withColumn("app", when($"appOrSite".endsWith("app"), 1).otherwise(0)).drop("appOrSite")
    )
  }

  def handleOS(): Cleanser = {
    val func = udf( (s:String) => if(s.toLowerCase.equals("ios")) 1 else 0 )
    Cleanser(
      this.dataFrame.withColumn("iOS", func($"os")).drop("os")
    )
  }

  def handleSize(): Cleanser = {
    // val func = udf( (s:String) => {
    //   case String("[\"",w ,"\",\"", h, "\"]") => Vector(w, h)
    //   case _ => null
    // })
    Cleanser(
      dataFrame
    )
  }

  /**
    * Returns the cleanser's dataframe with the interests column exploded
    * @return the dataframe modified.
    */
  def handleInterests(): Cleanser = {
    Cleanser(
      handleInterestTailrec(dataFrame, Cleanser.interests).drop("interests")
    )
  }
  // https://spark.apache.org/docs/latest/ml-features.html#stringindexer

  def handleExchange(): Cleanser = {
    Cleanser( 
      handleExchangeTailrec(dataFrame, Cleanser.exchanges).drop("exchange")
    )
  }

  @tailrec
  private def handleExchangeTailrec(df : DataFrame, exchanges : Array[String]): DataFrame ={
    if (exchanges.isEmpty){
      df
    } else {
      handleExchangeTailrec(
        df.withColumn(exchanges.head, when($"exchange".contains(exchanges.head), 1).otherwise(0)),
        exchanges.tail
      )
    }
  }

  @tailrec
  private def handleInterestTailrec(df : DataFrame, interests : Array[String]): DataFrame ={
    if (interests.isEmpty){
      df
    } else {
      val newDF = handleOneInterest(df, interests.head)
      handleInterestTailrec(newDF, interests.tail)
    }
  }

  //noinspection ComparingUnrelatedTypes
  def handleOneInterest(df : DataFrame, interestToHandle : String): DataFrame = {
    df.withColumn(interestToHandle, when(
      $"interests".contains(interestToHandle+",")
      || $"interests".contains(interestToHandle + "-")
      || $"interests".endsWith(interestToHandle),
      1).otherwise(0))
  }
}

object Cleanser {
  val interests = Array("IAB1", "IAB2", "IAB3", "IAB4", "IAB5", "IAB6", "IAB7",
    "IAB8", "IAB9", "IAB10", "IAB11", "IAB12", "IAB13", "IAB14", "IAB15",
    "IAB16", "IAB17", "IAB18", "IAB19", "IAB20", "ÏAB21", "IAB22", "IAB23", "IAB24", "IAB25", "IAB26"
  )
  val exchanges = Array(
    "46135ae0b4946b5f2f74274e5618e697",
    "fe86ac12a6d9ccaa8a2be14a80ace2f8",
    "c7a327a5027c1c4de094b0a9f33afad6",
    "f8dd61fb7d4ebfa62cd6acceae3f5c69"
  )
  val headers = Array(
    "app",
    "iOS"
  ) ++ interests ++ exchanges
  def clean(dataFrame: DataFrame): DataFrame = {
    Cleanser(dataFrame)
    .handleAppOrSite()
    .handleLabel()
    .handleOS()
    .handleInterests()
    .handleExchange()
    .dataFrame
  }
}
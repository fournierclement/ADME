package lib
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.annotation.tailrec


case class Cleanser(dataFrame: DataFrame) {

  def mapColumn(from: String, toColumn: String, foo: UserDefinedFunction): Cleanser = {
    Cleanser( dataFrame.withColumn(
        toColumn,
        foo(dataFrame(from))
    ))
  }


  /**
    * Returns the cleanser dataframe with the interests column exploded
    * @return the dataframe modified.
    */
  def handleInterests(): DataFrame = {
    val interests = Array("IAB1", "IAB2", "IAB3", "IAB4", "IAB5", "IAB6", "IAB7",
      "IAB8", "IAB9", "IAB10", "IAB11", "IAB12", "IAB13", "IAB14", "IAB15",
      "IAB16", "IAB17", "IAB18", "IAB19", "IAB20", "ÏAB21", "IAB22", "IAB23", "IAB24", "IAB25", "IAB26")
    handleInterestTailrec(this.dataFrame, interests).drop("interests")
  }


  @tailrec
  private def handleInterestTailrec(df : DataFrame, interests : Array[String]): DataFrame ={
    if (interests.isEmpty){
      df
    }
    else{
      val newDF = handleOneInterest(df, interests(0))
      handleInterestTailrec(newDF, interests.tail)
    }

  }

  //noinspection ComparingUnrelatedTypes
  def handleOneInterest(df : DataFrame, interestToHandle : String): DataFrame ={
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    df.withColumn(interestToHandle, when(
      $"interests".contains(interestToHandle+",")
      || $"interests".contains(interestToHandle + "-")
      || $"interests".equals(interestToHandle),
      1).otherwise(0))
  }

}
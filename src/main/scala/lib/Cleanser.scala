package lib
import org.apache.spark.sql.DataFrame

case class Cleanser(dataFrame: DataFrame) {
  def mapColumn(from: String, toColumn: String, foo: UserDefinedFunction): Cleanser = {
    Cleanser(dataFrame.withColumn())
  }
}
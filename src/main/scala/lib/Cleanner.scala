package lib;
import org.apache.spark.sql.DataFrame

case class Cleanner(dataFrame: DataFrame) {
  def mapColumn(from: String, toColumn: String, foo: UserDefinedFunction): Cleanner = {
    Cleanner(dataFrame.withColum())
  }
}
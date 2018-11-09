package lib
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class Cleanser(dataFrame: DataFrame) {
  def mapColumn(from: String, toColumn: String, foo: UserDefinedFunction): Cleanser = {
    Cleanser(dataFrame)
  }

  /**
    * Implicit function to flatten a given dataframe.
    * @param df
    */
  implicit class DataFrameFlattener(df: DataFrame) {
    def flattenSchema: DataFrame = {
      df.select(flatten(Nil, df.schema): _*)
    }

    protected def flatten(path: Seq[String], schema: DataType): Seq[Column] = schema match {
      case s: StructType => s.fields.flatMap(f => flatten(path :+ f.name, f.dataType))
      case other => col(path.map(n => s"`$n`").mkString(".")).as(path.mkString(".")) :: Nil
    }
  }
}



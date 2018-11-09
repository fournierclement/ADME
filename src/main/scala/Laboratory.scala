import lib.{Session}
import org.apache.spark.sql.{SparkSession}

object Laboratory extends App with Session {
  override def runner(spark: SparkSession) {
    val dataframe = spark.read.json("./resources/data-students.json");
    dataframe.show()
  }
}
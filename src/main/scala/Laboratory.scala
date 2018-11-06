
import org.apache.spark.sql.{SparkSession}

object Laboratory extends App {

  def run() {
    val spark = SparkSession.builder
      .master("local")
      .appName("Data Students")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val dataframe = spark.read.json("./resources/data-students.json");
    dataframe.show()
    spark.stop;
  }


  run()
}
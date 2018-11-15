package predictor
import lib.{Session}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.DataFrame

class WriteCsvSession(filepath: String, dataframe: DataFrame) extends Session {

  def runner(spark: SparkSession){
    //write csv
    dataframe
     .coalesce(1)
     .write.format("com.databricks.spark.csv")
     .option("header", "true")
     .save("mydata.csv")
  }


}

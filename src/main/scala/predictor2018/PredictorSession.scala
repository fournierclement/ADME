package predictor
import lib.{Session}
import org.apache.spark.sql.{SparkSession}

class PredictorSession(filepath: String) extends Session {

  def runner(spark: SparkSession){
  // Read Json -> dataframe
  val dataframe = spark.read.json( filepath );
  }
}

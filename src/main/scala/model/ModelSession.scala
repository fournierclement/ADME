package model
import lib.{Session}
import org.apache.spark.sql.{SparkSession}
import lib.{Cleanser}

class ModelSession(filepath: String) extends Session {
  // Model creation
  // step to follow to create a model
  
  def runner(spark: SparkSession){
  // Read Json -> dataframe 
  val headers = Cleanser.headers
  val dataFrame = Cleanser.clean(spark.read.json( filepath ))
  
  // Create trainning DataFrame
  val trainingDF = dataFrame;
  // Create validation DataFrame
  val validationDF = dataFrame;
  // Create Model
  // val model = 
  // Test Model
  // Save model  
  }
}
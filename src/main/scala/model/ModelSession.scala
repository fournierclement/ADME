package model
import lib.{Session}
import org.apache.spark.sql.{SparkSession}
import lib.{Cleanser, AdmeCleanser}

class ModelSession(filepath: String) extends Session {
  // Model creation
  // step to follow to create a model
  
  def runner(spark: SparkSession){
  // Read Json -> dataframe 
  val dataFrame = AdmeCleanser(Cleanser(spark.read.json( filepath )))
  AdmeCleanser(new Cleanser(dataFrame)).show
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
package model
import lib.{Session}
import org.apache.spark.sql.{SparkSession}
import lib.{Cleanser, PickDataSets}

class ModelSession(filepath: String) extends Session {
  // Model creation
  // step to follow to create a model
  
  def runner(spark: SparkSession){
  // Read Json -> dataframe 
  val headers = Cleanser.headers
  val dataFrame = Cleanser.clean(spark.read.json( filepath ))
  // dataFrame.show(10)

  // Create trainning DataFrame
  val (trainingDataframe, validationDataframe) = PickDataSets(dataFrame, headers, 0.5)
  trainingDataframe.show(10)
  // Create validation DataFrame
  // Create Model

  // trainingDataframe.show(10)

  // val model = 
  // Test Model
  // Save model  
  }
}
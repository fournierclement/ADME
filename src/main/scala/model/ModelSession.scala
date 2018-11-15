
package model
import lib.{Session}
import org.apache.spark.mllib.evaluation.{RegressionMetrics, MulticlassMetrics}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.SparkContext
import lib.{Cleanser, PickDataSets, Model}

class ModelSession(filepath: String) extends Session {
  // Model creation
  // step to follow to create a model

  def runner(spark: SparkSession){
    // Read Json -> dataframe
    val headers = Cleanser.headers
    val dataFrame = Cleanser.clean(spark.read.json( filepath ))
    // dataFrame.show(10)
    dataFrame.show(100)
    // Create training DataFrame
    // Create validation DataFrame
    val (trainingDataframe, validationDataframe) = PickDataSets(dataFrame, headers, 0.5)
    trainingDataframe.show(100)
    // Create Model
    val model = Model.train(trainingDataframe)
    // Test Model
    val labelAndPreds = Model.validate(validationDataframe, model)
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / validationDataframe.count()
    val matrix = new MulticlassMetrics(labelAndPreds)

    // .count().toDouble / validationDataframe.count()
    println(s"Test Error = $testErr")
    // Confusion matrix
    println("Confusion matrix:")
    println(matrix.confusionMatrix)
    // Overall Statistics
    println("Summary Statistics")
    println(s"Accuracy = ${matrix.accuracy}")
    val metrics = new RegressionMetrics(labelAndPreds)
    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")
    // Save model
    model.save(spark.sparkContext, "./randomTreeModel")
  }
}

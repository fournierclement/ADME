package lib

import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

object Model {
  def dataFrameToLabeled(dataFrame: DataFrame): RDD[LabeledPoint] = {
    dataFrame.rdd.map(row => new LabeledPoint(
      row.getAs[Double]("label"),
      // import on the fly to avoid messing with build-in Vector
      org.apache.spark.mllib.linalg.Vectors.fromML(
        row.getAs[org.apache.spark.ml.linalg.SparseVector]("features")
      )
    ))
  }
  val floor = 0.060
  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 30 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"
  val maxDepth = 30
  val maxBins = 30
  def train(dataFrame: DataFrame): RandomForestModel = {
    val trainingData = dataFrameToLabeled(dataFrame)
    RandomForest.trainRegressor(
      trainingData,
      categoricalFeaturesInfo,
      numTrees,
      featureSubsetStrategy,
      impurity,
      maxDepth,
      maxBins
    )
  }
  def validate(dataFrame: DataFrame, model: RandomForestModel) = {
    dataFrame.rdd.map { row => {
      val prediction = model.predict(
        org.apache.spark.mllib.linalg.Vectors.fromML(
          row.getAs[org.apache.spark.ml.linalg.SparseVector]("features")
        )
      )
      (row.getAs[Double]("label"), if (prediction > floor) 1.0 else 0.0)
    }}
  }
}

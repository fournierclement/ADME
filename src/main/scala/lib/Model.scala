package lib

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.SparseVector
import scala.languageFeature.implicitConversions

object VectorConversions {
  import org.apache.spark.mllib.{linalg => mllib}
  import org.apache.spark.ml.{linalg => ml}

  implicit def toNewVector(v: mllib.Vector) = v.asML
  implicit def toOldVector(v: ml.Vector) = mllib.Vectors.fromML(v)
}

object Model {

  def rowFeaturesToVector(row: Row)= {
    // import on the fly to avoid messing with build-in Vector
    org.apache.spark.mllib.linalg.Vectors.fromML(
      row.getAs[org.apache.spark.ml.linalg.SparseVector]("features")
    )
  }

  def dataFrameToLabeled(dataFrame: DataFrame): RDD[LabeledPoint] = {
    dataFrame.rdd.map(row => new LabeledPoint(
      row.getAs[Double]("label"),
      rowFeaturesToVector(row)
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
      val prediction = model.predict( rowFeaturesToVector(row))
      (row.getAs[Double]("label"), if (prediction > floor) 1.0 else 0.0)
    }}
  }

  def predict(cleanDf: DataFrame, dataFrame: DataFrame, model: RandomForestModel) = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val assembler = new VectorAssembler()
      .setInputCols(Cleanser.headers)
      .setOutputCol("features")
    val output = assembler.transform(cleanDf)

    val func = udf( (features: org.apache.spark.ml.linalg.Vector) =>
      model.predict( Vectors.fromML(features)) > floor
    )

    output.withColumn("Label", func($"features")).drop("features", "size")

  }
}

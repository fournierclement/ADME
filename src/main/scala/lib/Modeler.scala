package lib

import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils

class Modeler {
  
//   // https://spark.apache.org/docs/2.3.0/mllib-ensembles.html#regression-1
//   boostingStrategy = BoostingStrategy.defaultParams("Regression")
//   boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
//   boostingStrategy.treeStrategy.maxDepth = 5
// // Empty categoricalFeaturesInfo indicates all features are continuous.
//   boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

//   val model = GradientBoostedTrees.train(trainingData, boostingStrategy)


//   // def dtToRdd(dataFrame: DataFrame): RDD[LabeledPoint] = dataFrame.transform(dataFrame).map( row => LabeledPoint(
//   //   row.getDouble(0),
//   //   row.getAs[org.apache.spark.mllib.linalg.Vector](4)
//   // ))

  // def apply(dataFrame: DataFrame): GradientBoostedTreesModel = GradientBoostedTrees.train(dtToRdd(dataFrame), boostingStrategy)
}
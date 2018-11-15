package lib
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.ChiSqSelector

/**
* @desc Pick row in given dataframe to create a Training DataFrame and a Validation DataFrame
* @param {DataFrame} dataframe, Inputed data set
* @param features array of features to consider
* @param trainingRate double between 0 and 1, the percent of total rows to use for training.
* @returns {(DataFrame, DataFrame)} (TrainningSet, ValidationSet)
*/
object PickDataSets {

   def PickDataSets( dataframe: DataFrame, features: Array[String], trainingRate: Double ) : (DataFrame, DataFrame) = {
    val weights = Array(trainingRate, 1.0-trainingRate);

    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    val output = assembler.transform(dataframe)
      .select("features", "clicked")


    val chiselector = new ChiSqSelector()
      .setNumTopFeatures(6)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setOutputCol("selectedFeatures")
      .fit(output)
      // selector.write.overwrite().save(selectorPath)
    val selected = chiselector.transform(dataframe)

    val spreadSet = selected.randomSplit( weights )
    val trainingDataframe = spreadSet(0);
    val validationDataframe = spreadSet(1);

    return (trainingDataframe, validationDataframe)
  }
}

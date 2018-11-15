package predictor
import lib._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.ml.util.MLWritable

class PredictorSession(jsonPath: String, modelPath: String, csvPath: String) extends Session {

  def runner(spark: SparkSession){
  // Read Json -> dataframe
  val dataFrame = spark.read.json( jsonPath )
  val model = RandomForestModel.load( spark.sparkContext, modelPath)
  val cleanDf = Cleanser.clean(dataFrame)
  // predicte
  val predicted = Model.predict(cleanDf, dataFrame, model)
  // Save csv
  // val partitionPredicted = predicted.coalesce(1)
  // // partitionPredicted.saveAsTextFile(csvPath)
  //  // predicted.asInstanceOf[MLWritable].write
  //  // // .format("com.databricks.spark.csv")
  //  // .option("header", "true")
  //  // .save(csvPath)
  // partitionPredicted
  // .write.csv(csvPath)
  predicted
     .repartition(1)
     .write.format("com.databricks.spark.csv")
     .option("header", "true")
     .save(csvPath)
  }
}

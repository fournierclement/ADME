package predictor
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{SparkSession}

object Main extends App {

  /**
  * @param jsonFilePath
  * @param modelDirectoryPath
  * @param outputFilePath
  */
  override def main(args: Array[String]): Unit = {
    if( args.isEmpty || args.length < 3) {throw new IllegalArgumentException("Missing argument(s). Provide path to json data, path to model, path to output file.")}
    else if( !Files.exists(Paths.get(args(0))) ) {throw new IllegalArgumentException("File path for json provided but unreadable")}
    else if( !Files.exists(Paths.get(args(1))) ) {throw new IllegalArgumentException("File path for model provided but unreadable")}
    else {
      // read Json
      new PredictorSession(args(0))

      // run
      // For test only
      val CORES = scala.util.Properties.envOrElse("SPARK_CORE", "*" )
      val MEMORY = scala.util.Properties.envOrElse("SPARK_RAM", "2g" )
      val spark = SparkSession.builder
        .master(s"local[$CORES]")
        .appName("ADME PREDICTOR")
        .config("spark.executor.memory", MEMORY)
        .getOrCreate()

      val result = spark.createDataFrame(Seq()).toDF();
      // Replace result with output from prediction

      // output to csv
      new WriteCsvSession(args(2), result)

    }
  }
}

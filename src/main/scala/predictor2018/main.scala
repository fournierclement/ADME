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
      new PredictorSession(args(0), args(1), args(2))
    }
  }
}

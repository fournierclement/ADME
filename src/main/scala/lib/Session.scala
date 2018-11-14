package lib
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession}

trait Session {

  Logger.getLogger("org").setLevel(Level.WARN)

  val CORES = scala.util.Properties.envOrElse("SPARK_CORE", "*" )
  val MEMORY = scala.util.Properties.envOrElse("SPARK_RAM", "2g" )

  val spark = SparkSession.builder
    .master(s"local[$CORES]")
    .appName("ADME PREDICTOR")
    .config("spark.executor.memory", MEMORY)
    .getOrCreate()

  def runner(spark: SparkSession): Unit

  runner(spark)
  spark.stop
}
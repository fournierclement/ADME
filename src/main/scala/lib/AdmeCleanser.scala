package lib
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

object AdmeCleanser {

  val mapIOS = udf { os: String => os.toUpperCase == "IOS" }
  val mapApp = udf { appOrSite: String => appOrSite.toUpperCase == "APP" }


  def apply(cleanser: Cleanser): DataFrame = {
    cleanser
    .mapColumn("os", "ios", mapIOS)
    .mapColumn("appOrSite", "app", mapApp)
    .dataFrame
  }
}
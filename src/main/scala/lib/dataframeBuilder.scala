package lib
import org.apache.spark.sql.DataFrame

/**
* @desc Pick row in given dataframe to create a Training DataFrame and a Validation DataFrame
* @param {DataFrame} dataframe, Inputed data set
* @param interests array of interests to consider
* @returns {(DataFrame, DataFrame)} (TrainningSet, ValidationSet)
*/
def pickDataSets( dataframe: DataFrame, interests: Array[String] ) : (DataFrame, DataFrame) = {
  var usedColumns = Array("size", "bidfloor", "exchange", "os", "publisher", "city", "daytime") ++ interests;
  var trainingDataframe = dataframe
    .select("label", usedColumns.head, usedColumns.tail: _*)

  var validationDataframe = dataframe
    .select(usedColumns.head, usedColumns.tail: _*)

  return (trainingDataframe, validationDataframe)
}

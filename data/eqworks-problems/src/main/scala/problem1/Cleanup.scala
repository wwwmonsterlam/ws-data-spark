/*
  Problem 1. Cleanup
*/

package problem1

import org.apache.spark.sql.{DataFrame, SparkSession}

object Cleanup {
  def solution(spark: SparkSession, dataSampleDf: DataFrame): DataFrame = {

//    val CLEANED_DATA_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\DataSampleCleaned"
    val TAR_PATH: String = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val CLEANED_DATA_PATH: String = TAR_PATH + "/DataSampleCleaned"

    println("******************** Problem 1. Cleanup begins ********************")

    val dfNoDuplication = dataSampleDf.dropDuplicates("TimeSt", "Longitude", "Latitude")

    // save the results
    dfNoDuplication.repartition(1).write.option("header", "true")
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .save(CLEANED_DATA_PATH)

    println("******************** Problem 1. Cleanup results ********************")
    dfNoDuplication.show

    dfNoDuplication
  }
}

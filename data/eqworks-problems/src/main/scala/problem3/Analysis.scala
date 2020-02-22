/*
  Problem 3. Analysis
*/

package problem3

import org.apache.spark.sql.{DataFrame, SparkSession}

object Analysis {
  def solution(spark: SparkSession, labeledDf: DataFrame): DataFrame = {

//    val POI_STAT_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\PoiStat"
    val TAR_PATH: String = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val POI_STAT_PATH: String = TAR_PATH + "/PoiStat"

    println("******************** Problem 3. Analysis begins ********************")

    labeledDf.createOrReplaceTempView("labeledDf")
    val sqlText = "SELECT Label, COUNT(Distance)/MAX(Distance)/MAX(Distance)/3.1415926 AS Density, " +
      "AVG(Distance) AS Avg, VARIANCE(Distance) AS Variance FROM labeledDf GROUP BY Label ORDER BY Label"
    val poiStatDf = spark.sql(sqlText)

    // save the results
    poiStatDf.repartition(1).write.option("header", "true")
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .save(POI_STAT_PATH)

    println("******************** Problem 3. Analysis results ********************")
    poiStatDf.show

    poiStatDf
  }
}

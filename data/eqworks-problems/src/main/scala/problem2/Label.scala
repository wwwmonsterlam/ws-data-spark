/*
  Problem 2. Label

  Introduction:
    Algorithm: brute force
    Time complexity: Θ(M*N), where M is the amount of POIs and N is the amount of requests

  Possible future improvement(It's not implemented due to limited time):
    Data structure: K-D tree
    Algorithm: nearest neighbour search
    Time complexity: O(M*N) for the worst case, O(M*lgN) for the average
*/

package problem2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Label {
  def solution(spark: SparkSession, dataSampleDf: DataFrame, poiDf: DataFrame): DataFrame = {

//    val LABELED_DATA_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\DataSampleLabeled"
    val TAR_PATH: String = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val LABELED_DATA_PATH: String = TAR_PATH + "/DataSampleLabeled"

    println("******************** Problem 2. Label begins ********************")

    val pois: Array[(String, (Double, Double))] = poiDf.rdd.map{row =>
      (row.get(0).toString, (row.get(1).toString.toDouble, row.get(2).toString.toDouble))
    }
      .collect()

    // for each sample, calculate the distances to different POIs and label the closes POI
    val labeledSample: RDD[Row] = dataSampleDf.rdd.map{row =>
      var closestPoi = ""
      var distance = Double.MaxValue
      val coordinate = (row.get(5).toString.toDouble, row.get(6).toString.toDouble)
      for(x <- pois) {
        val tmpDistance = this.getDistance(coordinate, x._2)
        if(tmpDistance < distance) {
          distance = tmpDistance
          closestPoi = x._1
        }
      }
      Row(row.get(0).toString, row.get(1).toString, row.get(2).toString, row.get(3).toString,
        row.get(4).toString, row.get(5).toString, row.get(6).toString, distance.toString, closestPoi)
    }

    // convert the data type from RDD to DataFrame
    val schemaString: String = "_ID,TimeSt,Country,Province,City,Latitude,Longitude,Distance,Label"
    val fields: Array[StructField] = schemaString.split(",")
      .map{fieldName => StructField(fieldName, StringType, nullable = true)}
    val schema: StructType = StructType(fields)
    val labeledDf: DataFrame = spark.createDataFrame(labeledSample, schema)

    // save the results
    labeledDf.repartition(1).write.option("header", "true")
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .save(LABELED_DATA_PATH)

    println("******************** Problem 2. Label results ********************")
    labeledDf.show

    labeledDf
  }

  // For simplification, the earth is regarded as a sphere with a radius of 6378138 meters.
  private def getDistance(coordinate1: (Double, Double), coordinate2: (Double, Double)): Double = {
    import math._
    val radianLat1 = coordinate1._1*Pi/180
    val radianLng1 = coordinate1._2*Pi/180
    val radianLat2 = coordinate2._1*Pi/180
    val radianLng2 = coordinate2._2*Pi/180
    val tmpA = pow(sin((radianLat1 - radianLat2)/2), 2)
    val tmpB = cos(radianLat1)*cos(radianLat2)*pow(sin((radianLng1 - radianLng2)/2), 2)
    6378138*2*asin(sqrt(tmpA + tmpB))
  }
}

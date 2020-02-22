import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import problem1.Cleanup
import problem2.Label
import problem3.Analysis
import problem4b.PipelineDependency

object EqworksProblems {

  val TAR_PATH: String = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath + "/.."
  val DATA_PATH: String = TAR_PATH + "/DataSample.csv"
  val POIID_PATH: String = TAR_PATH + "/POIList.csv"

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("solution").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // load data from DataSample.csv
    val dataSampleDf: DataFrame = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(DATA_PATH)

    // load data from POIID.csv
    val poiDf: DataFrame = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(POIID_PATH)
      .dropDuplicates("Longitude", " Latitude")

    // problem 1. Cleanup
    val dataSampleDfNoDuplication: DataFrame = Cleanup.solution(spark, dataSampleDf)

    // problem 2. Label
    val labeledDf: DataFrame = Label.solution(spark, dataSampleDfNoDuplication, poiDf)

    // problem 3. Analysis
    val poiStatDf: DataFrame = Analysis.solution(spark, labeledDf)

    // problem 4b. PipelineDependency
    val schedule: List[Int] = PipelineDependency.solution(spark)
  }
}

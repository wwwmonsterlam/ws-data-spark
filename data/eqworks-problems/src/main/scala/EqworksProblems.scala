import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import problem1.Cleanup
import problem2.Label
import problem3.Analysis
import problem4b.PipelineDependency

object EqworksProblems {

  val TAR_PATH: String = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
  val DATA_PATH: String = TAR_PATH + "/DataSample.csv"
  val POIID_PATH: String = TAR_PATH + "/POIID.csv"

//  val DATA_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\DataSample.csv"
//  val POIID_PATH: String = "C:\\Users\\wuwei\\Desktop\\IdeaProjects\\eqworks-problems\\data\\POIID.csv"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("solution").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // load data from DataSample.csv
    val dataSampleDf = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(DATA_PATH)

    // load data from POIID.csv
    val poiDf = spark.sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(POIID_PATH)
      .dropDuplicates("Longitude", "Latitude")

    // problem 1. Cleanup
    val dataSampleDfNoDuplication = Cleanup.solution(spark, dataSampleDf)

    // problem 2. Label
    val labeledDf = Label.solution(spark, dataSampleDfNoDuplication, poiDf)

    // problem 3. Analysis
    val poiStatDf = Analysis.solution(spark, labeledDf)

    // problem 4b. PipelineDependency
    val schedule = PipelineDependency.solution(spark)
  }
}

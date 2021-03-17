package fileReading

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import preprocessing._
import utils.Config
import scala.io.Source

class FileReadingSuite extends AnyFunSuite with BeforeAndAfter {

  var spark : SparkSession = _
  var sc: SparkContext = _

  def beforeEach() {
    spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("testFileReading")
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
  }

  test("test reading point file") {

    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = "datasets/cams.json"
    val res = ReadPointFile(fileName)
    println(res.take(5).deep)
  }

  test("test reading trajectory file") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 1000)
    trajRDD.take(5).foreach(println(_))
  }

  test("test reading trajectory json") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajRDD = ReadTrajJson("datasets/traj_100000_converted.json", 4)
    println(trajRDD.count)
    println(trajRDD.take(5).deep)
  }

  def afterEach() {
    spark.stop()
  }
}

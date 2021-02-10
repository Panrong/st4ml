package Converter

import convertion.Converter
import geometry.Rectangle
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.ReadTrajJson
import selection.partitioner.HashPartitioner
import selection.selector.RTreeSelector
import extraction.PointsAnalysisExtractor
import scala.io.Source

class ConverterSuite extends AnyFunSuite with BeforeAndAfter {

  var spark: SparkSession = _
  var sc: SparkContext = _

  def beforeEach() {
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    spark = SparkSession
      .builder()
      .master(config("master"))
      .appName(config("appName"))
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
  }

  test("test traj2point") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajRDD = ReadTrajJson("datasets/traj_100000_converted.json", 4)
    val sQuery = Rectangle(Array(115, 25, 125, 35))

    val partitioner = new HashPartitioner(4)
    val pRDD = partitioner.partition(trajRDD).cache()
    val partitionRange = partitioner.partitionRange
    val selector = new RTreeSelector(sQuery, partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()
    val pointRDD = converter.traj2Point(queriedRDD)

    println(s"--- converted to ${pointRDD.count} points")

    val extractor = new PointsAnalysisExtractor()
    val filteredPointRDD = pointRDD.filter(x => x.coordinates(0) > 115 && x.coordinates(1) > 25 && x.coordinates(0) < 125 && x.coordinates(1) < 35)
    extractor.extractMostFrequentPoints("tripID", 5)(pointRDD).foreach(println(_))
    println(s" ... Spatial range: ${extractor.extractSpatialRange(filteredPointRDD).mkString("Array(", ", ", ")")}")
    println(s" ... Temporal range: ${extractor.extractTemporalRange(filteredPointRDD).mkString("Array(", ", ", ")")}")
  }

  def afterEach() {
    spark.stop()
  }
}

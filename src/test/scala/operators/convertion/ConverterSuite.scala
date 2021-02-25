package operators.convertion

import operators.convertion.Converter
import geometry.Rectangle
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.{ReadPointFile, ReadTrajJson}
import operators.selection.partitioner.HashPartitioner
import operators.selection.selectionHandler.RTreeHandler
import operators.extraction.PointsAnalysisExtractor

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
      .master("local[*]")
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
    val selector = new RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()
    val pointRDD = converter.traj2Point(queriedRDD)

    println(s"--- converted to ${pointRDD.count} points")

    val extractor = new PointsAnalysisExtractor()
    val filteredPointRDD = pointRDD.filter(x => x.coordinates(0) > 115 && x.coordinates(1) > 25 && x.coordinates(0) < 125 && x.coordinates(1) < 35)
    println(" ... Top 5 most frequent:")
    extractor.extractMostFrequentPoints("tripID", 5)(pointRDD).foreach(x => println(s" ....  $x"))
    println(s" ... Spatial range: ${extractor.extractSpatialRange(filteredPointRDD).mkString("Array(", ", ", ")")}")
    println(s" ... Temporal range: ${extractor.extractTemporalRange(filteredPointRDD).mkString("Array(", ", ", ")")}")

    println(s" ... Temporal median (approx.): " +
      s"${extractor.extractTemporalQuantile(0.5)(filteredPointRDD).toLong}")
    println(s" ... Temporal 25% and 75% (approx.): " +
      s"${extractor.extractTemporalQuantile(Array(0.25, 0.75))(filteredPointRDD).map(_.toLong).mkString(", ")}")

    val newMoveIn = extractor.extractNewMoveIn(1598176021, 10)(filteredPointRDD)
    println(s" ... Number of new move-ins after time 1598176021 : ${newMoveIn.length}")

    val pr = extractor.extractPermanentResidents((1596882269, 1598888976), 200)(filteredPointRDD)
    println(s" ... Number of permanent residences : ${pr.length}")

    val abnormity = extractor.extractAbnormity()(filteredPointRDD)
    println(s" ... Number of abnormal ids : ${abnormity.length}")

  }
  test("test point to trajectory") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajRDD = ReadTrajJson("datasets/traj_100000_converted.json", 8)
    println(trajRDD.count)
    println(s"Average length:  ${trajRDD.map(_.points.length).aggregate(0)(_ + _, _ + _) / trajRDD.count}")
    val converter = new Converter
    val pointRDD = converter.traj2Point(trajRDD.map((0, _)))
    val convertedTrajRDD = converter.point2Traj(pointRDD)
    println(convertedTrajRDD.count)
    println(s"Average length:  ${convertedTrajRDD.map(_.points.length).aggregate(0)(_ + _, _ + _) / trajRDD.count}")
  }

  def afterEach() {
    spark.stop()
  }
}

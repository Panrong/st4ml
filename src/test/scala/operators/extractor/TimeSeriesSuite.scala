package operators.extractor

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.TimeSeriesExtractor
import operators.selection.partitioner.FastPartitioner
import operators.selection.selectionHandler.RTreeHandler
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.ReadTrajFile

import scala.io.Source

class TimeSeriesSuite extends AnyFunSuite with BeforeAndAfter {

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

  test("test time series extraction apps") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 10, 16, limit = true)
    val sQuery = Rectangle(Array(-9, 40, -8, 42))

    val partitioner = new FastPartitioner(8)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()

    val pointRDD = converter.traj2Point(queriedRDD).map((0, _))

    val tsRDD = converter.point2TimeSeries(pointRDD, 1372636854, 100)
    val extractor = new TimeSeriesExtractor
    val extractedRDD = extractor.extractByTime((1596364709, 1596374709))(tsRDD)
    val c = pointRDD.map(_._2).filter(x => x.timeStamp._1 >= 1372638000 && x.timeStamp._2 <= 1372639000)
    assert(extractedRDD.count == c.count)

    println(extractor.CountTimeSlotSamples((1372638000, 1372639000))(tsRDD).collect().mkString("Array(", ", ", ")"))
  }

  def afterEach() {
    spark.stop()
  }
}
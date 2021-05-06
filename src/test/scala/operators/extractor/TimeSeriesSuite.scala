package operators.extractor

import geometry.Rectangle
import operators.convertion.{Point2TimeSeriesConverter, Traj2PointConverter}
import operators.extraction.TimeSeriesExtractor
import operators.selection.partitioner.{FastPartitioner, QuadTreePartitioner}
import operators.selection.selectionHandler.RTreeHandler
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.ReadTrajFile
import setup.SharedSparkSession


class TimeSeriesSuite extends AnyFunSuite with SharedSparkSession {
  test("test time series extraction apps") {
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 10, 16, limit = true)
    val sQuery = Rectangle(Array(-9, 40, -8, 42))

    val partitioner = new FastPartitioner(8)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Traj2PointConverter()

    val pointRDD = converter.convert(queriedRDD)
    println(s"Number of points: ${pointRDD.count()}")

    val tsRDD = new Point2TimeSeriesConverter( 1372636854, 100, new QuadTreePartitioner(8, samplingRate = Some(1))).convert(pointRDD)
    println(s"Number of points inside all time series: ${tsRDD.map(_.count).sum}")

    val extractor = new TimeSeriesExtractor
    val extractedRDD = extractor.extractByTime((1372636854,1372637854))(tsRDD)
    val c = pointRDD.filter(x => x.timeStamp._1 >= 1372636854 && x.timeStamp._2 <= 1372637854)
    assert(extractedRDD.count == c.count)

    println(extractor.countTimeSlotSamples((1372636854,1372637854))(tsRDD).collect().mkString("Array(", ", ", ")"))
  }
}
package operators.extractor

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.TimeSeriesExtractor
import operators.selection.partitioner.FastPartitioner
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

    val converter = new Converter()

    val pointRDD = converter.traj2Point(queriedRDD).map((0, _))

    val tsRDD = converter.point2TimeSeries(pointRDD, 1372636854, 100)
    val extractor = new TimeSeriesExtractor
    val extractedRDD = extractor.extractByTime((1597119319,1597120219))(tsRDD)
    val c = pointRDD.map(_._2).filter(x => x.timeStamp._1 >= 1597119319 && x.timeStamp._2 <= 1597120219)
    assert(extractedRDD.count == c.count)

    println(extractor.countTimeSlotSamples((1597119319,1597120219))(tsRDD).collect().mkString("Array(", ", ", ")"))
  }
}
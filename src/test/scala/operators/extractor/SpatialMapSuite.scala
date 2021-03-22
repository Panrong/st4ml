package operators.extractor

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.SpatialMapExtractor
import operators.selection.partitioner.QuadTreePartitioner
import operators.selection.selectionHandler.RTreeHandler
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.{ReadTrajFile, ReadTrajJson}
import setup.SharedSparkSession
import utils.Config

class SpatialMapSuite extends AnyFunSuite with SharedSparkSession {

  test("spatial map range query"){
    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val trajRDD = ReadTrajJson(trajectoryFile, 8)
    val partitioner = new QuadTreePartitioner(8, Some(0.5))
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(Rectangle(Array(-180, -180, 180, 180)))
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()

    val pointRDD = converter.traj2Point(queriedRDD).map((0, _))
    val tStart = pointRDD.map(_._2.timeStamp).min._1
    val tEnd = pointRDD.map(_._2.timeStamp).max._2
    val spatialMapRDD = converter.point2SpatialMap(pointRDD, tStart, tEnd, partitionRange)

    val extractor = new SpatialMapExtractor()
    val sQuery = Rectangle(Array(118.116, 29.061, 120.167, 30.184))
    val tQuery = (1597000000L, 1598000000L)
    val extractedRDD = extractor.rangeQuery(spatialMapRDD, sQuery, tQuery)
    val gt = pointRDD.filter(p => p._2.inside(sQuery) && p._2.t <= tQuery._2 && p._2.t >= tQuery._1).count
    println(extractedRDD.count)
    println(gt)
    assert(extractedRDD.count == gt)
  }
}

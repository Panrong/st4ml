package operators.extractor

import geometry.Rectangle
import operators.convertion.{Point2SpatialMapConverter, Traj2PointConverter}
import operators.extraction.SpatialMapExtractor
import operators.selection.partitioner.QuadTreePartitioner
import operators.selection.selectionHandler.RTreeHandler
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.ReadTrajJson
import setup.SharedSparkSession
import utils.Config

class SpatialMapSuite extends AnyFunSuite with SharedSparkSession {

  test("spatial map range query") {
    val trajectoryFile = Config.get("hzData")
    val trajRDD = ReadTrajJson(trajectoryFile, 8)
    val partitioner = new QuadTreePartitioner(8, Some(0.5))
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(Rectangle(Array(-180, -180, 180, 180)))
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Traj2PointConverter()

    val pointRDD = converter.convert(queriedRDD)
    val tStart = pointRDD.map(_.timeStamp).min._1
    val tEnd = pointRDD.map(_.timeStamp).max._2
    val spatialMapRDD = new Point2SpatialMapConverter(tStart, tEnd, partitionRange).convert(pointRDD)

    val extractor = new SpatialMapExtractor()
    val sQuery = Rectangle(Array(118.116, 29.061, 120.167, 30.184))
    val tQuery = (1597000000L, 1598000000L)
    val extractedRDD = extractor.rangeQuery(spatialMapRDD, sQuery, tQuery)
    val gt = pointRDD.filter(p => p.inside(sQuery) && p.t <= tQuery._2 && p.t >= tQuery._1).count
    println(extractedRDD.count)
    println(gt)
    assert(extractedRDD.count == gt)
  }
}

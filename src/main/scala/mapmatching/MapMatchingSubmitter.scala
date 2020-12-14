package mapmatching

import geometry.{Point, Trajectory}
import graph.{RoadGraph, RoadGrid}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.math.{ceil, min}
import scala.reflect.ClassTag
import preprocessing.mmTrajectory

object MapMatchingSubmitter {
  def apply(trajFile: String,
            mapFile: String,
            numTraj: Int,
            batchSize: Int,
            resDir: String): MapMatchingSubmitter = {
    val rGrid = RoadGrid(mapFile)
    val trajDS = preprocessing.readTrajFile(trajFile,
      numTraj,
      clean = true,
      mapRange = List(rGrid.minLon, rGrid.minLat, rGrid.maxLon, rGrid.maxLat))
    new MapMatchingSubmitter(trajDS, rGrid, batchSize, resDir)
  }
}

class MapMatchingSubmitter(trajDS: Dataset[Trajectory],
                           rGrid: RoadGrid,
                           batchSize: Int,
                           resDir: String) extends Serializable {
  def run: Unit = {
    val trajRDD = trajDS.rdd.zipWithIndex()
    trajRDD.persist(StorageLevel.MEMORY_AND_DISK)
    val numTraj = trajRDD.count
    println("==== Start Map Matching")
    /** do map matching per batch */
    val totalBatch = ceil(numTraj.toDouble / batchSize).toInt
    for (batch <- 0 to totalBatch) {
      val batchTrajRDD = trajRDD.filter(x => x._2 >= batch * batchSize
        && x._2 < min((batch + 1) * batchSize, numTraj))
      val mapmatchedRDD = batchTrajRDD.map(x => x._1)
        .map(f = traj => {
          try {
            val candidates = MapMatcher.getCandidates(traj, rGrid)
            for (i <- candidates.keys) {
              if (candidates(i).length < 5) println("!!!\n" + traj.tripID)
            }
            val roadDistArray = MapMatcher.getRoadDistArray(candidates, rGrid)
            val res = MapMatcher(candidates, roadDistArray, rGrid)
            val cleanedPoints = res._1
            val idsWPoints = res._2
            val ids = idsWPoints.map(x => x._1)
            var pointRoadPair = new Array[(Double, Double, String)](0)
            if (ids(0) != "-1") {
              for (i <- cleanedPoints.indices) {
                pointRoadPair = pointRoadPair :+ (cleanedPoints(i).lon, cleanedPoints(i).lat, ids(i))
              }
            }
            val lx = cleanedPoints.map(_.lon).min
            val hx = cleanedPoints.map(_.lon).max
            val ly = cleanedPoints.map(_.lat).min
            val hy = cleanedPoints.map(_.lat).max
            val connRoadEdges = rGrid.getGraphEdgesByPoint(Point((lx, ly)), Point((hx, hy)))
            val rg = RoadGraph(connRoadEdges)
            val finalRes = MapMatcher.connectRoads(ids, rg)
            println("==== Map Matching Done")
            val roadTime = MapMatcher.genRoadSeg(finalRes.map(x => x._1), ids zip cleanedPoints)

            val candidatesRes = candidates.zipWithIndex.map {
              case ((_, v), i) => (i, v)
            }.mapValues(x => x.map(x => x._1)).toMap
            mmTrajectory(traj.tripID, traj.points, finalRes.toMap, candidatesRes, pointRoadPair, roadTime)
          }
          catch {
            case _: Throwable =>
              println("****")
              val candidates = MapMatcher.getCandidates(traj, rGrid)
              val candidatesRes = candidates.zipWithIndex.map {
                case ((_, v), i) => (i, v)
              }.mapValues(x => x.map(x => x._1)).toMap
              mmTrajectory(traj.tripID, traj.points, Map("-1"-> -1), candidatesRes, Array((-1,-1,"-1")), Array(("-1",-1L)))
          }
        })
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      spark.createDataset(mapmatchedRDD).write
        .option("header", value = true)
        .option("encoding", "UTF-8")
        .json(resDir + s"/$batch")
    }
  }

  implicit def tuple2Array[T: ClassTag](x: (T, T)): Array[T] = Array(x._1, x._2)

}

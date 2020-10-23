import main.scala.mapmatching.{MapMatcher, preprocessing}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.graph.{RoadGraph, RoadGrid}
import System.nanoTime

import org.apache.hadoop.fs.{FileSystem, Path}

import scala.reflect.io.Directory
import java.io.File

import main.scala.geometry.Point
import org.apache.spark.storage.StorageLevel

object RunMapMatching extends App {
  def timeCount = true

  override def main(args: Array[String]): Unit = {
    val directory = new Directory(new File(args(2)))
    directory.deleteRecursively()
    var t = nanoTime
    val tStart = t
    //set up spark environment
    val conf = new SparkConf()
    conf.setAppName("MapMatching_v1").setMaster(args(3))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    if (timeCount) {
      println("... Setting Spark up took: " + (nanoTime - t) / 1e9d + "s")
      t = nanoTime
    }
    val filename = args(0) //read file name from argument input
    val rGrid = RoadGrid(args(1))
    //    val rg = RoadGraph(rGrid.edges)
    if (timeCount) {
      println("... Generating road graph took: " + (nanoTime - t) / 1e9d + "s")
      t = nanoTime
    }
    val trajRDD = preprocessing(filename, List(rGrid.minLat, rGrid.minLon, rGrid.maxLat, rGrid.maxLon)).zipWithIndex()
    if (timeCount) {
      println("... Generating trajRDD took: " + (nanoTime - t) / 1e9d + "s")
      t = nanoTime
    }
    println("==== Start Map Matching")
    /** do map matching per batch */
    val totalTraj = args(4).toInt

    val mapmatchedRDD = sc.parallelize(trajRDD.take(totalTraj)).map(x => x._1)
      .map(f = traj => {
        try {
          val candidates = MapMatcher.getCandidates(traj, rGrid)
          for (i <- candidates.keys) {
            if (candidates(i).length < 5) println("!!!\n" + traj.tripID)
          }
          if (timeCount) {
            println("--- trip ID: " + traj.tripID)
            println("--- Total GPS points: " + traj.points.length)
            println("... Looking for candidates took: " + (nanoTime - t) / 1e9d + "s")
            t = nanoTime
          }
          val roadDistArray = MapMatcher.getRoadDistArray(candidates, rGrid)
          if (timeCount) {
            println("... Calculating road distances took: " + (nanoTime - t) / 1e9d + "s")
            t = nanoTime
          }
          val res = MapMatcher(candidates, roadDistArray, rGrid)
          if (timeCount) {
            println("... HMM took: " + (nanoTime - t) / 1e9d + "s")
            t = nanoTime
          }
          val cleanedPoints = res._1
          val idsWPoints = res._2
          val ids = idsWPoints.map(x => x._1)
          var pointRoadPair = ""
          if (ids(0) != "-1") {
            for (i <- cleanedPoints.indices) {
              pointRoadPair = pointRoadPair + (cleanedPoints(i).lon, cleanedPoints(i).lat, ids(i))
            }
          }

          val lx = cleanedPoints.map(_.lon).min
          val hx = cleanedPoints.map(_.lon).max
          val ly = cleanedPoints.map(_.lat).min
          val hy = cleanedPoints.map(_.lat).max
          val connRoadEdges = rGrid.getGraphEdgesByPoint(Point(lx, ly), Point(hx, hy))
          val rg = RoadGraph(connRoadEdges)
          val finalRes = MapMatcher.connectRoads(ids, rg)

          /** cal road speed */
          //val roadSpeed = MapMatcher.connectRoadsAndCalSpeed(idsWPoints, rg, rGrid)
          //println(roadSpeed.deep)
          //          catch{
          //            case _: Throwable => println("speed fault")
          //          }
          if (timeCount) {
            println("... Connecting road segments took: " + (nanoTime - t) / 1e9d + "s")
            println("==== Map Matching Done")
            t = nanoTime
          }
          var vertexIDString = ""
          for (v <- finalRes) vertexIDString = vertexIDString + "(" + v._1 + ":" + v._2.toString + ") "
          vertexIDString = vertexIDString.dropRight(1)
          var pointString = ""
          //for (i <- traj.points) pointString = pointString + "(" + i.long + " " + i.lat + ")"
          var o = "0"
          for (i <- candidates.keys.toArray) {
            if (cleanedPoints.contains(i)) o = "1"
            pointString = pointString + "(" + i.lon + " " + i.lat + " : " + o + ")"
          }
          var candidateString = ""
          for (i <- 0 until candidates.size) {
            val v = candidates.values.toArray
            val c = v(i)
            val r = c.map(x => x._1)
            candidateString = candidateString + i.toString + ":("
            for (rr <- r) candidateString = candidateString + rr + " "
            candidateString = candidateString + ");"
          }

          /** cal road speed */
          //          val roadSpeedString = roadSpeed.map(x=>(x._1,(x._2*3.6)formatted("%.2f"),x._3)).mkString(" ")
          val roadSpeedString = MapMatcher.genRoadSeg(finalRes.map(x => x._1), ids zip cleanedPoints)
            .deep
            .toString.drop(6).dropRight(1)
          Row(traj.taxiID.toString, traj.tripID.toString, pointString, vertexIDString, candidateString, pointRoadPair, roadSpeedString)
        }
        catch {
          case _: Throwable =>
            println("****")
            val candidates = MapMatcher.getCandidates(traj, rGrid)
            var pointString = ""
            //for (i <- traj.points) pointString = pointString + "(" + i.long + " " + i.lat + ")"
            var o = "0"
            for (i <- candidates.keys.toArray) {
              if (candidates.keys.toArray.contains(i)) o = "1"
              pointString = pointString + "(" + i.lon + " " + i.lat + " : " + o + ")"
            }
            var candidateString = ""
            for (i <- 0 until candidates.size) {
              val v = candidates.values.toArray
              val c = v(i)
              val r = c.map(x => x._1)
              candidateString = candidateString + i.toString + ":("
              for (rr <- r) candidateString = candidateString + rr + " "
              candidateString = candidateString + ");"
            }

            /** cal road speed */
            Row(traj.taxiID.toString, traj.tripID.toString, pointString, "(-1:-1)", candidateString, "-1", "-1")
          //Row(traj.taxiID.toString, traj.tripID.toString, pointString, "(-1:-1)", candidateString, "-1")
        }
      })
    val persistMapMatchedRDD = mapmatchedRDD.persist(StorageLevel.MEMORY_AND_DISK)

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    //    val df = persistMapMatchedRDD.map({
    //      case Row(val1: String, val2: String, val3: String, val4: String, val5: String, val6: String) => (val1, val2, val3, val4, val5, val6)
    //    }).toDF("taxiID", "tripID", "GPSPoints", "VertexID", "Candidates", "PointRoadPair")

    /** with speed info */
    val df = persistMapMatchedRDD.map({
      case Row(val1: String, val2: String, val3: String, val4: String, val5: String, val6: String, val7: String) => (val1, val2, val3, val4, val5, val6, val7)
    }).toDF("taxiID", "tripID", "GPSPoints", "VertexID", "Candidates", "PointRoadPair", "RoadTime")

    df.write
      .option("header", value = true)
      .option("encoding", "UTF-8")
      .partitionBy("taxiID")
      .csv(args(2))

    println("Total time: " + (nanoTime() - tStart) / 1e9d)
  }
}

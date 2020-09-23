import main.scala.mapmatching.MapMatcher._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}
import preprocessing._
import main.scala.graph.{RoadGraph, RoadGrid}
import System.nanoTime
import org.apache.hadoop.fs._

import scala.reflect.io.Directory
import java.io.File

object RunMapMatching extends App {
  def timeCount = true

  override def main(args: Array[String]): Unit = {
    val directory = new Directory(new File(args(2)))
    directory.deleteRecursively()
    var t = nanoTime
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
    val rg = RoadGraph(rGrid.edges)
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
    val batchSize = args(5).toInt
    val totalTraj = args(4).toInt
    for (i <- Range(0, totalTraj, batchSize)) {
      println(sc.parallelize(trajRDD.take(i + batchSize)).filter(x => x._2 >= i).count)
      val mapmatchedRDD = sc.parallelize(trajRDD.take(i + batchSize)).filter(x => x._2 > i).map(x => x._1)
        .map(traj => {
        try {
          val candidates = MapMatcher.getCandidates(traj, rGrid)
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
          val ids = res._2
          var pointRoadPair = ""
          if (ids(0) != "-1") {
            for (i <- 0 to cleanedPoints.length - 1) {
              pointRoadPair = pointRoadPair + (cleanedPoints(i).long, cleanedPoints(i).lat, ids(i))
            }
          }
          val finalRes = MapMatcher.connectRoads(ids, rg)
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
            pointString = pointString + "(" + i.long + " " + i.lat + " : " + o + ")"
          }
          var candidateString = ""
          for (i <- 0 to candidates.size - 1) {
            val v = candidates.values.toArray
            val c = v(i)
            val r = c.map(x => x._1.id)
            candidateString = candidateString + i.toString + ":("
            for (rr <- r) candidateString = candidateString + rr + " "
            candidateString = candidateString + ");"
          }
          Row(traj.taxiID.toString, traj.tripID.toString, pointString, vertexIDString, candidateString, pointRoadPair)
        } catch {
          case _: Throwable => {
            val candidates = MapMatcher.getCandidates(traj, rGrid)
            var pointString = ""
            //for (i <- traj.points) pointString = pointString + "(" + i.long + " " + i.lat + ")"
            var o = "0"
            for (i <- candidates.keys.toArray) {
              if (candidates.keys.toArray.contains(i)) o = "1"
              pointString = pointString + "(" + i.long + " " + i.lat + " : " + o + ")"
            }
            var candidateString = ""
            for (i <- 0 to candidates.size - 1) {
              val v = candidates.values.toArray
              val c = v(i)
              val r = c.map(x => x._1.id)
              candidateString = candidateString + i.toString + ":("
              for (rr <- r) candidateString = candidateString + rr + " "
              candidateString = candidateString + ");"
            }
            Row(traj.taxiID.toString, traj.tripID.toString, "-1", "-1", "-1", "-1")
          }
        }
      })
      //for (i <- mapmatchedRDD.collect) println(i)
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._

      val df = mapmatchedRDD.map({
        case Row(val1: String, val2: String, val3: String, val4: String, val5: String, val6: String) => (val1, val2, val3, val4, val5, val6)
      }).toDF("taxiID", "tripID", "GPSPoints", "VertexID", "Candidates", "PointRoadPair")

      //val df = mapmatchedRDD.toDF()

      val directory = new Directory(new File(args(2)+"/tmp"))
      directory.deleteRecursively()
      df.write.option("header", true).option("encoding", "UTF-8").csv(args(2)+"/tmp")
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val file = fs.globStatus(new Path(args(2) + "/tmp/part*"))(0).getPath().getName()
      fs.rename(new Path(args(2) + "/tmp/" + file), new Path(args(2) + "/" +i.toString + ".csv"))
    }

  }
}

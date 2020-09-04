import main.scala.mapmatching.MapMatcher._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}
import RStarTree.{Node, RTree, queryWithTable}
import preprocessing._
import main.scala.graph.RoadGraph
import System.nanoTime

object RunMapMatching extends App {
  def timeCount = true

  override def main(args: Array[String]): Unit = {
    var t = nanoTime
    val t0 = t
    //set up spark environment
    val conf = new SparkConf()
    //conf.setAppName("MapMatching_v1").setMaster("spark://Master:7077")
    conf.setAppName("MapMatching_v1").setMaster(args(3))
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    if (timeCount) {
      println("... Setting Spark up took: " + (nanoTime - t) / 1e9d + "s")
      t = nanoTime
    }
    val filename = args(0) //read file name from argument input
    val rg = RoadGraph(args(1))
    if (timeCount) {
      println("... Generating road graph took: " + (nanoTime - t) / 1e9d + "s")
      t = nanoTime
    }
    val trajRDD = preprocessing(filename, List(rg.minLat, rg.minLon, rg.maxLat, rg.maxLon))
    if (timeCount) {
      println("... Generating trajRDD took: " + (nanoTime - t) / 1e9d + "s")
      t = nanoTime
    }
    println("==== Start Map Matching")
    val mapmatchedRDD = sc.parallelize(trajRDD.take(10)).map(traj => {
      val candidates = MapMatcher.getCandidates(traj, rg)
      if (timeCount) {
        println("... Looking for candidates took: " + (nanoTime - t) / 1e9d + "s")
        t = nanoTime
      }
      val roadDistArray = MapMatcher.getRoadDistArray(candidates, rg)
      if (timeCount) {
        println("... Calculating road distances took: " + (nanoTime - t) / 1e9d + "s")
        t = nanoTime
      }
      val res = MapMatcher(candidates, roadDistArray, rg)
      if (timeCount) {
        println("... HMM took: " + (nanoTime - t) / 1e9d + "s")
        t = nanoTime
      }
      val cleanedPoints = res._1
      val ids = res._2
      var pointRoadPair = ""
      if(ids(0)!="-1") {
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
    })
    for (i <- mapmatchedRDD.collect) println(i)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = mapmatchedRDD.map({
      case Row(val1: String, val2: String, val3: String, val4: String, val5: String, val6: String) => (val1, val2, val3, val4, val5, val6)
    }).toDF("taxiID", "tripID", "GPSPoints", "VertexID", "Candidates", "PointRoadPair")

    //val df = mapmatchedRDD.toDF()
    import scala.reflect.io.Directory
    import java.io.File
    val directory = new Directory(new File(args(2)))
    directory.deleteRecursively()
    df.write.option("header", true).option("encoding", "UTF-8").csv(args(2))





    /*
    val traj = trajRDD.take(4)(0)
    println(traj.points.deep)
    val candidates = MapMatcher.getCandidates(traj, rg)
    val roadDistArray = MapMatcher.getRoadDistArray(candidates, rg)
    val ids = MapMatcher(candidates, roadDistArray)
    val vertexIDs = MapMatcher.connectRoads(ids,rg)
    println(vertexIDs.deep)

     */
  }
}

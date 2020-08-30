import main.scala.mapmatching.MapMatcher._
import org.apache.spark.{SparkConf, SparkContext}
import RStarTree.{Node, RTree, queryWithTable}
import dijkstra.{DirectedEdge, EdgeWeightedDigraph}
import preprocessing._
import main.scala.mapmatching.SpatialClasses._
import main.scala.graph.RoadGraph
import _root_.dijkstra._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object RunMapMatching extends App {
  override def main(args: Array[String]): Unit = {
    val filename = args(0) //read file name from argument input
    //val filename = "/datasets/porto.csv"
    //set up spark environment
    val conf = new SparkConf()
    //conf.setAppName("MapMatching_v1").setMaster("spark://Master:7077")
    conf.setAppName("MapMatching_v1").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rg = RoadGraph("C:\\Users\\kaiqi001\\Documents\\GitHub\\spark-map-matching\\preprocessing\\porto.csv")
    val trajRDD = preprocessing(filename,  List(rg.minLat, rg.minLon, rg.maxLat, rg.maxLon))
    /*
    val mapmatchedRDD = trajRDD.take(10).map(traj => {
      val candidates = MapMatcher.getCandidates(traj, rg)
      val roadDistArray = MapMatcher.getRoadDistArray(candidates, rg)
      val ids = MapMatcher(candidates, roadDistArray)
      val vertexIDs = MapMatcher.connectRoads(ids,rg)
    })
    
     */



    val traj = trajRDD.take(4)(0)
    println(traj.points.deep)
    val candidates = MapMatcher.getCandidates(traj, rg)
    val roadDistArray = MapMatcher.getRoadDistArray(candidates, rg)
    val ids = MapMatcher(candidates, roadDistArray)
    val vertexIDs = MapMatcher.connectRoads(ids,rg)
    println(vertexIDs.deep)
  }
}

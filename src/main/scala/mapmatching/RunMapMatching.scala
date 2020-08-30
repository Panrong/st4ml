import main.scala.mapmatching.MapMatcher._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}
import RStarTree.{Node, RTree, queryWithTable}
import preprocessing._
import main.scala.graph.RoadGraph
import scala.reflect.io.Directory
import java.io.File
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

    val mapmatchedRDD = sc.parallelize(trajRDD.take(10)).map(traj => {
      val candidates = MapMatcher.getCandidates(traj, rg)
      val roadDistArray = MapMatcher.getRoadDistArray(candidates, rg)
      val ids = MapMatcher(candidates, roadDistArray)
      val vertexIDs = MapMatcher.connectRoads(ids,rg)
      var vertexIDString = ""
      for(v <- vertexIDs) vertexIDString = vertexIDString + v + " "
      vertexIDString = vertexIDString.dropRight(1)
      var pointString = ""
      for(i <- traj.points) pointString = pointString +"(" + i.lat + " " + i.long + ")"
      traj.taxiID.toString +","+ traj.tripID.toString +","+ pointString +","+ vertexIDString
    })
    for(i <- mapmatchedRDD.collect) println(i)

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = mapmatchedRDD.toDF()
    df.write.option("header", true).option("encoding", "UTF-8").text("file:///C://Users//kaiqi001//Desktop//res")



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

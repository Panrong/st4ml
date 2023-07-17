import org.apache.spark.sql.SparkSession
import st4ml.instances.{Extent, Trajectory}
import st4ml.operators.selector.SelectionUtils.readOsm
import st4ml.operators.selector.Selector
import st4ml.utils.mapmatching.MapMatcher
import st4ml.utils.mapmatching.road.RoadGraph

object MmTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MapMatchingExample")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val map = readOsm("./preprocessing/porto3")
    val sRange = Extent(map.minLon, map.minLat, map.maxLon, map.maxLat).toPolygon

    val selector = new Selector(sRange)
    val trajs = selector.selectTrajCSV("./datasets/porto_toy10.csv").filter(x => sRange.contains(x.extent))
      .take(10)
      .zipWithIndex.map{x =>
      if (x._2 == 2 || x._2 == 6) x._1.drop(8)
      else if (x._2 == 9) x._1.drop(17)
      else x._1.drop(0)
    }
    for (traj <- trajs) {
      val mapMamatcher = new MapMatcher(map)
      val mmTraj = mapMamatcher.mapMatchWithInterpolation(traj, candidateThresh = 50)
      val x = (traj, mmTraj)
      val res = (x._1.entries.map(x => s"(${x.spatial.getX} ${x.spatial.getY})").mkString(" "),
        x._2.entries.map(x => x.value).mkString(" "))
      println(s"${res._1}, ${res._2}")
    }
    sc.stop()
  }

}


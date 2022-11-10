package experiments

import st4ml.instances.{Duration, Entry, Extent, Trajectory, Point}
import st4ml.operators.selector.Selector
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

import java.lang.System.nanoTime
import scala.io.Source
import scala.reflect.ClassTag

object TrajStayPointExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val queryFile = args(2)
    val numPartitions = args(3).toInt
    val maxDist = args(4).toDouble
    val minTime = args(5).toInt

    val spark = SparkSession.builder()
      .appName("trajStayPoint")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    val t = nanoTime()
    type TRAJ = Trajectory[Option[String], String]

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[TRAJ](spatial, temporal, numPartitions)
      val trajRDD = selector.selectTraj(fileName, metadata, false)
      val stayPointRDD = trajRDD.map(x => (x.data, findStayPoint(x, maxDist, minTime)))
      val res = stayPointRDD.collect
      trajRDD.unpersist()
      println(res.take(5).deep)
    }
    println(s"Stay point extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()

    def findStayPoint[T <: Trajectory[_, _] : ClassTag](traj: T, maxDist: Double, minTime: Int): Array[Point] = {
      if (traj.entries.length < 2) return new Array[Point](0)
      import st4ml.instances.GeometryImplicits._
      val entries = traj.entries.toIterator
      var anchor = entries.next
      var res = new Array[Point](0)
      var tmp = new Array[Entry[Point, _]](0)
      while (entries.hasNext) {
        val candidate = entries.next
        if (anchor.spatial.greatCircle(candidate.spatial) < maxDist) tmp = tmp :+ candidate
        else {
          if (tmp.length > 0 && tmp.last.duration.end - anchor.duration.start > minTime)
            res = res :+ anchor.spatial
          anchor = candidate
          tmp = new Array[Entry[Point, _]](0)
        }
      }
      res
    }
  }
}

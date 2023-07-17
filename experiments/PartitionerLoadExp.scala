// For table 5

package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Event, Extent, Point, Trajectory}
import st4ml.operators.selector.Selector
import st4ml.utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object PartitionerLoadExp {
  def main(args: Array[String]): Unit = {
    val t = nanoTime
    val mode =args(0)
    val fileName = args(1)
    val metadata = args(2)
    val queryFile = args(3)
    val spark = SparkSession.builder()
      .appName("PartitionerComparison")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    if (mode == "event"){
      for((sQuery, tQuery) <- ranges) {
        val selector = new Selector[Event[Point, Option[String], String]](sQuery, tQuery, 256)
        val selectedRDD = selector.selectEvent(fileName, metadata)
        println(selectedRDD.count)
      }
    }
    if (mode == "traj"){
      for((sQuery, tQuery) <- ranges) {
        val selector = new Selector[Trajectory[Option[String], String]](sQuery, tQuery, 256)
        val selectedRDD = selector.selectTraj(fileName, metadata)
        println(selectedRDD.count)
      }
    }
    println(s"${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}

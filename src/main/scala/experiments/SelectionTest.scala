package experiments

import st4ml.instances.{Duration, Extent, Point, Polygon, RTree}
import st4ml.operators.selector.MultiRangeSelector
import st4ml.operators.selector.SelectionUtils.E
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object SelectionTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("LoadingTest")
      .master(Config.get("master"))
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    val lim = args(3).toInt
    val m = args(4)
    val sQuery = readQuery(queryFile)
    val t = nanoTime()
    if (m == "1") {
      val selector = MultiRangeSelector(sQuery.take(lim), Duration(0, 13727368000L), numPartitions)
      val r = selector.selectEventWithInfo(fileName, "None")
      println(r.flatMapValues(x => x).count)
      println(s"multi time: ${(nanoTime - t) * 1e-9} s")
    }
    else if (m == "2") {
      // build index
      val rdd = spark.read.parquet(fileName).as[E].toRdd
      val rTreeRDD = rdd.mapPartitions {
        x =>
          val events = x.toArray.zipWithIndex.map { y =>
            (y._1.spatialCenter, y._2.toString, y._2)
          }
          Iterator(RTree[Point](events, scala.math.sqrt(events.length).toInt))
      }
      println(sQuery.take(lim).map(s => rTreeRDD.map(x => x.range(s)).count).sum)
      println(s"rtree time: ${(nanoTime - t) * 1e-9} s")
    }
  }

  def readQuery(f: String): Array[Polygon] = {
    var s = new Array[Polygon](0)
    for ((line, i) <- (Source.fromFile(f).getLines).zipWithIndex) {
      val r = line.split(" ")
      s = s :+ Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon
    }
    s
  }
}

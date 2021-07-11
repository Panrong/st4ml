package experiments

import experiments.flowExp.{genGrids, genSTGrids}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.Config

object flowSpark {
  /**
   * divide the whole spatial range into grids and find how many points inside each grid
   * for every hour
   */

  case class Point(lon: Double, lat: Double, t: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("flowSpark")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /**
     * "-8.65, 41.13, -8.57, 41.17" "1372636800,1404172800" 5 3600
     */
    val sQuery = args(0).split(",").map(_.toDouble)
    val tQuery = args(1).split(",").map(_.toLong)
    val sSize = args(2).toInt
    val tSplit = args(3).toInt
    val grids = genGrids(sQuery, sSize)
    val stGrids = genSTGrids(grids, (tQuery(0), tQuery(1)), tSplit)
    val pointFile =args(4)

    val stGridMap = stGrids.zipWithIndex.map(_.swap).toMap
    val stGridMapReversed = stGrids.zipWithIndex.toMap

    val pointRDD = readPointCsv(pointFile)
    println(pointRDD.count)
    val gridRDD = pointRDD.map(point => {
      stGrids.filter { case (s, t) =>
        point.lon >= s(0) &&
          point.lat >= s(1) &&
          point.lon <= s(2) &&
          point.lat <= s(3) &&
          point.t >= t(0) &&
          point.t <= t(1)
      }
    })
    val resRDD = gridRDD.filter(x => x.length > 0)
      .map(x => (stGridMapReversed(x.head), 1))
      .reduceByKey(_ + _)
      .map(x => (stGridMap(x._1), x._2))
    //    resRDD.take(5).foreach(x => println(x._1._1.deep, x._1._2.deep, x._2))

    resRDD.collect.sortBy(x => (x._1._1(0), x._1._1(1), x._1._1(2), x._1._1(3), x._1._2(0)))
      .foreach(x => println(x._1._1.deep, x._1._2.deep, x._2))
    println(s"Total Points: ${resRDD.map(_._2).sum}")
    sc.stop()
  }

  def readPointCsv(fileName: String): RDD[Point] = {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.csv(fileName)
    df.rdd.map(row => Point(row(0).asInstanceOf[String].toDouble,
      row(1).asInstanceOf[String].toDouble,
      row(2).asInstanceOf[String].toLong))
  }
}

package experiments

import experiments.flowExp.{genGrids, genSTGrids}
import org.apache.spark.sql.SparkSession
import preprocessing.ReadParquet
import utils.Config

object flowSpark {
  /**
   * divide the whole spatial range into grids and find how many points inside each grid
   * for every hour
   */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SelectorExp")
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
    val pointFile = Config.get("portoPoints")

    val pointRDD = ReadParquet.ReadFaceParquet(pointFile)
    val gridRDD = pointRDD.map(point => {
      stGrids.filter { case (s, t) =>
        point.lon >= s(0) &&
          point.lat >= s(1) &&
          point.lon <= s(2) &&
          point.lat <= s(3) &&
          point.timeStamp._1 >= t(0) &&
          point.timeStamp._1 <= t(1)
      }
    })
    val resRDD = gridRDD.filter(x => x.length > 0).map(x => (x.head, 1))
      .groupByKey()
      .mapValues(_.sum)

    resRDD.take(5).foreach(x => println(x._1._1.deep, x._1._2.deep, x._2))
    sc.stop()

  }
}

package experiments

import instances.{Duration, Event, Point}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}
import utils.Config

import scala.math.{abs, acos, cos, sin}

object TrajSpeedSpark {

  case class Trajectory(points: Array[((Double, Double), Long)], id: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("flowExp")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /**
     * "C:\\Users\\kaiqi001\\Documents\\GitHub\\geomesa-fs_2.12-3.2.0\\face-point\\09_W964092771efa4a4b87c1c75d5c79d6ec.parquet" "-9.137,38.715,-7.740,41.523" "1372639359,1372755736" "5" "3600"
     */
    val sQuery = args(1).split(",").map(_.toDouble)
    val tQuery = args(2).split(",").map(_.toLong)
    val sSize = args(3).toInt
    val tSplit = args(4).toInt
    val grids = genGrids(sQuery, sSize)
    val stGrids = genSTGrids(grids, (tQuery(0), tQuery(1)), tSplit)
    val pointFile = args(0)

    val trajRDD = readGeoMesaTraj(pointFile)
    trajRDD.cache()
    var res = new Array[(Array[Double], Array[Long], Double)](0)

    for (query <- stGrids) {
      val tQuery = (query._2(0), query._2(1))
      val sQuery = (query._1(0), query._1(1), query._1(2), query._1(3))
      //      val countRDD = trajRDD.map(x => {
      //        x.points.zipWithIndex.count {
      //          case ((s, t), _) => {
      //            s._1 >= sQuery._1 && s._2 >= sQuery._2 && s._1 <= sQuery._3 && s._2 <= sQuery._4 &&
      //              t >= tQuery._1 && t <= tQuery._2
      //          }
      //        }
      //      })
      //      println(countRDD.sum())

      val results = trajRDD.map(x => {
        val pointsInside = x.points.zipWithIndex.filter {
          case ((s, t), _) => {
            s._1 >= sQuery._1 && s._2 >= sQuery._2 && s._1 <= sQuery._3 && s._2 <= sQuery._4 &&
              t >= tQuery._1 && t <= tQuery._2
          }
        }
        if (pointsInside.length < 2) None
        else {
          val distance = greatCircleDistance(pointsInside.last._1._1, pointsInside.head._1._1)
          val duration = pointsInside.last._1._2 - pointsInside.head._1._2
          Some(distance / duration)
        }
      })

      val validRDD = results.filter(_.isDefined).map(_.get)
      val avgSpeed = if (validRDD.count > 0) validRDD.filter(_ > 0).reduce(_ + _) / validRDD.count else 0
      res = res :+ (query._1, query._2, avgSpeed)
    }

    res.foreach(x => println(x._1.deep, x._2.mkString("Array(", ", ", ")"), x._3))

    sc.stop()
  }

  def genGrids(range: Array[Double], size: Int): Array[Array[Double]] = {
    val lonMin = range(0)
    val latMin = range(1)
    val lonMax = range(2)
    val latMax = range(3)
    val lons = ((lonMin until lonMax by (lonMax - lonMin) / size) :+ lonMax).sliding(2).toArray
    val lats = ((latMin until latMax by (latMax - latMin) / size) :+ latMax).sliding(2).toArray
    lons.flatMap(x => lats.map(y => Array(x(0), y(0), x(1), y(1))))
  }

  def genSTGrids(grids: Array[Array[Double]], tRange: (Long, Long), tSplit: Int): Array[(Array[Double], Array[Long])] = {
    val tSlots = ((tRange._1 until tRange._2 by tSplit.toLong).toArray :+ tRange._2).sliding(2).toArray
    grids.flatMap(grid => tSlots.map(t => (grid, t)))
  }

  def readGeoMesaTraj(file: String): RDD[Trajectory] = {
    val spark = SparkSession.builder().getOrCreate()

    def getPoints(points: String): Array[(Double, Double)] = {
      val coords = points.split("],")
      val xs = coords(0).replace("[", "").replace("]", "").split(",").map(_.toDouble)
      val ys = coords(1).replace("[", "").replace("]", "").split(",").map(_.toDouble)
      xs zip ys
    }

    //    def getPoints(x: (Array[Double], Array[Double])) : Array[(Double, Double)] = x._1 zip x._2

    def getTimeStamps(length: Int, tStart: Long, tInterval: Int = 15): Array[Long] = {
      (0 until length).toArray.map(x => tStart + x * tInterval)
    }

    val rdd = spark.read.parquet(file)
      .filter("fid is not null")
      .filter("points is not null")
      .filter("timestamp is not null")
      .withColumn("pointString", col("points").cast("string"))
      .select("fid", "timestamp", "pointString")
      .rdd

    rdd.map(row => {
      val id = row.getString(0)
      val pointString = row.getString(2)
      val tStart = row.getLong(1)
      val points = getPoints(pointString)
      val timeStamps = getTimeStamps(points.length, tStart, 15)
      Trajectory(points zip timeStamps, id)
    })
  }

  def greatCircleDistance(p1: (Double, Double), p2: (Double, Double)): Double = {
    val x1 = p1._1
    val x2 = p2._1
    val y1 = p1._2
    val y2 = p2._2
    val r = 6371009 // earth radius in meter
    val phi1 = y1.toRadians
    val lambda1 = x1.toRadians
    val phi2 = y2.toRadians
    val lambda2 = x2.toRadians
    val deltaSigma = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda2 - lambda1)))
    r * deltaSigma
  }

}

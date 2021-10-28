package experiments

import instances.{Duration, Extent, Point, Trajectory}
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.math.sqrt

object LoadingTest extends App {
  var spark = SparkSession.builder()
    .appName("LoadingTest")
    .master(Config.get("master"))
    .getOrCreate()

  var sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val fileName = args(0)
  val m = args(1)
  var t = nanoTime
  val sRange = args(2).split(",").map(_.toDouble) // -8.446832 41.010165 -7.932837 41.381359
  val tRange = args(3).split(",").map(_.toLong)
  val ratio = args(4).toDouble
  val wholeSpatial = Extent(sRange(0), sRange(1), sRange(2), sRange(3))
  val wholeTemporal = Duration(tRange(0), tRange(1))
  println(ratio)
  val random = new scala.util.Random(1)

  if (m == "csv") {
    val start1 = random.nextDouble * (1 - sqrt(ratio))
    val start2 = random.nextDouble * (1 - sqrt(ratio))
    val start3 = random.nextDouble * (1 - ratio)
    val spatial = Extent(wholeSpatial.xMin + start1 * (wholeSpatial.xMax - wholeSpatial.xMin),
      wholeSpatial.yMin + start2 * (wholeSpatial.yMax - wholeSpatial.yMin),
      wholeSpatial.xMin + (start1 + sqrt(ratio)) * (wholeSpatial.xMax - wholeSpatial.xMin),
      wholeSpatial.yMin + (start2 + sqrt(ratio)) * (wholeSpatial.yMax - wholeSpatial.yMin)).toPolygon
    val temporal = Duration(wholeTemporal.start + (start3 * (wholeTemporal.end - wholeTemporal.start)).toLong,
      wholeTemporal.start + ((start3 + ratio) * (wholeTemporal.end - wholeTemporal.start)).toLong)
    val df = spark.read.option("header", "true")
      .option("numPartitions", 256)
      .csv(fileName)

    val trajRDD = df.rdd.filter(row => row(8).toString.split(',').length >= 4)

    val resRDD = trajRDD.map(row => {
      try {
        val tripID = row(0).toString
        val startTime = row(5).toString.toLong
        val pointsString = row(8).toString
        val pointsCoord = pointsString.split(",").map(x => x.replaceAll("[\\[\\]]", "").toDouble)
        val points = pointsCoord.sliding(2, 2).map(x => Point(x(0), x(1))).toArray
        val ts = points.indices.map(x => Duration(x * 15 + startTime)).toArray
        Some(Trajectory(points, ts, points.indices.map(_ => None).toArray, tripID))
      }
      catch {
        case _: Throwable =>
          None
      }
    })

    val filteredRDD = resRDD.filter(_.isDefined).filter(_.get.intersects(spatial, temporal))
    println(filteredRDD.count)
    println((nanoTime - t) * 1e-9)
  }
  else {
    val dirs = Array(0, 1, 2, 3).map(x => fileName + s"/pId=$x")
    val df2 = spark.read.parquet(dirs: _*)
    //    val df2 = spark.read.parquet(fileName).filter(col("pId").isin(Array(0, 1, 2, 3, 4): _*))
    println(df2.count)
    println((nanoTime - t) * 1e-9)
  }
  sc.stop()
}

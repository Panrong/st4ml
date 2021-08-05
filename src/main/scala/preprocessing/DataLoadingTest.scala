package preprocessing

import instances.{Duration, Extent, Point, Trajectory}
import operatorsNew.selector.DefaultSelector
import org.apache.spark.sql.SparkSession
import preprocessing.geoJson2ParquetTraj.T
import utils.Config

import java.lang.System.nanoTime

object DataLoadingTest {
  def main(args: Array[String]): Unit = {
    val t = nanoTime
    val spark = SparkSession.builder()
      .appName("DataLoadingTest")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val spatialRange = args(1).split(",").map(_.toDouble)
    val temporalRange = args(2).split(",").map(_.toLong)
    val numPartitions = args(3).toInt

    val sQuery = new Extent(spatialRange(0), spatialRange(1), spatialRange(2), spatialRange(3))
    val tQuery = Duration(temporalRange(0), temporalRange(1))


    // read parquet
    val readDs = spark.read.parquet(fileName)

    import spark.implicits._
    val trajectoryRDD = readDs.as[T].rdd.map(x => {
      val pointArr = x.entries.map(e => Point(e.lon, e.lat)).toArray
      val durationArr = x.entries.map(x => Duration(x.t)).toArray
      val valueArr = x.entries.map(_ => None).toArray
      Trajectory(pointArr, durationArr, valueArr, x.id)
    })

    val selector = new DefaultSelector[Trajectory[None.type, String]](sQuery, tQuery, numPartitions)

    val res = selector.query(trajectoryRDD).collect

    println(fileName)
    println((nanoTime-t) * 1e-9)

  }
}

package experiments

import experiments.TrajConversionTest.T
import instances.{Duration, Point, Trajectory}
import operatorsNew.selector.DefaultSelector
import org.apache.spark.sql.SparkSession
import utils.Config

object TrajRangeQuery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("flowExp")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    // read parquet
    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })
    val lonMin = trajRDD.map(_.extent.xMin).filter(x => x > -10 && x < -6).min
    val latMin = trajRDD.map(_.extent.yMin).filter(x => x > 35 && x < 45).min
    val lonMax = trajRDD.map(_.extent.xMax).filter(x => x > -10 && x < -6).max
    val latMax = trajRDD.map(_.extent.yMax).filter(x => x > 35 && x < 45).max

    println(lonMin, latMin, lonMax, latMax)
    sc.stop()
  }
}

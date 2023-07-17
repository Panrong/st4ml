// For enlarge the porto traj dataset

package experiments

import st4ml.instances.{Duration, Entry, Point, Trajectory}
import st4ml.operators.selector.SelectionUtils.T
import org.apache.spark.sql.SparkSession
import st4ml.operators.selector.SelectionUtils._

object AddNoise {
  def main(args: Array[String]): Unit = {
    val fileDir = args(0)
    val resDir = args(1)
    val deviation1 = args(2).toDouble //1.8018e-4, 20m
    val deviation2 = args(3).toDouble //120
    val copies = args(4).toInt
    val r = new scala.util.Random

    val spark = SparkSession.builder()
      .appName("addNoise")
      //.master("local[4]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    val trajRDD = spark.read.parquet(fileDir).drop("pId").as[T]
      .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])
    //    println(trajRDD.count)
    //    println(trajRDD.map(_.extent.xMin).min, trajRDD.map(_.extent.yMin).min, trajRDD.map(_.extent.xMax).max, trajRDD.map(_.extent.yMax).max)

    val noiseRDD = trajRDD.flatMap(x => new Array[x.type](copies).map(_ => x)).map { traj =>
      val entries = traj.entries.map(entry => new Entry(addNoise(entry.spatial, deviation1, r),
        addNoise(entry.temporal, deviation2, r), entry.value)).sortBy(_.temporal.start)
      new Trajectory(entries, traj.data)
    }
    noiseRDD.toDs().toDisk(resDir)
    val trajRDD2 = spark.read.parquet(resDir).drop("pId").as[T]
      .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])
    //    println(trajRDD2.count)
    //    println(trajRDD2.map(_.extent.xMin).min, trajRDD2.map(_.extent.yMin).min, trajRDD2.map(_.extent.xMax).max, trajRDD2.map(_.extent.yMax).max)
    sc.stop()
  }

  def addNoise(point: Point, deviation: Double, r: scala.util.Random): Point = {
    val x = point.getX + r.nextGaussian * deviation
    val y = point.getY + r.nextGaussian * deviation
    Point(x, y)
  }

  def addNoise(duration: Duration, deviation: Double, r: scala.util.Random): Duration = {
    Duration((duration.start + r.nextGaussian * deviation).toLong)
  }
}

package experiments

import instances.{Entry, Point, Trajectory}
import operatorsNew.selector.SelectionUtils.T
import org.apache.spark.sql.SparkSession
import utils.Config
import operatorsNew.selector.SelectionUtils._

object AddNoise {
  def main(args: Array[String]): Unit = {
    val fileDir = args(0)
    val resDir = args(1)
    val deviation = args(2).toDouble //1.8018e-4

    val r = new scala.util.Random

    val spark = SparkSession.builder()
      .appName("addNoise")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    val trajRDD = spark.read.parquet(fileDir).drop("pId").as[T]
      .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])

    val noiseRDD = trajRDD.map { traj =>
      val entries = traj.entries.map(entry => new Entry(addNoise(entry.spatial, deviation, r), entry.temporal, entry.value))
      new Trajectory(entries, traj.data)
    }
    noiseRDD.toDs().toDisk(resDir)
  }

  def addNoise(point: Point, deviation: Double, r: scala.util.Random): Point = {
    val x = point.getX + r.nextGaussian * deviation
    val y = point.getY + r.nextGaussian * deviation
    Point(x, y)
  }
}

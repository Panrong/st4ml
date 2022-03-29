package experiments

import st4ml.instances.{Duration, Point, Trajectory}
import org.apache.spark.sql.SparkSession
import st4ml.operators.selector.SelectionUtils.T

import java.text.SimpleDateFormat
import java.util.Date

object PortoParquet2Geojson {

  case class geometry(coordinates: Seq[Seq[Double]], `type`: String = "LineString")

  case class properties(TIMESTAMP: Long)

  def timeLong2String(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm * 1000))
    tim
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("portoParquet2Geojson")
      .master(args(0))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(1)
    val res = args(2)
    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.points.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.d)
    })
    val formattedRDD = trajRDD.map(traj => {
      val id = traj.data
      val start = timeLong2String(traj.duration.start).split(" ").head
      val end = traj.duration.end
      val coords = traj.entries.map(_.spatial).map(p => Seq(p.getX, p.getY)).toSeq
      val `type` = "Feature"

      (geometry(coords), id, properties(traj.duration.start), `type`, start, end)
    })
    import spark.implicits._
    val df = formattedRDD.toDF("geometry", "id", "properties", "type", "start", "end")
    df.show(5, false)
    df.printSchema()
    df.repartition(256).write.json(res)

    sc.stop
  }
}
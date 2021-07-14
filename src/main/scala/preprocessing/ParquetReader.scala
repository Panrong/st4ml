package preprocessing

import instances._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ParquetReader {
  case class Document(id: String, latitude: Double, longitude: Double, timestamp: Long)

  case class P(latitude: Double, longitude: Double, timestamp: Long)

  case class T(id: String, points: Array[P])

  def readFaceParquet(filePath: String): RDD[Event[Point, String, None.type]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.parquet(filePath)
      .filter("id is not null")
      .filter("latitude is not null")
      .filter("longitude is not null")
      .filter("timestamp is not null")
    val faceRDD = df.as[Document].rdd
      .map(p => {
        val point = Point(p.longitude, p.latitude)
        val duration = Duration(p.timestamp)
        val id = p.id
        val entry = Entry(point, duration, id)
        Event(Array(entry), None)
      })
    faceRDD
  }

  def readVhcParquet(filePath: String): RDD[Trajectory[None.type, String]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.parquet(filePath)
      .filter("id is not null")
      .filter("points is not null")
    val tRDD = df.as[T].rdd
    tRDD.map(t => {
      val points = t.points.map(p => Point(p.longitude, p.latitude))
      val durations = t.points.map(_.timestamp).map(Duration(_))
      val entries = (points zip durations).map(x => Entry(x._1, x._2, None))
      val id = t.id
      Trajectory(entries, id)
    })
  }
}

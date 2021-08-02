package preprocessing

import instances._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object ParquetReader extends Serializable {
  case class Document(id: String, latitude: Double, longitude: Double, timestamp: Long)

  case class P(latitude: Double, longitude: Double, timestamp: Long)

  case class T(id: String, points: Array[P])

  case class GMP(x: Array[Double], y: Array[Double])

  case class GMT(fid: String, timestamp: Long, points: GMP)

  case class GMTS(fid: String, points: GMP, start: Long, interval: Int)

  def readFaceParquet(filePath: String): RDD[Event[Point, None.type, String]] = {
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
        val entry = Entry(point, duration, None)
        Event(Array(entry), id)
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
    tRDD.filter(_.points.length > 1)
      .map(t => {
        val points = t.points.map(p => Point(p.longitude, p.latitude))
        val durations = t.points.map(_.timestamp).map(Duration(_))
        val entries = (points zip durations).map(x => Entry(x._1, x._2, None))
        val id = t.id
        Trajectory(entries, id)
      })
  }

  def readTrajGeomesa(filePath: String, samplingRate: Int = 15): RDD[Trajectory[None.type, String]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val ds = spark.read.parquet(filePath)
      .filter("fid is not null")
      .filter("points is not null")
      .filter("timestamp is not null")
      .select("fid", "timestamp", "points").as[GMT]

    val rdd = ds.rdd
    rdd.map(traj => {
      val length = traj.points.x.length
      val valueArr = new Array[None.type](length)
      val pointArr = (traj.points.x zip traj.points.y).map(Point(_))
      val durationArr = (0 until length).toArray
        .map(x => traj.timestamp + x * samplingRate)
        .map(Duration(_))
      Trajectory(pointArr, durationArr, valueArr, traj.fid)
    })
  }

  def readSyntheticTraj(filePath: String): RDD[Trajectory[None.type, String]] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val ds = spark.read.parquet(filePath)
      .filter("fid is not null")
      .filter("points is not null")
      .select("fid", "points", "start", "interval").as[GMTS]
    val rdd = ds.rdd
    rdd.map(traj => {
      val length = traj.points.x.length
      val valueArr = new Array[None.type](length)
      val pointArr = (traj.points.x zip traj.points.y).map(Point(_))
      val durationArr = (0 until length).toArray
        .map(x => traj.start + x * traj.interval)
        .map(Duration(_))
      Trajectory(pointArr, durationArr, valueArr, traj.fid)
    })
  }
}

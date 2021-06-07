package preprocessing

import geometry.{Point, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadVhc {
  case class P(latitude: Double, longitude: Double, timestamp: Long)

  case class T(id: String, points: Array[P])

  def apply(filePath: String = "datasets/vhc_example.json"): RDD[Trajectory] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val tRDD = spark.read
      .option("header", "false")
      .json(filePath)
      .as[T]
      .rdd
    tRDD.map(t => {
      val points = t.points.map(p => Point(Array(p.longitude, p.latitude), p.timestamp))
      Trajectory(t.id, points.head.t, points)
    })
  }
}

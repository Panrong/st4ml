package preprocessing

import geometry.{Point, Rectangle, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

import scala.math.{max, min}

object ReadParquet {
  case class P(latitude: Double, longitude: Double, timestamp: Long)

  case class T(id: String, points: Array[P])

  case class Document(id: String, latitude: Double, longitude: Double, timestamp: Long)


  def ReadVhcParquet(filePath: String = "datasets/example.parquet"): RDD[Trajectory] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.parquet(filePath)
      .filter("id is not null")
      .filter("points is not null")
    val tRDD = df.as[T].rdd
    tRDD.map(t => {
      val points = t.points.map(p => Point(Array(p.longitude, p.latitude), p.timestamp))
      Trajectory(t.id, points.head.t, points)
    })
  }

  def ReadVhcParquet(filePath: String, samplingRate: Double): RDD[Trajectory] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.parquet(filePath).sample(samplingRate)
      .filter("id is not null")
      .filter("points is not null")
    val tRDD = df.as[T].rdd
    tRDD.map(t => {
      val points = t.points.map(p => Point(Array(p.longitude, p.latitude), p.timestamp))
      Trajectory(t.id, points.head.t, points)
    })
  }

  def ReadFaceParquet(filePath: String = "datasets/faceParquet"): RDD[Point] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.parquet(filePath)
      .filter("id is not null")
      .filter("latitude is not null")
      .filter("longitude is not null")
      .filter("timestamp is not null")
    val faceRDD = df.as[Document].rdd
      .map(p => Point(Array(p.longitude, p.latitude), p.timestamp, p.id))
    faceRDD
  }

  def ReadFaceParquet(filePath: String, limit: Double): RDD[Point] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.parquet(filePath).sample(limit)
      .filter("id is not null")
      .filter("latitude is not null")
      .filter("longitude is not null")
      .filter("timestamp is not null")
    val faceRDD = df.as[Document].rdd
      .map(p => Point(Array(p.longitude, p.latitude), p.timestamp, p.id))
    faceRDD
  }

  def ReadFaceParquetWithPartition(filePath: String,
                                   partitionRanges: Map[Int, Rectangle]): RDD[Point] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    def findPartition(partitionRanges: Map[Int, Rectangle])
                     (lon: Double, lat: Double): Int = {
      val lonMin = partitionRanges.values.map(_.xMin).toArray.min
      val latMin = partitionRanges.values.map(_.yMin).toArray.min
      val lonMax = partitionRanges.values.map(_.xMax).toArray.max
      val latMax = partitionRanges.values.map(_.yMax).toArray.max

      val point = Point(Array(lon, lat))
      val pointShrink = Rectangle(Array(
        min(max(point.lon, lonMin), lonMax),
        min(max(point.lat, latMin), latMax),
        max(min(point.lon, lonMax), lonMin),
        max(min(point.lat, latMax), latMin)))
      partitionRanges.filter(x => pointShrink.inside(x._2)).keys.head
    }

    val findPartitionUDF = udf[Int, Double, Double](findPartition(partitionRanges))
    val df = spark.read.parquet(filePath)
      .filter("id is not null")
      .filter("latitude is not null")
      .filter("longitude is not null")
      .filter("timestamp is not null")
      .withColumn("pId", findPartitionUDF(col("longitude"), col("latitude")))
      .repartition(partitionRanges.size, $"pId")
      .select("id", "latitude", "longitude", "timestamp")
    val faceRDD = df.as[Document].rdd
      .map(p => Point(Array(p.longitude, p.latitude), p.timestamp, p.id))
    faceRDD
  }
}

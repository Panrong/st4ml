package examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.elasticsearch.spark.sql.sparkDatasetFunctions
import utils.Config

object FaceDatasetToEsDemo {

  case class Point(latitude: String, longitude: String, timestamp: String)
  case class Traj(id: String, points: Array[Point])
  case class Document(id: String, latitude: Double, longitude: Double, timestamp: Long)

  def main(args: Array[String]): Unit = {
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("io-es")
      .master(Config.get("master"))
      .config("es.nodes", args(1))
      .config("es.port", args(2))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filePath = args(0)
    val esIndex = args(3)

    import spark.implicits._
    val dataDf = spark.read.option("multiline", "true").json(filePath)
    val dataDs = dataDf.as[Traj]

    val flattenDs = dataDs.withColumn("point", explode(col("points")))
      .withColumn("latitude", col("point.latitude").cast(DoubleType))
      .withColumn("longitude", col("point.longitude").cast(DoubleType))
      .withColumn("timestamp", col("point.timestamp").cast(LongType))

    val resultDs = flattenDs.drop("points", "point").as[Document]
    resultDs.saveToEs(esIndex)
    resultDs.show()


  }

}

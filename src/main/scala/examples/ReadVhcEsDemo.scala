package examples

import org.apache.spark.sql.SparkSession
import utils.Config

object ReadVhcEsDemo {
  case class Entry(vehicleId: String, timeStamp1: Long, cameraId: String)

  case class P(latitude: Double, longitude: Double, timestamp: Long)

  case class T(id: String, points: Array[P])

  def main(args: Array[String]): Unit = {
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("io-es-reading")
      .master(Config.get("master"))

      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val esNode = args(0)
    val esPort = args(1)
    val esIndex = args(2)
    val esQuery = args(3)
    val outputPath = args(4)
    val numPartitions = args(5).toInt

    // reading
    val options = Map("es.nodes" -> esNode,
      "es.port" -> esPort,
      "es.query" -> esQuery)

    val cameraSiteMap = readCameraSite()
    import spark.implicits._
    val TrajRDD = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(options)
      .load(esIndex)
      .select("vehicleId", "timeStamp1", "cameraId")
      .as[Entry]
      .rdd.groupBy(_.vehicleId)
      .mapValues(x => x.map(e => P(cameraSiteMap(e.cameraId)._1, cameraSiteMap(e.cameraId)._2, e.timeStamp1)))
      .map(x => {
        val points = x._2.toArray.sortBy(_.timestamp)
        T(x._1, points)
      })

    val TrajDs = spark.createDataset(TrajRDD)
    println("Reading result top 5: ")
    TrajDs.take(5).foreach(println)
    TrajDs.printSchema()
    TrajDs.repartition(numPartitions).write.json(outputPath)

  }

  case class CameraSiteEntry(devc_id: String, lng: String, lat: String)

  def readCameraSite(filePath: String = "datasets/camerasite.csv"): Map[String, (Double, Double)] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    spark.read.format("csv")
      .option("header", "true")
      .load(filePath)
      .select("devc_id", "lng", "lat")
      .filter("lng is not null")
      .filter("lat is not null")
      .filter("devc_id is not null")
      .as[CameraSiteEntry].rdd
      .map(x => (x.devc_id, (x.lng.toDouble, x.lat.toDouble)))
      .collect.toMap
  }
}

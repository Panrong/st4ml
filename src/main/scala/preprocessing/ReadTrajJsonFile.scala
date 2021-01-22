package preprocessing

import org.apache.spark.sql.SparkSession
import geometry.{Point, Trajectory}
import org.apache.spark.rdd.RDD


object ReadTrajJsonFile {

  case class Traj(coordinates: Array[Array[Double]], id: String, TIMESTAMP: String)

  case class TrajAuxi(
                       coordinates: Array[Array[Double]],
                       id: String,
                       CALL_TYPE: Option[String],
                       DAY_TYPE: Option[String],
                       MISSING_DATA: Option[String],
                       ORIGIN_CALL: Option[String],
                       ORIGIN_STAND: Option[String],
                       TAXI_ID: Option[String],
                       TIMESTAMP: String,
                       TRIP_ID: Option[String],
                     )

  /**
   *
   * @param fileName  : path to the geojson file
   * @param num       : number of items to be read
   * @param auxiliary : read full information or not
   * @return : RDD of geometry.Trajectory
   */
  def apply(fileName: String, num: Int, auxiliary: Boolean = false): RDD[geometry.Trajectory] = {
    val spark = SparkSession.builder().getOrCreate()
    val samplingRate = 15
    val inputDF = spark.read.json(fileName).limit(num)
    inputDF.createOrReplaceTempView("jsonTable")
    import spark.implicits._
    if (auxiliary) {
      val trajDF = spark.sql("SELECT geometry.coordinates, " +
        "id, properties.CALL_TYPE, properties.DAY_TYPE, properties.MISSING_DATA, " +
        "properties.ORIGIN_CALL, properties.ORIGIN_STAND, properties.TAXI_ID," +
        "properties.TIMESTAMP, properties.TRIP_ID " +
        "FROM jsonTable")
      val trajDS = trajDF.as[TrajAuxi]
      trajDS.rdd.map(x => {
        val tripID = x.id.toLong
        val startTime = x.TIMESTAMP.toLong
        val points = x.coordinates.zipWithIndex.map(x => {
          Point(x._1, x._2 * samplingRate + startTime)
        })
        val properties = Map(
          "callType" -> x.CALL_TYPE.get,
          "dayType" -> x.DAY_TYPE.get,
          "missingData" -> x.MISSING_DATA.get,
          "originCall" -> x.ORIGIN_CALL.get,
          "originStand" -> x.ORIGIN_STAND.get,
          "taxiID" -> x.TAXI_ID.get,
          "tripID" -> x.TRIP_ID.get)
        Trajectory(tripID, startTime, points, properties)
      })
    } else {
      val trajDF = spark.sql("SELECT geometry.coordinates, id, properties.TIMESTAMP FROM jsonTable")
      val trajDS = trajDF.as[Traj]
      trajDS.rdd.map(x => {
        val tripID = x.id.toLong
        val startTime = x.TIMESTAMP.toLong
        val points = x.coordinates.zipWithIndex.map(x => {
          Point(x._1, x._2 * samplingRate + startTime)
        })
        Trajectory(tripID, startTime, points)
      })
    }

  }
}

object ReadTrajJsonFileTest extends App {
  override def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("FATAL")

    val jsonFile = args(0)
    val trajRDD = ReadTrajJsonFile(jsonFile, 100)
    trajRDD.take(5).foreach(println(_))

    val trajAuxiRDD = ReadTrajJsonFile(jsonFile, 100, auxiliary = true)
    trajAuxiRDD.take(5).foreach(println(_))

    spark.stop()
  }
}

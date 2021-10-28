package preprocessing

import org.apache.spark.sql.SparkSession
import geometry.{Point, Trajectory}
import instances.Duration
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
  def apply(fileName: String, num: Int = Double.PositiveInfinity.toInt, auxiliary: Boolean = false): RDD[geometry.Trajectory] = {
    val spark = SparkSession.builder().getOrCreate()
    val samplingRate = 15
    val inputDF = spark.read.json(fileName).limit(num)
    println(inputDF.count())
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
        val tripID = x.id
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
        val tripID = x.id
        val startTime = x.TIMESTAMP.toLong
        val points = x.coordinates.zipWithIndex.map(x => {
          Point(x._1, x._2 * samplingRate + startTime)
        })
        Trajectory(tripID, startTime, points)
      })
    }
  }

  def select(fileName: String, num: Int = Double.PositiveInfinity.toInt): RDD[instances.Trajectory[None.type, String]] = {
    val spark = SparkSession.builder().getOrCreate()
    val samplingRate = 15
    val inputDF = spark.read.json(fileName).limit(num)
    println(inputDF.count())
    inputDF.createOrReplaceTempView("jsonTable")
    import spark.implicits._

    val trajDF = spark.sql("SELECT geometry.coordinates, id, properties.TIMESTAMP FROM jsonTable")
    val trajDS = trajDF.as[Traj]
    trajDS.rdd.map(x => {
      val tripID = x.id
      val startTime = x.TIMESTAMP.toLong
      val ts = x.coordinates.indices.map(x => {
        Duration(x * samplingRate + startTime)
      }).toArray
      val points = x.coordinates.map(x => instances.Point(x(0), x(1)))
      instances.Trajectory(points, ts, ts.map(_ => None), tripID)
    })
  }
}

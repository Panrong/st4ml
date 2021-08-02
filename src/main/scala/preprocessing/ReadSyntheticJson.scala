package preprocessing

import instances.{Duration, Point, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadSyntheticJson {

  case class Traj(
                   coordinates: Array[Array[Double]],
                   id: String,
                   start: Long,
                   end: Long,
                   interval: Long
                 )

  /**
   *
   * @param fileName : path to the geojson file
   * @param num      : number of items to be read
   * @return : RDD of geometry.Trajectory
   */
  def apply(fileName: String, num: Int = Double.PositiveInfinity.toInt): RDD[Trajectory[None.type, String]] = {
    val spark = SparkSession.builder().getOrCreate()
    val inputDF = spark.read.json(fileName).limit(num)
    println(inputDF.rdd.getNumPartitions)
    inputDF.createOrReplaceTempView("jsonTable")
    import spark.implicits._
    val trajDF = spark.sql("SELECT geometry.coordinates, " +
      "properties.id, properties.start, properties.end, properties.interval " +
      "FROM jsonTable")
    val trajDS = trajDF.as[Traj]
    trajDS.rdd.map(x => {
      val tripID = x.id
      val startTime = x.start
      val samplingRate = x.interval
      val points = x.coordinates.map(x => Point(x(0), x(1)))
      val timeStamps = x.coordinates.indices.map(x =>
        Duration(x * samplingRate + startTime)).toArray
      val arr = (points zip timeStamps).map(x => (x._1, x._2, None))
      Trajectory(arr, tripID)
    })
  }
}

object ReadSyntheticJsonTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReadSyntheticJsonTest")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajRDD = ReadSyntheticJson("C:\\Users\\kaiqi001\\Desktop\\synthetic\\test.geojson")
    println(trajRDD.count)
    trajRDD.take(5).foreach(println)
  }
}





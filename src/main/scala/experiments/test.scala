package experiments

import com.twitter.chill.Kryo
import instances.{Duration, Event}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import utils.Config

object test {
  case class Point(lon: Double, lat: Double, t: Long, value: String) {
    def tInside(y: (Long, Long)): Boolean = y._1 <= t && y._2 >= t
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("test")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val pointArr = List(
      (0, 0, 10L, "point1"),
      (0, 1, 15L, "point2"),
      (2, 2, 12L, "point3")
    )

    import spark.implicits._
    val pointDs = pointArr.toDF("lon", "lat", "t", "value").as[Point]
    pointDs.show()

    def createPoint(lon: Double, lat: Double, t: Long, value: String): Point = Point(lon, lat, t, value)

    //    val createPointUDF = udf[Point, Double, Double, Long, String](createPoint)
    //    val pointDf2 = pointDs.withColumn("point", createPointUDF(col("lon"), col("lat"), col("t"), col("value")))
    //    pointDf2.show

    spark.udf.register("NEWPOINT", Point(_: Double, _: Double, _: Long, _: String))
    spark.udf.register("TINSIDE", (x: Long, y: (Long, Long)) => y._1 <= x && y._2 >= x)
    pointDs.createOrReplaceTempView("points")
    //    val pointDf2 = spark.sql("SELECT NEWPOINT(lon, lat, t, value) AS point FROM points")
    //    pointDf2.createOrReplaceTempView("points2")

    spark.sql("SELECT * FROM points WHERE TINSIDE(points.t, (CAST(10 AS BIGINT), CAST(12 AS BIGINT)))").show
    pointDs.filter(_.tInside(10L, 12L)).show


//    // test case class instances
//
//    val eventArr = List(
//      Event(instances.Point(0, 0), Duration(0)),
//      Event(instances.Point(1, 1), Duration(1)),
//      Event(instances.Point(2, 2), Duration(2))
//    )
//    val eventDs = spark.createDataset(eventArr)
//    eventDs.show
  }
}
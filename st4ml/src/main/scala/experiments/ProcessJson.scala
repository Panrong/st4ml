package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Event, Extent, Point}
import st4ml.operators.selector.SelectionUtils._

import java.text.SimpleDateFormat
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.TimeZone

object ProcessJson {
  case class Geom(ele: Option[Double],
                  fileName: Option[String],
                  g: Option[String],
                  segmentnumber: Option[Long],
                  time: Option[String],
                  trackindex: Option[Long],
                  trackname: Option[String],
                  tracknumber: Option[Long]) extends Serializable {
    def toMap: Map[String, String] = {
      var res = Map[String, String]()
      if (ele.isDefined) res = res + ("ele" -> ele.get.toString)
      if (fileName.isDefined) res = res + ("fileName" -> fileName.get)
      if (segmentnumber.isDefined) res = res + ("segmentnumber" -> segmentnumber.get.toString)
      if (time.isDefined) res = res + ("time" -> time.get)
      if (trackindex.isDefined) res = res + ("trackindex" -> trackindex.get.toString)
      if (trackname.isDefined) res = res + ("trackname" -> trackname.get)
      if (tracknumber.isDefined) res = res + ("tracknumber" -> tracknumber.get.toString)
      res
    }

    def getData: String = toMap.toString
  }

  case class Nyc(g: Option[String],
                 `attr#0`: Option[String],
                 `attr#1`: Option[String],
                 `attr#2`: Option[String],
                 `attr#3`: Option[String],
                 `attr#4`: Option[String],
                 `attr#5`: Option[String],
                 `attr#6`: Option[String],
                 `attr#7`: Option[String],
                 `attr#8`: Option[String],
                 `attr#9`: Option[String],
                 `attr#10`: Option[String],
                 `attr#11`: Option[String]) {
    def toMap: Map[String, String] = {
      var res = Map[String, String]()
      if (g.isDefined) res = res + ("g" -> g.get)
      if (`attr#0`.isDefined) res = res + ("medallion" -> `attr#1`.get)
      if (`attr#1`.isDefined) res = res + ("hack_license" -> `attr#1`.get)
      if (`attr#2`.isDefined) res = res + ("vendor_id" -> `attr#2`.get)
      if (`attr#3`.isDefined) res = res + ("rate_code" -> `attr#3`.get)
      if (`attr#4`.isDefined) res = res + ("store_and_fwd_flag" -> `attr#4`.get)
      if (`attr#5`.isDefined) res = res + ("pickup_datetime" -> `attr#5`.get)
      if (`attr#6`.isDefined) res = res + ("dropoff_datetime" -> `attr#6`.get)
      if (`attr#7`.isDefined) res = res + ("passenger_count" -> `attr#7`.get)
      if (`attr#8`.isDefined) res = res + ("trip_time_in_secs" -> `attr#8`.get)
      if (`attr#9`.isDefined) res = res + ("trip_distance" -> `attr#9`.get)
      if (`attr#10`.isDefined) res = res + ("dropoff_longitude" -> `attr#10`.get)
      if (`attr#11`.isDefined) res = res + ("dropoff_latitude" -> `attr#11`.get)
      res
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("processJson")
      //     .master("local[4]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val fileDir = args(0)
    val data = args(1)
    val tRange = args(2).split(",").map(_.toLong)
    val save = args(3)

    // ../OSM_GPS.json osm 1483228800,1514764800 none

    def parseTemporalString(s: String, pattern: String = "yyyy-MM-dd HH:mm:ss", timeZone: TimeZone = TimeZone.getDefault): Long = {
      val fm = new SimpleDateFormat(pattern)
      fm.setTimeZone(timeZone)
      fm.parse(s).getTime / 1000
    }

    if (data == "osm") {
      val ds = spark.read.json(fileDir).as[Geom].filter(x => x.g.get.startsWith("POINT") && x.time.isDefined)
      val eventsRDD = ds.rdd.map { x =>
        val coords = x.g.get.replaceAll("[()]", "").split(" ").drop(1).map(_.toDouble)
        val lon = coords(0)
        val lat = coords(1)
        val tString = x.time.get
        val timeStart = tString.indexOf("time=") + 5
        val timeEnd = tString.indexOf(',')
        val t = tString.substring(timeStart, timeEnd).toLong / 1000
        val d = x.getData
        Event(Point(lon, lat), Duration(t), None, d)
      }.filter(x => x.temporalCenter <= tRange(1) && x.temporalCenter >= tRange(0))
      eventsRDD.take(5).foreach(println)
      println(eventsRDD.map(_.spatialCenter.getX).min, eventsRDD.map(_.spatialCenter.getY).min,
        eventsRDD.map(_.spatialCenter.getX).max, eventsRDD.map(_.spatialCenter.getY).max,
        eventsRDD.map(_.temporalCenter).min, eventsRDD.map(_.temporalCenter).max)
      //      val (a, b) = eventsRDD.map(_.temporalCenter).histogram(30)
      //      println(a.deep)
      //      println(b.deep)
      val eventsDs = eventsRDD.toDs()
      eventsDs.printSchema()
      eventsDs.show(5)
      if (save != "none") eventsDs.write.option("maxRecordsPerFile", 100000).parquet(save)
    }

    // ../NYCTaxi.json nyc 1356998400,1388534400 none
    else if (data == "nyc") {
      val sRange = Extent(-80, 40, -70, 50).toPolygon
      val ds = spark.read.json(fileDir).as[Nyc].filter(x => x.g.get.startsWith("POINT")
        && x.`attr#5`.isDefined && x.`attr#6`.isDefined && x.`attr#10`.isDefined && x.`attr#11`.isDefined)
      val eventsRDD = ds.rdd.flatMap { x =>
        val coords = x.g.get.replaceAll("[()]", "").split(" ").drop(1).map(_.toDouble)
        val lonStart = coords(0)
        val latStart = coords(1)
        val tStart = parseTemporalString(x.`attr#5`.get, timeZone = TimeZone.getTimeZone("America/New_York"))
        val tEnd = parseTemporalString(x.`attr#6`.get, timeZone = TimeZone.getTimeZone("America/New_York"))
        val lonEnd = x.`attr#10`.get.toDouble
        val latEnd = x.`attr#11`.get.toDouble
        val d = x.toMap + ("pickup_longitude" -> lonStart.toString) + ("pickup_latitude" -> latStart.toString)
        Array(Event(Point(lonStart, latStart), Duration(tStart), None, d),
          Event(Point(lonEnd, latEnd), Duration(tEnd), None, d))
      }.filter(x => x.temporalCenter <= tRange(1) && x.temporalCenter >= tRange(0) && x.intersects(sRange))
      eventsRDD.take(5).foreach(println)
      //      println(eventsRDD.map(_.spatialCenter.getX).min, eventsRDD.map(_.spatialCenter.getY).min,
      //        eventsRDD.map(_.spatialCenter.getX).max, eventsRDD.map(_.spatialCenter.getY).max,
      //        eventsRDD.map(_.temporalCenter).min, eventsRDD.map(_.temporalCenter).max)
      //      val (a, b) = eventsRDD.map(_.temporalCenter).histogram(30)
      //      println(a.deep)
      //      println(b.deep)
      val eventsDs = eventsRDD.toDs()
      eventsDs.printSchema()
      eventsDs.show(5, false)
      println(eventsDs.count)
      if (save != "none") eventsDs.write.parquet(save)
    }
    sc.stop()

  }
}

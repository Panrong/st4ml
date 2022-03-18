package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Event, Point}

object ProcessGeoJson {
  case class Geometry(coordinates: Array[Double], `type`: String)

  case class Properties(ele: Option[Double], fileName: Option[String], segmentnumber: Option[Long],
                        time: Option[String], trackindex: Option[Long], trackname: Option[String], tracknumber: Option[Long]){
    def toMap : Map[String, String] = {
      var res =  Map[String, String]()
      if(ele.isDefined) res = res+("ele"-> ele.get.toString)
      if(fileName.isDefined) res = res+("fileName"-> fileName.get)
      if(segmentnumber.isDefined) res = res+("segmentnumber"-> segmentnumber.get.toString)
      if(time.isDefined) res = res+("time"-> time.get)
      if(trackindex.isDefined) res = res+("trackindex"-> trackindex.get.toString)
      if(trackname.isDefined) res = res+("trackname"-> trackname.get)
      if(tracknumber.isDefined) res = res+("tracknumber"-> tracknumber.get.toString)
      res
    }
  }

  case class Element(geometry: Geometry, properties: Properties)

  case class GeojsonObject(features: Array[Element], `type`: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("processGeoJson")
      .master("local[4]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val fileDir = "../OSM_GPS.geojson"
    val df = spark.read.option("multiLine", "true").json(fileDir)
//    df.printSchema()
    val ds = df.as[GeojsonObject]

    val events = ds.rdd.flatMap(_.features).filter(x => x.properties.time.isDefined && x.geometry.`type`=="Point").map{x =>
      val lon = x.geometry.coordinates(0)
      val lat = x.geometry.coordinates(1)
      val tString = x.properties.time.get
      val timeStart = tString.indexOf("time=") + 5
      val timeEnd = tString.indexOf(',')
      val t = tString.substring(timeStart, timeEnd).toLong
      Event(Point(lon, lat), Duration(t), None, x.properties.toMap)
    }
    println(events.count)

  }
}

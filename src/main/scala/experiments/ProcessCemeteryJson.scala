package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Event, Extent, Point}
import st4ml.operators.selector.SelectionUtils._

import java.text.SimpleDateFormat
import java.util.TimeZone
import org.locationtech.jts.io.WKTReader

object ProcessCemeteryJson {
  case class Geom(g: Option[String],
                  id: Option[String],
                  version: Option[String],
                  timestamp: Option[String],
                  changeSetId: Option[String],
                  uid: Option[String],
                  uname: Option[String],
                  tagsMap: Option[String]) extends Serializable

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("processJson")
      .master("local[4]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val ds = spark.read.json("../osm21_cemetery.json").as[Geom]
    val rdd = ds.rdd.filter(_.g.isDefined).map(x => {
      val reader = new WKTReader()
      val wkt = x.g.get
      val geom = reader.read(wkt)
      (geom, x.uid.get)
    }).filter(_._1.intersects(Extent(-80, 40, -70, 50).toPolygon))
    rdd.take(5).foreach(println)
    println(rdd.count)
    sc.stop()
  }
}

package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances._
import st4ml.operators.converter.Event2RasterConverter
import st4ml.operators.extractor.RasterAirExtractor
import st4ml.operators.selector.SelectionUtils.readMap
import st4ml.operators.selector.Selector
import st4ml.utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object AirQuality {
  def main(args: Array[String]): Unit = {
    val t = nanoTime()
    val dataDir = args(0)
    val mapDir = args(1)
    val queryFile = args(2)
    val metadata = args(3)
    val numPartitions = args(4).toInt
    val spark = SparkSession.builder()
      .appName("AirQuality")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    for ((sRange, tRange) <- ranges) {
      val selector = new Selector[Event[Point, Array[Double], Int]](sRange, tRange, numPartitions)
      val selectedRDD = selector.selectEvent(dataDir, metaDataDir = metadata).map(x => x.asInstanceOf[Event[Point, Array[Double], Int]])
      val polygons = readMap(mapDir).filter(x => x.intersects(sRange))
      val ts = Range(tRange.start.toInt, (tRange.end + 1).toInt, 86400).sliding(2).toArray
      val st = for (i <- polygons; j <- ts) yield (i, j)
      if (st.length > 0) {
        val converter = new Event2RasterConverter(st.map(_._1), st.map(x => Duration(x._2(0).toLong, x._2(1).toLong)))
        val convertedRDD = converter.convert(selectedRDD)
        val extractor = new RasterAirExtractor
        val extracted = extractor.extract(convertedRDD)
        println(extracted.entries.length)
      }
      else println(0)
    }
    println(s"Air aggregation ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}

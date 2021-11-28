package experiments

import instances.{Duration, Event, Extent, Point}
import operatorsNew.converter.Event2TimeSeriesConverter
import operatorsNew.selector.Selector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object TsFlowExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val queryFile = args(2)
    val tSplit = args(3).toInt
    val numPartitions = args(4).toInt
    val spark = SparkSession.builder()
      .appName("TsFlowExtraction")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    val t = nanoTime()
    type EVENT = Event[Point, Option[String], String]

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[EVENT](spatial, temporal, numPartitions)
      val eventRDD = selector.selectEvent(fileName, metadata, false)
        .map(_.asInstanceOf[Event[Point, None.type, String]])
      val tRanges = splitTemporal(Array(temporal.start, temporal.end), tSplit)
      val converter = new Event2TimeSeriesConverter[Point, None.type,
        String, Int, None.type](x => x.length, tRanges)
        val tsRDD = converter.convertWithRTree(eventRDD)
      val res = tsRDD.collect()
      val ts1 = res.head
      val ts2 = res(1)
      def valueMerge(x:Int, y:Int):Int = x+y
      val mergedTs = res.drop(1).foldRight(res.head)(_.merge(_, valueMerge,  (_,_)=> None))
      eventRDD.unpersist()
      println(mergedTs.entries.map(_.value).deep)
    }
    println(s"Anomaly extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
  def splitTemporal(temporalRange: Array[Long], tStep: Int): Array[Duration] = {
    val tMin = temporalRange(0)
    val tMax = temporalRange(1)
    val tSplit = ((tMax - tMin) / tStep).toInt
    val ts = (0 to tSplit).map(x => x * tStep + tMin).sliding(2).toArray
    for (t <- ts) yield Duration(t(0), t(1))
  }

}

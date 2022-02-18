package operatorsNew.extractor

import instances.{Extent, Instance}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Polygon

import java.text.SimpleDateFormat
import java.util.Date
import scala.reflect.ClassTag

class AnomalyExtractor[T <: Instance[_, _, _] : ClassTag] extends Extractor[T] {
  def timeLong2String(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm * 1000))
    tim
  }

  def extractMultiRanges(rdd: RDD[T], threshold: Array[Int], ranges: Array[Polygon] = Array(Extent(-180, -90, 180, 90).toPolygon)): Array[Array[T]] = {
    def getHour(t: Long): Int =
      timeLong2String(t).split(" ")(1).split(":")(0).toInt

    val condition = if (threshold(0) > threshold(1)) (x: Int) => x >= threshold(0) || x < threshold(1)
    else (x: Int) => x >= threshold(0) && x < threshold(1)
    val filteredRDD = rdd.filter(point => condition(getHour(point.duration.start)))
    var res = new Array[Array[T]](0)
    for (range <- ranges) {
      val a = filteredRDD.filter(_.intersects(range)).collect
      val coords = range.getCoordinates
      println(s"In range (${(coords(0).x, coords(0).y, coords(2).x, coords(2).y)}): ${a.length} anomalies.")
      res = res :+ a
    }
    res
  }

  def extract(rdd: RDD[T], threshold: Array[Int]): RDD[T] = {
    val condition = if (threshold(0) > threshold(1)) (x: Double) => x >= threshold(0) || x < threshold(1)
    else (x: Double) => x >= threshold(0) && x < threshold(1)
    rdd.filter(x => condition(x.duration.hours))
  }

  def extractWithInfo(rdd: RDD[(T, Array[Int])], threshold: Array[Int], ranges: Array[Polygon]): Array[Array[T]] = {
    def getHour(t: Long): Int =
      timeLong2String(t).split(" ")(1).split(":")(0).toInt

    val condition = if (threshold(0) > threshold(1)) (x: Int) => x >= threshold(0) || x < threshold(1)
    else (x: Int) => x >= threshold(0) && x < threshold(1)
    val filteredRDD = rdd.filter(point => condition(getHour(point._1.duration.start)))
      .flatMap(x => x._2.map(y => (y, x._1)))
      .groupByKey
    val res = filteredRDD.collect()
    res.foreach(x => {
      val coords = ranges(x._1).getCoordinates
      println(s"In range (${(coords(0).x, coords(0).y, coords(2).x, coords(2).y)}): ${x._2.size} anomalies.")
    })
    res.map(x => x._2.toArray)
  }
}

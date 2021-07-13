package operatorsNew.extractor

import instances.Instance
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Polygon
import utils.TimeParsing.timeLong2String

import scala.reflect.ClassTag

class AnomalyExtractor[T <: Instance[_, _, _] : ClassTag] extends Extractor[T] {
  def extract(rdd: RDD[T], threshold: Array[Int], ranges: Array[Polygon]): Array[Array[T]] = {

    def getHour(t: Long): Int =
      timeLong2String(t).split(" ")(1).split(":")(0).toInt

    val condition = if (threshold(0) > threshold(1)) (x: Int) => x >= threshold(0) || x < threshold(1)
    else (x: Int) => x >= threshold(0) && x < threshold(1)
    val filteredRDD = rdd.filter(point => condition(getHour(point.duration.start)))
    var res = new Array[Array[T]](0)
    for (range <- ranges) {
      val a = filteredRDD.filter(_.intersects(range)).collect
      println(s"In range (${range.toString}): ${a.length} anomalies.")
      res = res :+ a
    }
    res
  }
}

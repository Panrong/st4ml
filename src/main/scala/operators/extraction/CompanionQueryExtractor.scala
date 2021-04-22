package operators.extraction

import geometry.Point
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.math.abs

class CompanionQueryExtractor(tThreshold: Double, sThreshold: Double, query: Array[Point]) extends Extractor {

  def isCompanion(p1: Point, p2: Point): Boolean = {
    abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold &&
      abs(p1.geoDistance(p2)) <= sThreshold &&
      p1.attributes("tripID") != p2.attributes("tripID")
  }

  val bc: Broadcast[Array[Point]] = SparkContext.getOrCreate().broadcast(query)

  def flatMapFunc(p: Point): Vector[(String, Array[(Long, String)])] =
    bc.value.filter(x => isCompanion(x, p)).map(x => (x.id, Array((x.timeStamp._1, x.id)))).toVector

  def reduceFunc(x: Array[(Long, String)], y: Array[(Long, String)]): Array[(Long, String)] = x ++ y

  def collectFunc(x: Array[(String, Array[(Long, String)])]): Array[(String, Array[(Long, String)])] = x.take(5)

  val extractor = new FlatMapExtractor[Point, String, Array[(Long, String)]](flatMapFunc, reduceFunc, collectFunc)

  def extract(rdd: RDD[Point]): Array[(String, Array[(Long, String)])] = extractor.extract(rdd)

}

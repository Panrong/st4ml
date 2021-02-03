package extraction

import geometry.Point
import org.apache.spark.rdd.RDD

class PointsAnalysisExtractor extends Serializable {
  def extractMostFrequentPoints(n: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
    pRDD.map(point => (point.id.toString, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(n)
  }

  def extractPermanentResidents(t: (Long, Long), occurrenceThreshold: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
    pRDD.filter(p => p.timeStamp._2 >= t._1 && p.timeStamp._1 <= t._2)
      .map(point => (point.id.toString, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= occurrenceThreshold)
      .collect
  }

  def extractNewMoveIn(t: Long, occurrenceThreshold: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
    pRDD.filter(p => p.timeStamp._1 >= t)
      .map(point => (point.id.toString, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= occurrenceThreshold)
      .collect
  }
}

package extraction

import geometry.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class PointsAnalysisExtractor extends Serializable {
  def extractMostFrequentPoints(n: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
    pRDD.map(point => (point.id, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(n)
  }

  def extractMostFrequentPoints(attribute: String, n: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
    pRDD.map(point => (point.attributes(attribute), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(n)
  }

  def extractPermanentResidents(t: (Long, Long), occurrenceThreshold: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
    pRDD.filter(p => p.timeStamp._2 >= t._1 && p.timeStamp._1 <= t._2)
      .map(point => (point.id, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= occurrenceThreshold)
      .collect
  }

  def extractNewMoveIn(t: Long, occurrenceThreshold: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
    pRDD.filter(p => p.timeStamp._1 >= t)
      .map(point => (point.id, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= occurrenceThreshold)
      .collect
  }

  def extractSpatialRange(pRDD: RDD[Point]) : Array[Double] = {
    val lons = pRDD.map(_.coordinates(0))
    val lats = pRDD.map(_.coordinates(1))
    val lonMin = lons.reduce(math.min)
    val latMin = lats.reduce(math.min)
    val lonMax = lons.reduce(math.max)
    val latMax = lats.reduce(math.max)
    Array(lonMin, latMin, lonMax, latMax)
  }

  def extractTemporalRange(pRDD:RDD[Point]):Array[Long] = {
    val tStarts = pRDD.map(_.timeStamp._1)
    val tEnds = pRDD.map(_.timeStamp._2)
    Array(tStarts.reduce(math.min), tEnds.reduce(math.max))
  }

  def extractTemporalQuantile(percentages: Array[Double])(pRDD:RDD[Point]): Array[Double] ={
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val pDF = pRDD.map(_.timeStamp._1).toDF()
    pDF.stat.approxQuantile("value", percentages, 0.01)
  }

  def extractTemporalQuantile(percentages: Double)(pRDD:RDD[Point]): Double ={
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val pDF = pRDD.map(_.timeStamp._1).toDF()
    pDF.stat.approxQuantile("value", Array(percentages), 0.01).head
  }
}

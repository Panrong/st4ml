package operators.extraction

import geometry.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import utils.TimeParsing._

class PointsAnalysisExtractor extends BaseExtractor[Point] with Serializable {

  //  def extractMostFrequentPoints(n: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
  //    pRDD.map(point => (point.id, 1))
  //      .reduceByKey(_ + _)
  //      .sortBy(_._2, ascending = false)
  //      .take(n)
  //  }

  def extractMostFrequentPoints(n: Int)(pRDD: RDD[Point]): Array[(String, Int)] = {
    val extractor = new FreqPointExtractor(n)
    extractor.extract(pRDD)
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

  def extractNewMoveIn(t: Long, occurrenceThresholdAfter: Int, occurrenceThresholdBefore: Int = 3)
                      (pRDD: RDD[Point]): Array[(String, Int)] = {
    val validAfter = pRDD.filter(p => p.timeStamp._1 >= t)
      .map(point => (point.id, 1))
      .reduceByKey(_ + _)
      .filter(_._2 >= occurrenceThresholdAfter)
    val validBefore = pRDD.filter(p => p.timeStamp._1 <= t)
      .map(point => (point.id, 1))
      .reduceByKey(_ + _)
      .filter(_._2 <= occurrenceThresholdBefore)
    validAfter
      .join(validBefore)
      .mapValues(x => x._1).collect
  }

  def extractSpatialRange(pRDD: RDD[Point]): Array[Double] = {
    val lons = pRDD.map(_.coordinates(0))
    val lats = pRDD.map(_.coordinates(1))
    val lonMin = lons.reduce(math.min)
    val latMin = lats.reduce(math.min)
    val lonMax = lons.reduce(math.max)
    val latMax = lats.reduce(math.max)
    Array(lonMin, latMin, lonMax, latMax)
  }

  def extractTemporalRange(pRDD: RDD[Point]): Array[Long] = {
    val tStarts = pRDD.map(_.timeStamp._1)
    val tEnds = pRDD.map(_.timeStamp._2)
    Array(tStarts.reduce(math.min), tEnds.reduce(math.max))
  }

  def extractTemporalQuantile(percentages: Array[Double])(pRDD: RDD[Point]): Array[Double] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val pDF = pRDD.map(_.timeStamp._1).toDF()
    pDF.stat.approxQuantile("value", percentages, 0.01)
  }

  def extractTemporalQuantile(percentages: Double)(pRDD: RDD[Point]): Double = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val pDF = pRDD.map(_.timeStamp._1).toDF()
    pDF.stat.approxQuantile("value", Array(percentages), 0.01).head
  }

  def extractAbnormity(range: (Int, Int) = (23, 5))(pRDD: RDD[Point]): Array[String] = {
    val (a, b) = if (range._1 > range._2) {
      (range._1, range._2)
    } else {
      (range._2, range._1)
    }
    pRDD.map(p =>
      (p.attributes("tripID"), new SimpleDateFormat("HH").format(p.timeStamp._1 * 1000).toInt)) // get hour of each point
      .filter(p => p._2 >= a || p._2 <= b)
      .map(_._1)
      .distinct
      .collect
  }

  def extractNumAttribute(key: String)(pRDD: RDD[Point]): Long =
    pRDD.map(x => x.attributes(key))
      .distinct
      .count

  def extractNumIds(pRDD: RDD[Point]): Long = {
    extractNumAttribute("tripID")(pRDD)
  }

  def extractDailyNum(pRDD: RDD[Point]): Array[(String, Int)] = {
    val startTime = pRDD.map(_.timeStamp._1).min
    val startDate = date2Long(getDate(startTime))
    pRDD.map(_.timeStamp._1)
      .map(x => ((x - startDate) / (24 * 60 * 60), 1))
      .reduceByKey(_ + _)
      .collect
      .sortBy(_._1).map {
      case (date, count) => (getDate(date * (24 * 60 * 60) + startDate), count)
    }
  }
}

package examples

import geometry.Rectangle
import operators.SttDefault
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson

import utils.TimeParsing._
import utils.Config

object PointAnalysisDemo {
  def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("PointAnalysisDemo")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** parse input arguments */
    val numPartitions = Config.get("numPartitions").toInt
    val sQuery = Rectangle(args(0).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(1))

    /**
     * example input arguments: "118.116, 29.061, 120.167, 30.184" "2020-08-11 01:33:20, 2020-08-11 15:26:40"
     */

    /** initialize operators */
    val operator = new SttDefault(numPartitions)

    /** read input data */
    val trajRDD = ReadTrajJson(Config.get("hzData"), numPartitions)
    println(s" ... before selection ${trajRDD.count} trajectories")

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD, sQuery, tQuery)
    rdd1.cache()

    /** step 2: Conversion */
    val rdd2 = operator.converter.convert(rdd1)
    rdd2.cache()

    /** step 3: Extraction */
    val extractor = operator.extractor
    val topN = 3
    println("=== Analysing Results:")

    println(s" ... Total number of trajectories: ${rdd1.count}")
    println(s" ... Total number of points: ${rdd2.count}")

    println(s" ... Top $topN most frequent:")
    extractor.extractMostFrequentPoints("tripID", topN)(rdd2).foreach(x => println(s" ....  $x"))

    /** repeat for more applications */
    println(s" ... Spatial range: ${extractor.extractSpatialRange(rdd2).mkString("(", ", ", ")")}")
    val temporalRange = extractor.extractTemporalRange(rdd2)
    println(s" ... Temporal range: ${temporalRange.mkString("(", ", ", ")")}")
    println(s" ...               : ${temporalRange.map(timeLong2String).mkString("(", ", ", ")")}")

    val temporalMedian = extractor.extractTemporalQuantile(0.5)(rdd2).toLong
    println(s" ... Temporal median (approx.): " +
      s"$temporalMedian (${timeLong2String(temporalMedian)})")

    val temporalQuantiles = extractor.extractTemporalQuantile(Array(0.25, 0.75))(rdd2).map(_.toLong)
    println(s" ... Temporal 25% and 75% (approx.): " +
      s"${temporalQuantiles.mkString(", ")}")
    println(s" ...                               : " +
      s"${temporalQuantiles.map(timeLong2String).mkString(", ")}")

    val newMoveIn = extractor.extractNewMoveIn(1598176021, 10)(rdd2)
    println(s" ... Number of new move-ins after time ${timeLong2String(1598176021)} : ${newMoveIn.length}")

    val pr = extractor.extractPermanentResidents((1596882269, 1598888976), 200)(rdd2)
    println(s" ... Number of permanent residences : ${pr.length}")

    val abnormity = extractor.extractAbnormity()(rdd2)
    println(s" ... Number of abnormal ids : ${abnormity.length}")

    val dailyCount = extractor.extractDailyNum(rdd2)
    dailyCount.foreach(x => println(s"${x._1}: ${x._2}"))
    println(dailyCount.map(_._2).sum)
    sc.stop()
  }
}
package examples

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.TrajCompanionExtractor
import operators.selection.DefaultSelector
import operators.CustomOperatorSet
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajFile

import java.text.SimpleDateFormat
import java.util.Date

object CompanionApp {
  def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("ExampleApp")
      //      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** parse input arguments */
    val trajectoryFile = args(0)
    val numPartitions = args(1).toInt
    val dataSize = args(2).toInt
    val sQuery = Rectangle(args(3).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(4))
    val queryFile = args(5)
    val queryThreshold = args(6).split(",").map(_.toDouble)
    val sThreshold = queryThreshold.head
    val tThreshold = queryThreshold.last


    /** initialize operators */
    val operator = new CustomOperatorSet(
      DefaultSelector(numPartitions),
      new Converter,
      new TrajCompanionExtractor)

    /** read input data */
    val trajRDD = ReadTrajFile(trajectoryFile, dataSize, numPartitions, limit = true)
    val queryRDD = ReadTrajFile(queryFile, 1)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD, sQuery, tQuery)
    //    rdd1.cache()

    /** step 2: Conversion */
    val rdd2 = operator.converter.doNothing(rdd1)
    //    rdd2.cache()
    /** step 3: Extraction */
    val companionPairs = operator.extractor.queryWithIDs(sThreshold, tThreshold)(rdd2, queryRDD)
    val count = companionPairs.mapValues(_.distinct.length)
    println("=== Companion Analysis done: ")
    println(" ... number of companion IDs for each query: ")
    count.foreach { case (q, c) => println(s"  ... $q: $c") }
    sc.stop()
  }

  /** helper functions */
  def timeLong2String(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm * 1000))
    tim
  }

  def parseTemporalRange(s: String, pattern: String = "yyyy-MM-dd HH:mm:ss"): (Long, Long) = {
    val format = new SimpleDateFormat(pattern)
    val tRange = if (s.split(",").head forall Character.isDigit) {
      s.split(",").map(_.toLong)
    } else {
      s.split(",").map(format.parse(_).getTime / 1000)
    }
    (tRange.head, tRange.last)
  }
}
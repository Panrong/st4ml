package examples

import geometry.Rectangle
import operators.CustomOperatorSet
import operators.convertion.Traj2PointConverter
import operators.extraction.PointCompanionExtractor
import operators.selection.DefaultSelectorOld
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing._

object PointCompanionDemo {
  def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("CompanionAll")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** parse input arguments */
    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val sQuery = Rectangle(args(0).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(1))
    val queryThreshold = args(2).split(",").map(_.toDouble)
    val sThreshold = queryThreshold.head
    val tThreshold = queryThreshold.last

    /**
     * example input arguments: "118.35,29.183,120.5,30.55" "1597132800,1597136400" "1000,600"
     */

    /** initialize operators */
    val operator = new CustomOperatorSet(
      DefaultSelectorOld(numPartitions, sQuery, tQuery),
      new Traj2PointConverter,
      new PointCompanionExtractor)

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD)
    //    rdd1.cache()

    /** step 2: Conversion */
    val rdd2 = operator.converter.convert(rdd1)
    //    rdd2.cache()
    println(rdd2.count)
    /** step 3: Extraction */
    val companionPairs = operator.extractor.optimizedExtract(sThreshold, tThreshold)(rdd2)
    println("=== Companion Analysis done: ")
    companionPairs.foreach { case (q, c) => println(s"  ... $q: ${c.size}") }
    sc.stop()
  }
}
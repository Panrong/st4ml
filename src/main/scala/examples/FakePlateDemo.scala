package examples

import geometry.Rectangle
import operators.CustomOperatorSet
import operators.convertion.DoNothingConverter
import operators.extraction.FakePlateExtractor
import operators.selection.DefaultSelector
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing.{parseTemporalRange, timeLong2String}

import java.lang.System.nanoTime

object FakePlateDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("FakePlateDemo")
      .getOrCreate()

    /** parse inpuy arguments */
    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val sQuery = Rectangle(args(0).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(1))
    val speedThreshold = args(2).toDouble // unit: m/s
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // example input: "73,3,135,53" "2020-07-31 23:30:00,2020-08-02 00:30:00" 40

    /** initialize operators */
    val operator = new CustomOperatorSet(
      DefaultSelector(numPartitions),
      new DoNothingConverter,
      new FakePlateExtractor)

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD, sQuery, tQuery)
    println(s"--- ${rdd1.count} trajectories")

    /** step 2: Conversion */
    val rdd2 = operator.converter.convert(rdd1)
    rdd2.cache()

    /** step 3: Extraction */
    val t = nanoTime()
    //    /** extract id only */
    //    val rdd3 = operator.extractor.extract(rdd2, speedThreshold)
    //    println(s"=== Found ${rdd3.count} suspicious fake plates")
    //    println(" ... 5 examples: ")
    //    rdd3.take(5).foreach(x => println(x))
    //    println(s" ... Finding suspicious fake plates takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    /** extract id and abnormal speed */
    val rdd4 = operator.extractor.extractAndShowDetail(rdd2, speedThreshold)
    println(s"=== Found ${rdd4.count} suspicious fake plates")

    rdd4.take(5).foreach(x => {
      println(s"id: ${x._1} \nabnormal speeds:")
      x._2.map(x => (timeLong2String(x._1._1), x._1._2, timeLong2String(x._2._1), x._2._2, x._3 * 3.6))
        .foreach(x => println(s" ${x._1} ${x._2} ${x._3} ${x._4} ${"%.3f".format(x._5)} km/h"))
    })
    println(s" ... Finding suspicious fake plates takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    sc.stop()
  }
}
package experiments

import geometry.{Point, Rectangle, Trajectory}
import operators.OperatorSet
import operators.convertion.Traj2PointConverter
import operators.extraction.PointCompanionExtractor
import operators.selection.DefaultSelector
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing.parseTemporalRange

import java.lang.System.nanoTime

object CompanionExp {
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
    val operator = new OperatorSet {
      override type I = Trajectory
      override type O = Point
      override val selector = new DefaultSelector[Trajectory](sQuery, tQuery)
      override val converter = new Traj2PointConverter
      override val extractor = new PointCompanionExtractor
    }


    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD)
    //    rdd1.cache()

    /** step 2: Conversion */
    val rdd2 = operator.converter.convert(rdd1)
    //    rdd2.cache()
    //    println(rdd2.count)

    /** step 3: Extraction */
    var t = nanoTime()
    val extracted = operator.extractor.optimizedExtract(sThreshold, tThreshold)(rdd2)
      .mapValues(_.toArray).flatMapValues(x => x)
    println(s"${extracted.count} pairs have companion relationship (optimized)")
    println(s"... Optimized scanning takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    t = nanoTime()
    val benchmark = operator.extractor.extract(sThreshold, tThreshold)(rdd2)
      .mapValues(_.toArray).flatMapValues(x => x)
    println(s"${benchmark.count} pairs have companion relationship (optimized)")
    println(s"... Optimized scanning takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    sc.stop()
  }
}

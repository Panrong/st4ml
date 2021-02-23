package examples

import geometry.Rectangle
import operators.SttDefault
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajFile

import java.text.SimpleDateFormat
import java.util.Date


object PointAnalysisApp {
  def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("ExampleApp")
      //.master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** parse input arguments */
    val trajectoryFile = args(0)
    val numPartitions = args(1).toInt
    val dataSize = args(2).toInt
    val sQuery = Rectangle(args(3).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(4))

    /** initialize operators */
    val operator = new SttDefault(numPartitions)

    /** read input data */
    val trajRDD = ReadTrajFile(trajectoryFile, dataSize, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD, sQuery, tQuery)
    rdd1.cache()

    /** step 2: Conversion */
    val rdd2 = operator.converter.traj2Point(rdd1)
    rdd2.cache()

    /** step 3: Extraction */
    val extractor = operator.extractor
    val topN = 3
    println("=== Analysing Results:")
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
package examples

import extraction.PointsAnalysisExtractor
import geometry.Rectangle
import org.apache.spark.sql.SparkSession
import preprocessing.{GenFakePoints, ReadPointFile}
import selection.partitioner.HashPartitioner
import selection.selector.{RTreeSelector, TemporalSelector}

import java.lang.System.nanoTime
import scala.io.Source

object RangeDataAnalysis {
  def main(args: Array[String]): Unit = {
    var t = nanoTime()
    /** set up Spark environment */
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()

    val spark = SparkSession
      .builder()
      .master(config("master"))
      .appName("rangeDataAnalysis")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val pointFile = args(0)
    val numPartitions = args(1).toInt

    /** read point data file */
    //    val pointRDD = ReadPointFile(pointFile)
    val pointRDD = GenFakePoints(100000)
    println(s"... Prepare datasets takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    /** step 1: Selection */
    /** select points inside Hangzhou city only */
    t = nanoTime()
    val sQuery = Rectangle(Array(118.35, 29.183, 120.5, 30.55))
    val tQuery = (200L, 1000L)
    val partitioner = new HashPartitioner(numPartitions)
    val pRDD = partitioner.partition(pointRDD)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = new RTreeSelector(sQuery, partitionRange)
    val temporalSelector = new TemporalSelector(tQuery)
    val queriedRDD = temporalSelector.query(
      spatialSelector.query(pRDD)
    )

    println(s"${queriedRDD.count} points inside range ($sQuery)")
    println(s"... Step 1 Selection takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    /** step 2: Conversion */
    /** not required in this application, only need to remove partition id */
    t = nanoTime()
    val convertedRDD = queriedRDD.map(x => x._2)
    println(s"... Step 2 Conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    /** step 3: Extraction */
    /** perform different data analytics */

    // find the top-n frequent IDs
    t = nanoTime()
    val n = 5
    val extractor = new PointsAnalysisExtractor
    val topN = extractor.extractMostFrequentPoints(n)(convertedRDD)
    println(s"Top $n frequent IDs: ")
    topN.foreach(p => println(s"  ${p._1}: ${p._2} times"))
    println(s"... Step 3.1 Find top $n takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    // find permanent residents
    t = nanoTime()
    val t1 = (500L, 1000L)
    val occurrenceTh1 = 3
    val pr = extractor.extractPermanentResidents(t1, occurrenceTh1)(convertedRDD)
    println(s"${pr.length} IDs are permanent residents with the following number of occurrences")
    pr.foreach(x => println(s"  ${x._1} -> ${x._2}"))
    println(s"... Step 3.2 Find permanent residents takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    //find new move-ins
    t = nanoTime()
    val t2 = 700
    val occurrenceTh2 = 3
    val newMoveIn = extractor.extractNewMoveIn(t2, occurrenceTh2)(convertedRDD)
    println(s"${newMoveIn.length} IDs are new move-ins with the following number of occurrences after time $t2")
    newMoveIn.foreach(x => println(s"  ${x._1} -> ${x._2}"))
    println(s"... Step 3.3 Find new move-in takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    spark.stop()
  }
}

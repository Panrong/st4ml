package examples

import instances.Extent.toPolygon
import instances.{Duration, Event, Extent, Point}
import operatorsNew.OperatorSet
import operatorsNew.converter.DoNothingConverter
import operatorsNew.extractor.AnomalyExtractor
import operatorsNew.selector.MultiSpatialRangeSelector
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.ParquetReader
import utils.TimeParsing.parseTemporalRange

import java.lang.System.nanoTime
import scala.io.Source

object AnomalyNew {
  def main(args: Array[String]): Unit = {

    /** parse input arguments */
    val master = args(0)
    val dataPath = args(1)
    val numPartitions = args(2).toInt
    val duration = parseTemporalRange(args(3))
    val tQuery = Duration(duration._1, duration._2)
    val queryFile = args(4)
    val tThreshold = args(5).split(",").map(_.toInt)

    /**
     * example input arguments:
     * "local" datasets/face_example.parquet "16" "2020-07-31 23:30:00,2020-08-02 00:30:00" "datasets/sample_queries.txt" "23,4"
     */

    /** parse queries */
    val sQueries = readQueries(queryFile).map(toPolygon)

    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("AnomalyApp")
      .master(master)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** initialize operators */
    val operator = new OperatorSet {
      type I = Event[Point, None.type, String]
      type O = Event[Point, None.type, String]
      val selector = new MultiSpatialRangeSelector(sQueries, tQuery, numPartitions)
      val converter = new DoNothingConverter[I]
      val extractor = new AnomalyExtractor[O]
    }

    /** read input data */
    val t = nanoTime()
    val pointRDD = ParquetReader.readFaceParquet(dataPath)

    println(pointRDD.count)
    println(s"Takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    /** step 1: Selection */
    val rdd1 = operator.selector.query(pointRDD)

    /** step 2: Conversion */
    val rdd2 = operator.converter.convert(rdd1)
    rdd2.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"--- Total points: ${rdd2.count}")

    /** step 3: Extraction */
    val anomalies = operator.extractor.extract(rdd2, tThreshold, sQueries)
    println("done")
    sc.stop()
  }

  def readQueries(filename: String): Array[Extent] = {
    var res = new Array[Extent](0)
    val f = Source.fromFile(filename)
    for (line <- f.getLines) {
      val query = line.split(" ").map(_.toDouble)
      res = res :+ Extent(query(0), query(1), query(2), query(3))
    }
    f.close()
    res
  }
}

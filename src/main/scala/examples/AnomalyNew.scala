package examples

import instances.Extent.toPolygon
import instances.{Duration, Event, Extent, Point}
import operatorsNew.OperatorSet
import operatorsNew.converter.DoNothingConverter
import operatorsNew.selector.MultiRangeSelector
import operatorsNew.selector.SelectionUtils.E
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.ParquetReader
import utils.Config
import utils.TimeParsing.parseTemporalRange

import java.lang.System.nanoTime
import scala.io.Source

object AnomalyNew {
  def main(args: Array[String]): Unit = {

    /** parse input arguments */
    val master = Config.get("master")
    val dataPath = args(0)
    val metadataPath = args(1)
    val numPartitions = args(2).toInt
    val duration = args(3).split(",").map(_.toLong)
    val tQuery = Duration(duration(0), duration(1))
    val queryFile = args(4)
    val tThreshold = args(5).split(",").map(_.toInt)

    /**
     * example input arguments:
     * datasets/event_example_parquet_tstr "16" 1372636858,1372637188 "datasets/sample_queries.txt" "23,4"
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

    type I = Event[Point, None.type, String]
    type O = Event[Point, None.type, String]

    val selector = MultiRangeSelector[I](sQueries, tQuery, numPartitions)

    class AnomalyExtractor {
      def extract(rdd: RDD[(I, Array[Int])], threshold: Array[Int]): RDD[(Int, Array[I])] = {
        val condition = if (threshold(0) > threshold(1)) (x: Double) => x >= threshold(0) || x < threshold(1)
        else (x: Double) => x >= threshold(0) && x < threshold(1)
        rdd.filter(x => {
          val h = x._1.duration.hours
          condition(h)
        }).flatMap(x => x._2.map(i => (i, x._1))).groupBy(_._1).mapValues(x => x.toArray.map(_._2))
      }
    }

    val extractor = new AnomalyExtractor

    /** read input data */
    val t = nanoTime()
    import spark.implicits._
    val pointRDD = spark.read.parquet(dataPath).drop("pId").as[E]
      .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])

    println(pointRDD.count)


    /** step 1: Selection */
    val rdd1 = selector.selectEventWithInfo(dataPath, metadataPath, true).map(_.asInstanceOf[(I, Array[Int])])

    /** step 3: Extraction */
    val rdd3 = extractor.extract(rdd1, tThreshold)
    rdd3.map(x => (x._1, x._2.length))
    rdd3.collect.foreach(println)

    println(s"Takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
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

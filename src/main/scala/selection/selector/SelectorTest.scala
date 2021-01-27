package selection.selector

import convertion.Converter
import extraction.SMExtractor
import geometry.{Rectangle, Trajectory}
import org.apache.spark.sql.{Dataset, SparkSession}
import preprocessing.ReadTrajJsonFile
import road.RoadGrid
import selection.partitioner.{HashPartitioner, STRPartitioner}

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source
import extraction.PointCompanionExtractor

object SelectorTest extends App {
  override def main(args: Array[String]): Unit = {

    import preprocessing.{ReadTrajFile, ReadPointFile}
    import java.lang.System.nanoTime

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
      .appName(config("appName"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajectoryFile = args(0)
    val numPartitions = args(1).toInt
    val dataSize = args(2).toInt

    /** *************************
     * test trajectory dataset
     * ************************ */

    val trajDS: Dataset[geometry.Trajectory] = ReadTrajFile(trajectoryFile, num = dataSize)
    val trajRDD = trajDS.rdd //.map(x => x.mbr.setTimeStamp((x.points.head.timeStamp._1, x.points.last.timeStamp._2)))
    //    val trajRDD = ReadTrajJsonFile(trajectoryFile, 10000)

    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    //    val tQuery = (1372700000L, 1372750000L)
    val tQuery = (1399900000L, 1400000000L)

    println(s"\nOriginal trajectory dataset contains ${trajRDD.count} entries")
    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))

    println(s"*- Full scan S: ${fullSRDD.count} -*")
    val fullSRDDStrict1 = trajRDD.filter(x => x.points.exists(p => p.intersect(sQuery)))
    //    fullSRDDStrict1.take(5).foreach(x => println((x.tripID, x.points.map(x => (x, x.intersect(sQuery))).deep)))
    val fullSRDDStrict2 = trajRDD.filter(x => x.strictIntersect(sQuery))
    println(s"*- Full scan S strict 1 : ${fullSRDDStrict1.count} -*")
    println(s"*- Full scan S strict 2 : ${fullSRDDStrict2.count} -*")
    //    fullSRDD.map(_.tripID).saveAsTextFile("queriedTrajs_mbr")
    //    fullSRDDStrict1.map(_.tripID).saveAsTextFile("queriedTrajs_point")
    //    fullSRDDStrict2.map(_.tripID).saveAsTextFile("queriedTrajs_linestring")


    val fullSTRDD = fullSRDDStrict2.filter(x => {
      val (ts, te) = x.timeStamp
      (ts <= tQuery._2 && ts >= tQuery._1) || (te <= tQuery._2 && te >= tQuery._1)
    })
    println(s"*- Full scan ST: ${fullSTRDD.count} -*")
    println("*-*-*-*-*-*-*-*-*-*-*-*")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.\n")

    /**
     * Usage of spatialSelector (+ indexer + partitioner)
     */

    /** partition */
    //    println("==== STR ====")
    //    t = nanoTime()
    //    val partitioner = new STRPartitioner(numPartitions)
    //    val pRDD = partitioner.partition(trajRDD)
    //    val partitionRange = partitioner.partitionRange
    //    val spatialSelector = new RTreeSelector(sQuery, partitionRange)
    //    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")
    //
    //    /** spatial query by filtering */
    //    t = nanoTime()
    //    val queriedRDD1 = spatialSelector.query(partitionRange) //.map(x => (x._2.id, x)).groupByKey().flatMap(x => x._2.take(1))
    //    println(s"==== Queried dataset contains ${queriedRDD1.count} entries (filtering)")
    //    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")
    //
    //    /** spatial query with index */
    //    t = nanoTime()
    //    val queriedRDD2 = spatialSelector.query(pRDD) //.map(x => (x._2.id, x)).groupByKey().flatMap(x => x._2.take(1))
    //    println(s"==== Queried dataset contains ${queriedRDD2.count} entries (RTree)")
    //    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")
    //
    //    /** temporal query by filtering */
    //    t = nanoTime()
    //    val temporalSelector = new TemporalSelector(tQuery)
    //    val queriedRDD3 = temporalSelector.query(queriedRDD2)
    //    println(s"==== Queried dataset contains ${queriedRDD3.count} entries (ST)")
    //    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** test hash partitioner */
    println("\n==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD)
    pRDDHash.collect
    val partitionRangeHash = hashPartitioner.partitionRange
    val selectorHash = new RTreeSelector(sQuery, partitionRangeHash)
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Hash = selectorHash.query(pRDDHash)
    println(s"==== Queried dataset contains ${queriedRDD2Hash.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorH = new TemporalSelector(tQuery)
    val queriedRDD3Hash = temporalSelectorH.query(queriedRDD2Hash)
    println(s"==== Queried dataset contains ${queriedRDD3Hash.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")


    sc.stop()
  }
}
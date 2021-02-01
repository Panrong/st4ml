package examples

import extraction.PointCompanionExtractor
import geometry.Rectangle
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import selection.partitioner.HashPartitioner
import selection.selector.{RTreeSelector, TemporalSelector}

import scala.io.Source
import preprocessing.ReadTrajFile

import java.lang.System.nanoTime

object CompanionTest extends App {

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


  val trajRDD: RDD[geometry.Trajectory] = ReadTrajFile(trajectoryFile, num = dataSize).repartition(numPartitions)

  val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
  val tQuery = (0L, 1500000000L)

  val pointRDD = trajRDD.flatMap(traj => {
    traj.points.zipWithIndex.map(x => x._1.setID((traj.hashCode().abs + x._2.toString).toLong))
  })

  /** initialise operators */

  val partitioner = new HashPartitioner(numPartitions)
  val pRDD = partitioner.partition(pointRDD)
  val partitionRange = partitioner.partitionRange
  val spatialSelector = new RTreeSelector(sQuery, partitionRange)
  val temporalSelector = new TemporalSelector(tQuery)

  /** step 1: selection */

  val sRDD = spatialSelector.query(pRDD)
  val stRDD = temporalSelector.query(sRDD)

  println(s"${sRDD.count} points after spatial filtering")
  println(s"${stRDD.count} points after spatio-temporal filtering")

  /** step 3: extraction */
  t = nanoTime()
  val extractor = new PointCompanionExtractor(500, 100)
  val extractedO = extractor.optimizedExtract(stRDD.map(x => x._2))
  println(s"${extractedO.length} pairs have companion relationship (optimized)")
  println(s"... Optimized scanning takes ${(nanoTime() - t) * 1e-9} s.")
  //    println(extracted2.sorted.deep)
  val extracted = extractor.extract(stRDD.map(x => x._2))
  println(s"${extracted.length} pairs have companion relationship (full scan)")
  println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.")
  //    println(extracted.sorted.deep)

  sc.stop()
}
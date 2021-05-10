package examples

import geometry.{Point, Rectangle}
import operators.convertion.Traj2PointConverter
import operators.extraction.PointCompanionExtractor
import operators.repartitioner.TSTRRepartitioner
import operators.selection.partitioner.HashPartitioner
import operators.selection.selectionHandler.{RTreeHandler, TemporalSelector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajFile
import utils.Config

import java.lang.System.nanoTime

object PointCompanionDev {
  def main(args: Array[String]): Unit = {
    var t = nanoTime()
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("PointCompanionDev")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajectoryFile = Config.get("portoData")
    val numPartitions = Config.get("numPartitions").toInt
    val dataSize = args(0).toInt
    val queryFile = args(1)

    /**
     * example input arguments: 1000 preprocessing/query.csv
     */

    val trajRDD: RDD[geometry.Trajectory] = ReadTrajFile(trajectoryFile, num = dataSize, limit = true).repartition(numPartitions)

    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (0L, 1500000000L)

    /** initialise operators */

    val partitioner = new HashPartitioner(numPartitions)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = RTreeHandler(partitionRange)
    val temporalSelector = new TemporalSelector

    /** step 1: selection */

    val sRDD = spatialSelector.query(pRDD)(sQuery)
    val stRDD = temporalSelector.query(sRDD)(tQuery)

    println(s"${stRDD.count} trajectories after spatio-temporal filtering")

    /** step 2: Conversion */
    val converter = new Traj2PointConverter()
    val pointRDD = converter.convert(stRDD)
    println(s"${pointRDD.count} points after conversion")

    /** step 3: repartition */
    t = nanoTime()
    val repartitioner = new TSTRRepartitioner[Point](Config.get("tPartition").toInt,
      100, 600, Config.get("samplingRate").toDouble)
    val rpRDD = repartitioner.partition(pointRDD)

    /** step 4: extraction */
    val extractor = new PointCompanionExtractor
    val extractedO = extractor.optimizedExtract(100, 600)(rpRDD)
    println(s"${extractedO.map(_._2.size).sum} pairs have companion relationship (optimized)")
    println(s"... Optimized scanning takes ${(nanoTime() - t) * 1e-9} s.")
    //    println(extracted2.sorted.deep)
    val extracted = extractor.extract(100, 600)(pointRDD)
    println(s"${extracted.map(_._2.size).sum} pairs have companion relationship (full scan)")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.")
    //    println(extracted.sorted.deep)

    /** step 4.1: Querying companion with IDs */
    val queries = ReadTrajFile(queryFile, num = 1)
    val queryPoints = converter.convert(queries)
    val queried1 = extractor.optimizedQueryWithIDs(100, 600)(rpRDD, queryPoints.collect) // 500m and 10min
    val count1 = queried1.mapValues(_.size).collect
    val queried2 = extractor.queryWithIDs(100, 600)(rpRDD, queryPoints)
    val count2 = queried2.mapValues(_.size).collect
    println(count1.sortBy(_._1).deep)
    println(count2.sortBy(_._1).deep)

    sc.stop()
  }
}
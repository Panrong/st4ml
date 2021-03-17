package examples

import geometry.{Rectangle, Trajectory}
import operators.extraction.TrajCompanionExtractor
import operators.selection.partitioner.HashPartitioner
import operators.selection.selectionHandler.{RTreeHandler, TemporalSelector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config

import java.lang.System.nanoTime
import scala.collection.mutable

// Find companion of a query set
object TrajCompanionDev {
  def main(args: Array[String]): Unit = {
    var t = nanoTime()

    val spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("CompanionTest")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val queryFile = args(0)
    val sQuery = Rectangle(args(1).split(",").map(_.toDouble))
    val tQuery = (args(2).split(",").head.toLong, args(2).split(",").last.toLong)

    /**
     * example input arguments: datasets/query100.json 118.116,29.061,120.167,30.184 1597000000,1598000000
     */

    val trajRDD: RDD[geometry.Trajectory] = ReadTrajJson(trajectoryFile, numPartitions).repartition(numPartitions)

    /** initialise operators */

    val partitioner = new HashPartitioner(numPartitions)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = RTreeHandler(partitionRange)
    val temporalSelector = new TemporalSelector

    /** step 1: selection */

    val sRDD: RDD[(Int, Trajectory)] = spatialSelector.query(pRDD)(sQuery)
    val stRDD = temporalSelector.query(sRDD)(tQuery)

    println(s"${stRDD.count} trajectories after spatio-temporal filtering")

    /** step 2: Conversion */
    val rdd = stRDD.map(_._2)

    /** step 3: extraction */
    t = nanoTime()
    val extractor = new TrajCompanionExtractor
    val queries = ReadTrajJson(queryFile, 1)
    val queried1 = extractor.queryWithIDs(500, 600)(rdd, queries) // 500m and 10min
    val count1 = queried1.mapValues(_.distinct.length)
    println(mutable.ListMap(count1.toSeq.sortBy(_._1): _*))
    //    val queried2 = extractor.queryWithIDsFS(500, 600)(rdd, queries)
    //    val count2 = queried2.mapValues(_.distinct.length)
    //    println(mutable.ListMap(count2.toSeq.sortBy(_._1): _*))

    sc.stop()
  }
}
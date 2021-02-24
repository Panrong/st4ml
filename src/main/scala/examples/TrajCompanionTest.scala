package examples

import geometry.{Rectangle, Trajectory}
import operators.extraction.TrajCompanionExtractor
import operators.selection.partitioner.HashPartitioner
import operators.selection.selectionHandler.{RTreeHandler, TemporalSelector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajFile

import java.lang.System.nanoTime
import scala.collection.mutable
import scala.io.Source

object TrajCompanionTest {
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
      .appName(config("appName"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajectoryFile = args(0)
    val numPartitions = args(1).toInt
    val dataSize = args(2).toInt
    val queryFile = args(3)

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

    val sRDD: RDD[(Int, Trajectory)] = spatialSelector.query(pRDD)(sQuery)
    val stRDD = temporalSelector.query(sRDD)(tQuery)

    println(s"${stRDD.count} trajectories after spatio-temporal filtering")

    /** step 2: Conversion */
    val rdd = stRDD.map(_._2)

    /** step 3: extraction */
    t = nanoTime()
    val extractor = new TrajCompanionExtractor
    val queries = ReadTrajFile(queryFile, num = 1)
    val queried1 = extractor.queryWithIDs(500, 600)(rdd, queries) // 500m and 10min
    val count1 = queried1.mapValues(_.distinct.length)
    println(mutable.ListMap(count1.toSeq.sortBy(_._1): _*))
    val queried2 = extractor.queryWithIDsFS(500, 600)(rdd, queries)
    val count2 = queried2.mapValues(_.distinct.length)
    println(mutable.ListMap(count2.toSeq.sortBy(_._1): _*))

    sc.stop()
  }
}
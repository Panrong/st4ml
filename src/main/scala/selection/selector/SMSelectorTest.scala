package selection.selector

import geometry.{Rectangle, mmTrajectory}
import org.apache.spark.sql.{Dataset, SparkSession}
import preprocessing.ReadMMTrajFile
import selection.partitioner.{HashPartitioner, STRPartitioner}

import scala.io.Source

object SMSelectorTest extends App {
  override def main(args: Array[String]): Unit = {

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
    val numPartitions = args(2).toInt
    val samplingRate = args(3).toDouble
    val rtreeCapacity = args(4).toInt
    val dataSize = args(5).toInt
    val mapFile = args(6)

    /** **********************************
     * test map-matched trajectory dataset
     * ********************************** */

    val trajDS: Dataset[mmTrajectory] = ReadMMTrajFile(trajectoryFile, mapFile)
    val trajRDD = trajDS.rdd
    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372630000L, 1372660000L)

    println(s"\nOriginal trajectory dataset contains ${trajRDD.count} entries")
    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    println(s"*- Full scan S: ${fullSRDD.count} -*")
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      x.intersect(sQuery) && (ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1)
    })
    println(s"*- Full scan ST: ${fullSTRDD.count} -*")
    println("*-*-*-*-*-*-*-*-*-*-*-*")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.\n")

    /** partition */
    println("==== STR ====")
    t = nanoTime()
    val partitioner = new STRPartitioner(numPartitions, samplingRate)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = new SpatialSelector(pRDD, sQuery)
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    /** spatial query by filtering */
    t = nanoTime()
    val queriedRDD1 = spatialSelector.query(partitionRange) //.map(x => (x._2.id, x)).groupByKey().flatMap(x => x._2.take(1))
    println(s"==== Queried dataset contains ${queriedRDD1.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    /** spatial query with index */
    t = nanoTime()
    val queriedRDD2 = spatialSelector.queryWithRTreeIndex(rtreeCapacity, partitionRange) //.map(x => (x._2.id, x)).groupByKey().flatMap(x => x._2.take(1))
    println(s"==== Queried dataset contains ${queriedRDD2.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")
    /** temporal query by filtering */
    t = nanoTime()
    val temporalSelector = new TemporalSelector(queriedRDD2, tQuery)
    val queriedRDD3 = temporalSelector.query()
    println(s"==== Queried dataset contains ${queriedRDD3.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** test hash partitioner */
    println("\n==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD)
    val partitionRangeHash = hashPartitioner.partitionRange
    val selectorHash = new SpatialSelector(pRDDHash, sQuery)
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD1Hash = selectorHash.query(partitionRangeHash)
    println(s"==== Queried dataset contains ${queriedRDD1Hash.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Hash = selectorHash.queryWithRTreeIndex(rtreeCapacity, partitionRangeHash)
    println(s"==== Queried dataset contains ${queriedRDD2Hash.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorH = new TemporalSelector(queriedRDD2Hash, tQuery)
    val queriedRDD3Hash = temporalSelectorH.query()
    println(s"==== Queried dataset contains ${queriedRDD3Hash.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    sc.stop()
  }
}

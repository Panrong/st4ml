package selection.selector

import geometry.Rectangle
import org.apache.spark.sql.{Dataset, SparkSession}
import selection.partitioner.{HashPartitioner, STRPartitioner}

import scala.io.Source

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
    val numPartitions = args(2).toInt
    val samplingRate = args(3).toDouble
    val rtreeCapacity = args(4).toInt
    val dataSize = args(5).toInt

    /** *************************
     * test trajectory dataset
     * ************************ */

    val trajDS: Dataset[geometry.Trajectory] = ReadTrajFile(trajectoryFile, num = dataSize)
    val trajRDD = trajDS.rdd.map(x => x.mbr)
    val query = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))

    println(s"\nOriginal trajectory dataset contains ${trajRDD.count} entries")

    /**
     * Usage of selector (+ indexer + partitioner)
     */

    /** partition */
    t = nanoTime()
    val partitioner = new STRPartitioner(numPartitions, samplingRate)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = new SpatialSelector(pRDD, query)
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    /** query by filtering */
    t = nanoTime()
    val queriedRDD1 = selector.query(partitionRange)
    println(s"==== Queried dataset contains ${queriedRDD1.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    /** query with index */
    t = nanoTime()
    val queriedRDD2 = selector.queryWithRTreeIndex(rtreeCapacity, partitionRange)
    println(s"==== Queried dataset contains ${queriedRDD2.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")


    /** test hash partitioner */
    println("\n==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD)
    val partitionRangeHash = hashPartitioner.partitionRange
    val selectorHash = new SpatialSelector(pRDDHash, query)
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD1Hash = selectorHash.query(partitionRangeHash)
    println(s"==== Queried dataset contains ${queriedRDD1Hash.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Hash = selectorHash.queryWithRTreeIndex(rtreeCapacity, partitionRangeHash)
    println(s"==== Queried dataset contains ${queriedRDD2Hash.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    /** *******************
     * test point dataset
     * ******************* */
    val pointRDD = ReadPointFile("datasets/cams.json")

    println(s"\n\nOriginal point dataset contains ${pointRDD.count} entries")
    val query2 = Rectangle(Array(118.35, 29.183, 120.5, 30.55))
    //val query2 = Rectangle(Array( 118,29,121, 31))


    /**
     * Usage of selector (+ indexer + partitioner)
     */

    /** partition */
    t = nanoTime()
    val partitioner2 = new STRPartitioner(numPartitions, samplingRate)
    val pRDD2 = partitioner2.partition(pointRDD)
    val partitionRange2 = partitioner2.partitionRange
    val selector2 = new SpatialSelector(pRDD2, query2)
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    /** query by filtering */
    t = nanoTime()
    val queriedRDD1p = selector2.query(partitionRange2) //.map(x => x._2.id.toString).distinct
    println(s"==== Queried dataset contains ${queriedRDD1p.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    /** query with index */
    t = nanoTime()
    val queriedRDD2p = selector2.queryWithRTreeIndex(rtreeCapacity, partitionRange2) //.map(x => x._2.id.toString).distinct
    println(s"==== Queried dataset contains ${queriedRDD2p.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    sc.stop()
  }
}

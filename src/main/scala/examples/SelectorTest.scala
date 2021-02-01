package examples

import geometry.Rectangle
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.ReadTrajFile
import selection.partitioner.HashPartitioner
import selection.selector.{RTreeSelector, TemporalSelector}

import java.lang.System.nanoTime
import scala.io.Source
import scala.math.{max, sqrt}

object SelectorTest extends App {
  override def main(args: Array[String]): Unit = {
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


    val trajRDD = ReadTrajFile(trajectoryFile, num = dataSize, numPartitions)
      .map(_.mbr)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372700000L, 1372750000L)
    //val tQuery = (1399900000L, 1400000000L)
    val rTreeCapacity = max(sqrt(dataSize / numPartitions).toInt, 100)

    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    println(s"*- Full scan S: ${fullSRDD.count} -*")
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1
    })
    println(s"*- Full scan ST: ${fullSTRDD.count} -*")
    println("*-*-*-*-*-*-*-*-*-*-*-*")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.\n")

    /** test hash partitioner */
    println("==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD).cache()
    val partitionRangeHash = hashPartitioner.partitionRange

    val selectorHash = new RTreeSelector(sQuery, partitionRangeHash, Some(rTreeCapacity))
    pRDDHash.count()
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Hash = selectorHash.query(pRDDHash).cache()
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

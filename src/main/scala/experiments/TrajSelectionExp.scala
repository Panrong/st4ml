package experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.{ReadQueryFile, ReadTrajJson}
import operators.selection.partitioner.STRPartitioner
import operators.selection.selectionHandler.RTreeHandler
import utils.Config

import java.lang.System.nanoTime

object TrajSelectionExp extends App {
  val spark = SparkSession.builder()
    .appName("SelectorExp")
    .master(Config.get("master"))
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val trajectoryFile = Config.get("hzData")
  val numPartitions = Config.get("numPartitions").toInt
  val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)
    .persist(StorageLevel.MEMORY_AND_DISK)
  val dataSize = trajRDD.count

  val spatialRanges = ReadQueryFile(Config.get("queryFile")).collect()
  val temporalRange = (1399900000L, 1400000000L)

  println("test naive spark benchmark")
  var t = nanoTime()
  var b = collection.mutable.ArrayBuffer[Long]()
  for (spatialRange <- spatialRanges) {
    val c = trajRDD.filter(x => x.intersect(spatialRange) && x.temporalOverlap(temporalRange)).count
    b += c
  }
  println(b)
  println(s"... naive spark benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

  println("test STR partition + RTree")
  t = nanoTime()
  val partitioner = new STRPartitioner(numPartitions)
  val pRDD = partitioner.partition(trajRDD)
  pRDD.take(1)
  pRDD.cache()

  val partitionRange = partitioner.partitionRange
  val selector = RTreeHandler(partitionRange)

  println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

  t = nanoTime()
  var s = collection.mutable.ArrayBuffer[Long]()
  for(spatialRange <- spatialRanges) {
    val queriedRDD = selector.query(pRDD)(spatialRange).filter(_.temporalOverlap(temporalRange))
    s += queriedRDD.count
  }
  println(s)
  println(s"... st-tool takes ${(nanoTime() - t) * 1e-9} s.")
  sc.stop()

}

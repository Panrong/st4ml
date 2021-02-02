package examples

import geometry.Shape
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import preprocessing.{ReadQueryFile, ReadTrajFile}
import selection.indexer.RTree
import selection.partitioner.STRPartitioner

import java.lang.System.nanoTime
import scala.collection.immutable.ListMap
import scala.io.Source

/**
 * Test key partitioner with range query
 */
object KeyPartitionerTest extends App {
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
  val spark = SparkSession.builder().master(config("master")).appName(config("appName")).getOrCreate()
  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  val trajectoryFile = args(0)
  val queryFile = args(1)
  val numPartitions = args(2).toInt
  val samplingRate = args(3).toDouble
  val rtreeCapacity = args(4).toInt
  val dataSize = args(5).toInt
  val trajRDD: RDD[geometry.Trajectory] = ReadTrajFile(trajectoryFile, num = dataSize)
  val queryRDD = ReadQueryFile(queryFile).rdd.map(_.mbr)

  /** full scan */
  println("\n=== Test full scan ===")
  var t = nanoTime
  val res1 = trajRDD.cartesian(queryRDD).filter(x => x._1.intersect(x._2)).map {
    case (traj, query) => (query.id, traj)
  }
    .groupByKey(numPartitions)
    .mapValues(x => x.toArray.length)
  // .mapValues(x => x.map(x => x.id))
  println(ListMap(res1.collect.toSeq.sortBy(_._1): _*))
  println(s"=== Full scan takes ${(nanoTime - t) / 1e9} s.")

  /** with partitioner */
  t = nanoTime
  // partition
  println("\n=== Test querying with partitioning ===")
  val partitioner = new STRPartitioner(numPartitions, Some(samplingRate))
  val (pRDD, pQueryRDD) = partitioner.copartition(trajRDD, queryRDD)
  val partitionRange = partitioner.partitionRange

  println(s"=== Partitioning takes ${(nanoTime - t) / 1e9} s.")
  t = nanoTime
  // query test
  val res2 = pQueryRDD.cogroup(pRDD)
    .map {
      case (_, v) => v._1.map(query => (query, v._2.filter(shape => shape.intersect(query)))) //  TODO RTree
    }
    .flatMap(x => x)
    .map(x => (x._1.id, x._2))
    .groupByKey()
    .mapValues(_.flatten.toList.map(x => x.id).distinct.size)
  println(ListMap(res2.collect.toSeq.sortBy(_._1): _*))
  println(s"=== Querying with partitioning takes ${(nanoTime - t) / 1e9} s.")
  t = nanoTime
  //    println("====== Checking difference")
  //    val r1: Map[Long, Iterable[Long]] = res1.collect.toMap
  //    val r2: Map[Long, List[Long]] = res2.collect.toMap
  //    for (i <- r1.keys) {
  //      if ((r1(i).toList diff r2(i)).length != 0) println(s"$i ${r1(i).toList diff r2(i)}")
  //    }
  //    val mbrs = trajRDD.collect
  //    for (i <- mbrs) {
  //      if (i.id == 1372702836620000080L) println(s"... $i, ${i.id}")
  //    }

  /** test sub-trajectory with RTree indexing */
  println("\n=== Test querying with partitioning + RTree ===")
  t = nanoTime
  val pRDDWithIndex1 = pRDD.map(x => x._2).mapPartitionsWithIndex {
    (index, partitionIterator) => {
      val partitionsMap = scala.collection.mutable.Map[Int, List[Shape]]()
      var partitionList = List[Shape]()
      while (partitionIterator.hasNext) {
        partitionList = partitionIterator.next() :: partitionList
      }
      partitionsMap(index) = partitionList
      partitionsMap.iterator
    }
  }
  val indexedRDD1 = pRDDWithIndex1.mapPartitions(x => {
    val (pIndex, contents) = x.toArray.head
    val entries = contents.map(x => (x.mbr, x.id)).zipWithIndex.toArray.map(x => (x._1._1, x._1._2.toInt, x._2))
    val rtree = RTree(entries, rtreeCapacity)
    List((pIndex, rtree)).iterator
  })
  val queryPartition1 = pQueryRDD.map { case (k, v) => (v, k) }
  println(s"=== Building index takes ${(nanoTime - t) / 1e9} s.")
  t = nanoTime
  val res2_1 = indexedRDD1.cartesian(queryPartition1)
    .filter(x => x._2._2 == x._1._1)
    .coalesce(numPartitions)
    .map(x => (x._2._1, x._1._2))
    .map { case (query, rtree) => (query.id, rtree.range(query)) }
    .groupByKey()
    .map(x => (x._1, x._2.flatten.map(x => x._1.id).toList.distinct.size))
  println(ListMap(res2_1.collect.toSeq.sortBy(_._1): _*))
  println(s"=== Querying with partitioning and indexing takes ${(nanoTime - t) / 1e9} s.")

  /** test sub-trajectory with reference point */
  println("\n=== Test querying with sub-trajectory, partitioning, RTree + reference point ===")

  t = nanoTime
  val res2_2 = indexedRDD1.cartesian(queryPartition1)
    .filter(x => x._2._2 == x._1._1)
    .coalesce(numPartitions)
    .map(x => (x._2._1, x._1._2, x._1._1))
    .map { case (query, rtree, partitionID) =>
      (query.id,
        rtree.range(query)
          .filter(x => query.referencePoint(x._1).get.inside(partitionRange(partitionID))))
    }
    .groupByKey()
    .map(x => (x._1, x._2.flatten.map(x => x._1.id).toList.distinct.size))
  println(ListMap(res2_2.collect.toSeq.sortBy(_._1): _*))
  println(s"=== Querying with partitioning and indexing takes ${(nanoTime - t) / 1e9} s.")


  /** test sub-trajectory */

  println("\n=== Test querying with sub-trajectory ===")
  val subTrajRDD = trajRDD.flatMap(x => x.genLineSeg().map(line => line.mbr().setID(x.tripID)))

  //    /** full scan */
  //    println("\n=== Test full scan on sub-trajectory ===")
  //    t = nanoTime
  //    val res3 = subTrajRDD.cartesian(queryRDD).filter(x => x._1.intersect(x._2.query)).map {
  //      case (traj, query) => (query.queryID, traj)
  //    }
  //      .groupByKey(numPartitions)
  //      .mapValues(x => x.toArray.length)
  //    // .mapValues(x => x.map(x => x.id))
  //    println(ListMap(res3.collect.toSeq.sortBy(_._1): _*))
  //    println(s"=== Full scan takes ${(nanoTime - t)/1e9} s.")

  // partition
  t = nanoTime
  val (pRDD2, pQueryRDD2) = partitioner.copartition(subTrajRDD, queryRDD)
  println(s"=== Partitioning takes ${(nanoTime - t) / 1e9} s.")
  // query test
  t = nanoTime
  val res4 = pQueryRDD2.cogroup(pRDD2)
    .map {
      case (_, v) => v._1.map(query => (query, v._2.filter(shape => shape.intersect(query))))
    }
    .flatMap(x => x)
    .map(x => (x._1.id, x._2))
    .groupByKey()
    .mapValues(_.flatten.toList.map(x => x.id).distinct.size)
  println(ListMap(res4.collect.toSeq.sortBy(_._1): _*))
  println(s"=== Querying with sub-trajectory + partitioning takes ${(nanoTime - t) / 1e9} s.")

  /** test sub-trajectory with RTree indexing */
  println("\n=== Test querying with sub-trajectory, partitioning + RTree ===")
  t = nanoTime
  val pRDDWithIndex = pRDD2.map(x => x._2).mapPartitionsWithIndex {
    (index, partitionIterator) => {
      val partitionsMap = scala.collection.mutable.Map[Int, List[Shape]]()
      var partitionList = List[Shape]()
      while (partitionIterator.hasNext) {
        partitionList = partitionIterator.next() :: partitionList
      }
      partitionsMap(index) = partitionList
      partitionsMap.iterator
    }
  }
  val indexedRDD = pRDDWithIndex.mapPartitions(x => {
    val (pIndex, contents) = x.toArray.head
    val entries = contents.map(x => (x.mbr, x.id)).zipWithIndex.toArray.map(x => (x._1._1, x._1._2.toInt, x._2))
    val rtree = RTree(entries, rtreeCapacity)
    List((pIndex, rtree)).iterator
  })
  val queryPartition = pQueryRDD2.map { case (k, v) => (v, k) }
  println(s"=== Building index takes ${(nanoTime - t) / 1e9} s.")
  t = nanoTime
  val res5 = indexedRDD.cartesian(queryPartition)
    .filter(x => x._2._2 == x._1._1)
    .coalesce(numPartitions)
    .map(x => (x._2._1, x._1._2))
    .map { case (query, rtree) => (query.id, rtree.range(query)) }
    .groupByKey()
    .map(x => (x._1, x._2.flatten.map(x => x._1.id).toList.distinct.size))
  println(ListMap(res5.collect.toSeq.sortBy(_._1): _*))
  println(s"=== Querying with partitioning and indexing takes ${(nanoTime - t) / 1e9} s.")

  /** test sub-trajectory with reference point */
  println("\n=== Test querying with sub-trajectory, partitioning, RTree + reference point ===")

  t = nanoTime
  val res6 = indexedRDD.cartesian(queryPartition)
    .filter(x => x._2._2 == x._1._1)
    .coalesce(numPartitions)
    .map(x => (x._2._1, x._1._2, x._1._1))
    .map { case (query, rtree, partitionID) =>
      (query.id,
        rtree.range(query)
          .filter(x => query.referencePoint(x._1).get.inside(partitionRange(partitionID))))
    }
    .groupByKey()
    .map(x => (x._1, x._2.flatten.map(x => x._1.id).toList.distinct.size))
  println(ListMap(res6.collect.toSeq.sortBy(_._1): _*))
  println(s"=== Querying with partitioning and indexing takes ${(nanoTime - t) / 1e9} s.")
  sc.stop()
}

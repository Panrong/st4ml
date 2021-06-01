package operators.extraction

import geometry.Point
import operators.repartitioner.TSTRRepartitioner
import org.apache.spark.rdd.RDD

import scala.math.abs
import operators.selection.partitioner._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import utils.Config


class PointCompanionExtractor extends BaseExtractor[Point] with Serializable {

  def isCompanion(sThreshold: Double, tThreshold: Double)(p1: Point, p2: Point): Boolean = {
    abs(p1.timeStamp._1 - p2.timeStamp._1) <= tThreshold &&
      abs(p1.geoDistance(p2)) <= sThreshold &&
      p1.attributes("tripID") != p2.attributes("tripID")
  }

  // find all companion pairs with native spark implementation
  def extract(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point]): RDD[(String, Map[Long, String])] = {
    pRDD.cartesian(pRDD).filter {
      case (p1, p2) =>
        isCompanion(sThreshold, tThreshold)(p1, p2)
    }.map {
      case (p1, p2) => (p1.id, (p1.timeStamp._1, p2.id))
    }.groupByKey.mapValues(_.toMap).reduceByKey(_ ++ _)
  }

  // find all companion pairs, make sure that the pRDD is partitioned with overlap to ensure correctness
  def optimizedExtract(sThreshold: Double, tThreshold: Double)
                      (pRDD: RDD[Point]): RDD[(String, Map[Long, String])] = {

    val numPartitions = pRDD.getNumPartitions

    val repartitioner = new TSTRRepartitioner[Point](Config.get("tPartition").toInt,
      sThreshold, tThreshold, Config.get("samplingRate").toDouble)
    val rdd = repartitioner.partition(pRDD)
      .mapPartitionsWithIndex((id, p) => p.map((id, _)))

    // val partitioner = new TemporalPartitioner(startTime = pRDD.map(_.timeStamp._1).min,
    //      endTime = pRDD.map(_.timeStamp._2).max, numPartitions = numPartitions)
    //    val repartitionedRDD = partitioner.partitionGrid(pRDD, 2, tOverlap = tThreshold, sOverlap = sThreshold) // temporal + grid
    //    val repartitionedRDD = partitioner.partitionWithOverlap(pRDD, tThreshold) // temporal only
    //    val repartitionedRDD = partitioner.partitionSTR(pRDD, tPartition, tThreshold, sThreshold, Config.get("samplingRate").toDouble) //temporal + str

    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

    /** v1: for yield */
    //    repartitionedRDD.mapPartitions(x => {
    //      val points = x.toStream.map(_._2)
    //      for (p1 <- points;
    //           p2 <- points
    //           if isCompanion(tThreshold, sThreshold)(p1, p2)
    //           ) yield (p1.id, Array((p1.timeStamp._1, p2.id)))
    //    }.toIterator)
    //      .mapValues(_.toMap)
    //      .reduceByKey(_ ++ _, 1000)

    /** v2: join */
    rdd.join(rdd).map(_._2).filter {
      case (p1, p2) => isCompanion(sThreshold, tThreshold)(p1, p2)
    }.map {
      case (p1, p2) => (p1.id, Array((p1.timeStamp._1, p2.id)))
    }
      .mapValues(_.toMap)
      .reduceByKey(_ ++ _, numPartitions * 4)
  }

  // find all companion pairs, make sure that the pRDD is partitioned with overlap to ensure correctness
  def extractSTR(sThreshold: Double, tThreshold: Double)
                (pRDD: RDD[Point]): RDD[(String, Map[Long, String])] = {

    val numPartitions = pRDD.getNumPartitions

    val repartitioner = new STRPartitioner(Config.get("numPartitions").toInt,
      Some(Config.get("samplingRate").toDouble), sThreshold)
    val rdd = repartitioner.partition(pRDD)
      .mapPartitionsWithIndex((id, p) => p.map((id, _)))
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

    rdd.join(rdd).map(_._2).filter {
      case (p1, p2) => isCompanion(sThreshold, tThreshold)(p1, p2)
    }.map {
      case (p1, p2) => (p1.id, Array((p1.timeStamp._1, p2.id)))
    }
      .mapValues(_.toMap)
      .reduceByKey(_ ++ _, numPartitions * 4)
  }

  // find all companion pairs, make sure that the pRDD is partitioned with overlap to ensure correctness
  def extractQuadTree(sThreshold: Double, tThreshold: Double)
                (pRDD: RDD[Point]): RDD[(String, Map[Long, String])] = {

    val numPartitions = pRDD.getNumPartitions

    val repartitioner = new QuadTreePartitioner(Config.get("numPartitions").toInt,
      Some(Config.get("samplingRate").toDouble), sThreshold)
    val rdd = repartitioner.partition(pRDD)
      .mapPartitionsWithIndex((id, p) => p.map((id, _)))
    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

    rdd.join(rdd).map(_._2).filter {
      case (p1, p2) => isCompanion(sThreshold, tThreshold)(p1, p2)
    }.map {
      case (p1, p2) => (p1.id, Array((p1.timeStamp._1, p2.id)))
    }
      .mapValues(_.toMap)
      .reduceByKey(_ ++ _, numPartitions * 4)
  }

  //find companion pairs of some queries with native spark implementation
  def queryWithIDs(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point], queryRDD: RDD[Point]): RDD[(String, Map[Long, String])] = {
    queryRDD.cartesian(pRDD).filter {
      case (p1, p2) =>
        isCompanion(sThreshold, tThreshold)(p1, p2)
    }.map {
      case (p1, p2) => (p1.attributes("tripID"), (p2.timeStamp._1, p2.attributes("tripID")))
    }.groupByKey.mapValues(_.toMap).reduceByKey(_ ++ _)
  }

  //find companion pairs of some queries, make sure that the pRDD is partitioned with overlap to ensure correctness
  def optimizedQueryWithIDs(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point],
                                                                    queries: Array[Point],
                                                                    tPartition: Int = 4): RDD[(String, Map[Long, String])] = {
    val numPartitions = pRDD.getNumPartitions
    val partitioner = new TemporalPartitioner(startTime = pRDD.map(_.t).min,
      endTime = pRDD.map(_.t).max, numPartitions = numPartitions)
    val repartitionedRDD = partitioner.partitionSTR(pRDD, tPartition, tThreshold, sThreshold, Config.get("samplingRate").toDouble) //temporal + str
    val queriesBroadcast = SparkContext.getOrCreate().broadcast(queries)
    repartitionedRDD.flatMap(p1 =>
      queriesBroadcast.value.filter(p2 => isCompanion(sThreshold, tThreshold)(p1, p2))
        .map(p2 => (p2.attributes("tripID"), Array((p1.timeStamp._1, p1.attributes("tripID"))))))
      .mapValues(_.toMap)
      .reduceByKey(_ ++ _, numPartitions * 4)
  }

  //  //find companion pairs of some queries
  //  def optimizedQueryWithIDs(sThreshold: Double, tThreshold: Double)(pRDD: RDD[Point],
  //                                                                    queries: Array[Point],
  //                                                                    tPartition: Int = 4): Array[(String, Array[(Long,String)])] = {
  //    val numPartitions = pRDD.getNumPartitions
  //    val partitioner = new TemporalPartitioner(startTime = pRDD.map(_.t).min,
  //      endTime = pRDD.map(_.t).max, numPartitions = numPartitions)
  //    val repartitionedRDD = partitioner.partitionSTR(pRDD, tPartition, tThreshold, sThreshold, Config.get("samplingRate").toDouble).map(_._2) //temporal + str
  //    val extractor = new CompanionQueryExtractor(tThreshold, sThreshold, queries)
  //    extractor.extract(repartitionedRDD)
  //  }
}
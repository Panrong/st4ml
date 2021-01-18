package query

import geometry.{Rectangle, Shape, Trajectory}
import selection.indexer.RTree
import org.apache.spark.sql.functions.{asc, col, collect_list, count}
import org.apache.spark.sql.{Dataset, SparkSession}
import partitioner.{STRPartitioner, gridPartitioner}
import preprocessing.{ReadQueryFile, TrajMBRQuery, TrajectoryWithMBR, ReadTrajFile, resRangeQuery}
import STInstance._
class QuerySubmitter(trajDS: Dataset[Trajectory], queryDS: Dataset[Query2d], numPartitions: Int) {
  val qDS: Dataset[Query2d] = queryDS
  val tDS: Dataset[Trajectory] = trajDS

  def queryWithRDD(queryDS: Dataset[Query2d])(trajDS: Dataset[Trajectory]): Dataset[resRangeQuery] = {
    println("==== START QUERY WITH RDD")
    val trajRDD = trajDS.rdd.map(x => (x.tripID, x.mbr))
    val queryRDD = queryDS.rdd
    val res = trajRDD.cartesian(queryRDD)
      .filter { case ((_, center), query) => center.intersect(query.query) }
      .coalesce(numPartitions)
      .map { case ((tripID, _), query) => (query.queryID, tripID) }
      .groupByKey()
      .map { case (k, v) => (k, v.toList, v.size.toLong) }
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val resDS = res.toDS
      .withColumnRenamed("_1", "queryID")
      .withColumnRenamed("_2", "trips")
      .withColumnRenamed("_3", "count")
      .as[resRangeQuery]
    (resDS join(queryDS, resDS("queryID") === queryDS("queryID")) drop queryDS.col("queryID"))
      .select("queryID", "query", "trips", "count")
      .orderBy(asc("queryID")).as[resRangeQuery]
  }

  def queryWithDS(queryDS: Dataset[Query2d])(trajDS: Dataset[Trajectory]): Dataset[resRangeQuery] = {
    println("==== START QUERY WITH DATASET")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    def addMBR(ds: Dataset[Trajectory]): Dataset[TrajectoryWithMBR] = {
      val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      val trajRDD = ds.rdd
      trajRDD.map(traj => TrajectoryWithMBR(traj.tripID, traj.startTime, traj.points, traj.mbr.coordinates))
        .toDS()
        .as[TrajectoryWithMBR]
    }

    val trajMbrDS = addMBR(trajDS)
    //    println("=== trajWithMBR DS: ")
    //    trajMbrDS.show(5)

    def rangeQuery(queryDS: Dataset[Query2d])(trajMbrDs: Dataset[TrajectoryWithMBR]): Dataset[resRangeQuery] = {
      trajMbrDs.join(queryDS).as[TrajMBRQuery]
        .filter(x => x.query.intersect(Rectangle(x.mbr)))
        .groupBy(col("queryID"))
        .agg(collect_list("tripID").as("trips"),
          count("tripID").as("count"))
        .as[resRangeQuery]
    }

    val resDS = trajMbrDS.transform(rangeQuery(queryDS))
    (resDS join(queryDS, resDS("queryID") === queryDS("queryID")) drop queryDS.col("queryID"))
      .select("queryID", "query", "trips", "count")
      .orderBy(asc("queryID")).as[resRangeQuery]
  }

  def queryWithPartitioner(queryDS: Dataset[Query2d])
                          (samplingRate: Double, partitioner: String = "STR")
                          (trajDS: Dataset[Trajectory]): Dataset[resRangeQuery] = {

    println(s"==== START QUERY WITH ${partitioner.toUpperCase} PARTITIONER")
    val trajRDD = trajDS.rdd.map(x => x.mbr.assignID(x.tripID))
    val queryRDD = queryDS.rdd

    var pRDD = trajRDD
    var gridBound: Map[Int, Rectangle] = Map()

    partitioner match {
      case "grid" =>
        val r = gridPartitioner(trajRDD, numPartitions, samplingRate)
        pRDD = r._1
        gridBound = r._2
      case "STR" | "str" =>
        val r = STRPartitioner(trajRDD, numPartitions, samplingRate)
        pRDD = r._1
        gridBound = r._2
      case _ => throw new Exception("partitioner not supported")
    }

    val pRDDWithIndex = pRDD.mapPartitionsWithIndex {
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
    val queryPartiton = queryRDD.map(query => (query, gridBound.filter {
      case (_, bound) => bound.intersect(query.query)
    }.keys.toArray))
      .flatMapValues(x => x)
    val res = pRDDWithIndex.cartesian(queryPartiton)
      .filter(x => x._2._2 == x._1._1)
      .coalesce(numPartitions)
      .map(x => (x._2._1, x._1._2))
      .map { case (query, points) => (query.queryID, points.filter(point => point.intersect(query.query))) }
      .groupByKey()
      .map(x => (x._1, x._2.flatten.map(x => x.id).toList, x._2.flatten.size.toLong))
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val resDS = res.toDS
      .withColumnRenamed("_1", "queryID")
      .withColumnRenamed("_2", "trips")
      .withColumnRenamed("_3", "count")
      .as[resRangeQuery]

    (resDS join(queryDS, resDS("queryID") === queryDS("queryID")) drop queryDS.col("queryID"))
      .select("queryID", "query", "trips", "count")
      .orderBy(asc("queryID")).as[resRangeQuery]
  }

  def queryWithIndex(queryDS: Dataset[Query2d])
                    (samplingRate: Double, max_entries_per_node: Int, partitioner: String = "STR")
                    (trajDS: Dataset[Trajectory]): Dataset[resRangeQuery] = {

    println(s"==== START QUERY WITH RTREE INDEXING AND ${partitioner.toUpperCase} PARTITIONER")
    val trajRDD = trajDS.rdd.map(x => x.mbr.assignID(x.tripID))
    val queryRDD = queryDS.rdd

    var pRDD = trajRDD
    var gridBound: Map[Int, Rectangle] = Map()

    partitioner match {
      case "grid" =>
        val r = gridPartitioner(trajRDD, numPartitions, samplingRate)
        pRDD = r._1
        gridBound = r._2
      case "STR" | "str" =>
        val r = STRPartitioner(trajRDD, numPartitions, samplingRate)
        pRDD = r._1
        gridBound = r._2
      case _ => throw new Exception("partitioner not supported")
    }

    val pRDDWithIndex = pRDD.mapPartitionsWithIndex {
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
      val entries = contents.map(x => x.center()).zipWithIndex.toArray
      val rtree = RTree(entries, max_entries_per_node)
      List((pIndex, rtree)).iterator
    })
    val queryPartition = queryRDD.map(query => (query, gridBound.filter {
      case (_, bound) => bound.intersect(query.query)
    }.keys.toArray))
      .flatMapValues(x => x)

    val res = indexedRDD.cartesian(queryPartition)
      .filter(x => x._2._2 == x._1._1)
      .coalesce(numPartitions)
      .map(x => (x._2._1, x._1._2))
      .map { case (query, rtree) => (query.queryID, rtree.range(query.query)) }
      .groupByKey()
      .map(x => (x._1, x._2.flatten.map(x => x._2.toLong).toList, x._2.flatten.size.toLong))

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val resDS = res.toDS
      .withColumnRenamed("_1", "queryID")
      .withColumnRenamed("_2", "trips")
      .withColumnRenamed("_3", "count")
      .as[resRangeQuery]

    (resDS join(queryDS, resDS("queryID") === queryDS("queryID")) drop queryDS.col("queryID"))
      .select("queryID", "query", "trips", "count")
      .orderBy(asc("queryID")).as[resRangeQuery]
  }
}

object QuerySubmitter {

  def apply(trajectoryFile: String, queryFile: String, numPartitions: Int, dataSize: Int = Double.PositiveInfinity.toInt): QuerySubmitter = {
    val trajDS = ReadTrajFile(trajectoryFile, num = dataSize)
    val trajRDD = trajDS.rdd.map(x=>STInstance.implicits.geometry2Instance(x))
    val queryDS = ReadQueryFile(queryFile)
    val queryRDD = queryDS.rdd
    new QuerySubmitter(trajDS, queryDS, numPartitions)
  }
}
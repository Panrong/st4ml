package query

import geometry.{Point, Rectangle, Shape, Trajectory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import partitioner.{STRPartitioner, gridPartitioner}
import preprocessing.{Query, resRangeQuery}

import java.lang.System.nanoTime
import scala.reflect.ClassTag

object QueryWithPartitioner {
  def apply(trajDS: Dataset[Trajectory], queryDS: Dataset[preprocessing.Query],
            numPartitions: Int, samplingRate: Double, partitioner:String = "STR"): Dataset[resRangeQuery] = {

    println(s"==== START QUERY WITH ${partitioner.toUpperCase} PARTITIONER")
    val trajRDD = trajDS.rdd.map(x => x.mbr.assignID(x.tripID))
    val queryRDD = queryDS.rdd

    var pRDD = trajRDD
    var gridBound : Map[Int, Rectangle] = Map()

    partitioner match {
      case "grid" =>
        val r = gridPartitioner(trajRDD, numPartitions, samplingRate)
        pRDD = r._1
        gridBound = r._2
      case "STR" | "str" =>
        val r = STRPartitioner(trajRDD, numPartitions, samplingRate)
        pRDD = r._1
        gridBound = r._2
      case _ => throw new Exception("Partitioner not supported")
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
    res.toDS
      .withColumnRenamed("_1", "queryID")
      .withColumnRenamed("_2", "trips")
      .withColumnRenamed("_3", "count")
      .as[resRangeQuery]
  }
}
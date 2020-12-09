package query

import geometry.Trajectory
import org.apache.spark.sql.{Dataset, SparkSession}
import preprocessing._

object queryWithRDD {
  def apply(trajDS: Dataset[Trajectory], queryDS: Dataset[Query], numPartitions: Int): Dataset[resRangeQuery] = {
    println("==== START QUERY WITH RDD")
    val trajRDD = trajDS.rdd.map(x => (x.tripID, x.mbr.center()))
    val queryRDD = queryDS.rdd
    val res = trajRDD.cartesian(queryRDD)
      .filter { case ((_, center), query) => center.inside(query.query) }
      .coalesce(numPartitions)
      .map { case ((tripID, _), query) => (query.queryID, tripID) }
      .groupByKey()
      .map { case (k, v) => (k, v.toList, v.size.toLong) }
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    res.toDS
      .withColumnRenamed("_1", "queryID")
      .withColumnRenamed("_2", "trips")
      .withColumnRenamed("_3", "count")
      .as[resRangeQuery]
  }
}

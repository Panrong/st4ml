package query

import geometry.{Rectangle, Trajectory}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, count, countDistinct, length, size}
import preprocessing._

object QueryWithDS extends App {

  def apply(trajDS: Dataset[Trajectory], queryDS:Dataset[Query]): Dataset[resRangeQuery] = {
    println("==== START QUERY WITH DATASET")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val trajMbrDS = addMBR(trajDS)
    println("=== trajWithMBR DS: ")
    trajMbrDS.show(5)
    def rangeQuery(queryDS: Dataset[Query])(trajMbrDs: Dataset[TrajectoryWithMBR]): Dataset[resRangeQuery] = {
      trajMbrDs.join(queryDS).as[TrajMBRQuery]
        .filter(x => x.query.intersect(Rectangle(x.mbr)))
        .groupBy(col("queryID"))
        .agg(collect_list("tripID").as("trips"),
          count("tripID").as("count"))
        .as[resRangeQuery]
    }

    trajMbrDS.transform(rangeQuery(queryDS))
  }

  def addMBR(ds: Dataset[Trajectory]): Dataset[TrajectoryWithMBR] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val trajRDD = ds.rdd
    trajRDD.map(traj => TrajectoryWithMBR(traj.tripID, traj.startTime, traj.points, traj.mbr.coordinates))
      .toDS()
      .as[TrajectoryWithMBR]
  }
}

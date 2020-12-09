//package query
//
//import geometry.{Point, Rectangle, Trajectory}
//import org.apache.spark.sql.{Dataset, Row}
//import org.apache.spark.sql.functions.{col, collect_list, size}
//import preprocessing.{Query, SparkSessionWrapper, TrajMBRQuery, TrajectoryWithMBR, readQueryFile, readTrajFile, resRangeQuery}
//
//import java.lang.System.nanoTime
//
///** required case classes */
////case class Query(query: Rectangle, queryID: Long) extends Serializable
////
////case class TrajectoryWithMBR(tripID: Long, startTime: Long, points: Array[Point],
////                             mbr: Array[Double]) extends Serializable
////
////case class TrajMBRQuery(tripID: Long, startTime: Long, points: Array[Point], mbr: Array[Double],
////                        query: Rectangle) extends Serializable
////
////case class resRangeQuery(queryID: Long, trips: List[Long], count: Long) extends Serializable
//
//object queryDS extends App {
//
//  override def main(args: Array[String]): Unit = {
//    val ss = new SparkSessionWrapper("config")
//    val spark = ss.spark
//    val sc = ss.sc
//
//    val trajectoryFile = args(0)
//    val queryFile = args(1)
//    val numPartitions = args(2).toInt
//    val samplingRate = args(3).toDouble
//    val dataSize = args(4).toInt
//
//    /** generate trajectory MBR RDD */
//    val trajDS = readTrajFile(trajectoryFile, num = dataSize)
//
//    /** generate query RDD */
//    val queryDS = readQueryFile(queryFile)
//    println("=== query DS: ")
//    queryDS.show(5)
//
//    println("=== traj DS: ")
//    trajDS.show(5)
//
//    val trajMbrDS = addMBR(trajDS)
//
//    println("=== trajWithMBR DS: ")
//    trajMbrDS.show(5)
//
//    def rangeQuery(queryDs: Dataset[Query])(trajMbrDs: Dataset[TrajectoryWithMBR]): Dataset[resRangeQuery] = {
//      trajMbrDs.join(queryDs).as[TrajMBRQuery]
//        .filter(x => x.query.intersect(Rectangle(x.mbr)))
//        .groupBy(col("queryID"))
//        .agg(collect_list("tripID").alias("trips"))
//        .withColumn("count", size($"trips"))
//        .as[resRangeQuery]
//    }
//
//    println("=== range query results: ")
//    trajMbrDS
//      .transform(rangeQuery(queryDS))
//      .show(5)
//
//
//    sc.stop()
//  }
//
//  def addMBR(ds: Dataset[Trajectory]): Dataset[TrajectoryWithMBR] = {
//    val ss = new SparkSessionWrapper("config")
//    val spark = ss.spark
//
//    import spark.implicits._
//    val trajRDD = ds.rdd
//    trajRDD.map(traj => TrajectoryWithMBR(traj.tripID, traj.startTime, traj.points, traj.mbr.coordinates))
//      .toDS()
//      .as[TrajectoryWithMBR]
//  }
//
//
//}

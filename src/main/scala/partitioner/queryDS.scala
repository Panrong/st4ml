//package partitioner
//
//import geometry.{Point, Rectangle, Trajectory}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Dataset, Row}
//import org.apache.spark.sql.functions.{col, collect_list, struct}
//import preprocessing.readTrajFile
//
//import java.lang.System.nanoTime
//
//case class Query(bottomLeft: Point, topRight: Point, queryID: Long) extends Serializable
//
//case class TrajectoryWithMBR(tripID: Long, taxiID: Long, startTime: Long, points: Array[Point], mbr: Rectangle) extends Serializable
//
//case class TrajMBRQuery(tripID: Long, taxiID: Long, startTime: Long, points: Array[Point], mbr: Rectangle,
//                        bottomLeft: Point, TopRight: Point) extends Serializable
//
//case class resRangeQuery(queryID: Long, trips: List[Long]) extends Serializable
//
//object queryDS extends App with preprocessing.SparkSessionWrapper {
//  override def main(args: Array[String]): Unit = {
//
//    val trajectoryFile = args(1)
//    val queryFile = args(2)
//    val numPartitions = args(3).toInt
//    val samplingRate = args(4).toDouble
//    val dataSize = args(5).toInt
//
//    import spark.implicits._
//
//    /** generate trajectory MBR RDD */
//    val trajDS = readTrajFile(trajectoryFile, dataSize)
//
//    /** generate query RDD */
//    val queries = preprocessing.readQueryFile(queryFile)
//    val queryDS = queries.toSeq.toDS.withColumnRenamed("ID", "queryID").as[Query]
//    queryDS.show(5)
//
//    var t = nanoTime()
//
//    import spark.implicits._
//    val trajMbrDS = addMBR(trajDS) //.toDS()
//
//    trajMbrDS.show(2)
//    trajMbrDS
//      .transform(rangeQuery(queryDS))
//      .show(5)
//
//    def rangeQuery(queryDs: Dataset[Query])(trajMbrDs: Dataset[TrajectoryWithMBR]): Dataset[resRangeQuery] = {
//      trajMbrDs.join(queryDs).as[TrajMBRQuery]
//        .filter(x => Rectangle(Array.concat(x.bottomLeft.coordinates, x.TopRight.coordinates)).intersect(x.mbr))
//        .groupBy(col("queryID"))
//        .agg(collect_list("tripID").alias("trips")).as[resRangeQuery]
//    }
//
//    sc.stop()
//  }
//
//  //  def addMBR(ds: Dataset[Trajectory]): RDD[TrajectoryWithMBR] = {
//  //    val trajRDD = ds.rdd
//  //    trajRDD.map(traj => TrajectoryWithMBR(traj.tripID, traj.taxiID, traj.startTime, traj.points, mbr = {
//  //      val lons = traj.points.map(_.lon).sorted
//  //      val lats = traj.points.map(_.lat).sorted
//  //      Rectangle(Point(lons.head, lats.head), Point(lons.last, lats.last))
//  //    }))
//  //  }
//  def addMBR(ds: Dataset[Trajectory]): Dataset[TrajectoryWithMBR] = {
//    import spark.implicits._
//    val rectangle = ds.select("points").map { case Row(points) => {
//      println(points.getClass)
//      val p = points.asInstanceOf[Array[Point]]
//      val lons = p.map(_.lon).sorted
//      val lats = p.map(_.lat).sorted
//      Rectangle(Point(lons.head, lats.head), Point(lons.last, lats.last))
//      }
//    }
//    rectangle.show(4)
//    val res = ds.withColumn("mbr",
//      struct(
//        rectangle.col("bottomLeft"),
//        rectangle.col("topRight"))
//    )
//    res.show(4)
//    res.as[TrajectoryWithMBR]
//  }
//}

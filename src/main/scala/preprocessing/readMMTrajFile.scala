package preprocessing

import geometry.{mmTrajectoryS, subTrajectory}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object readMMTrajFile extends SparkSessionWrapper{
  /**
   *
   * @param filename : path to the data file
   * @return : Dataset[mmTrajectoryS]
   * +-------------------+--------+----------+--------------------+
   * |             tripID|  taxiID| startTime|     subTrajectories|
   * +-------------------+--------+----------+--------------------+
   * |1372945809620000443|20000443|1372945816|[[1372945816, 0, ...|
   * |1373005493620000136|20000136|1373005493|[[1373005493, 0, ...|
   * |1373009069620000595|20000595|1373009121|[[1373009121, 0, ...|
   * |1373008468620000039|20000039|1373008468|[[1373008468, 0, ...|
   * |1373009580620000009|20000009|1373009580|[[1373009580, 0, ...|
   * +-------------------+--------+----------+--------------------+
   *
   */
  def apply(filename: String): Dataset[mmTrajectoryS] = {
    val customSchema = StructType(Array(
      StructField("taxiID", LongType, nullable = true),
      StructField("tripID", LongType, nullable = true),
      StructField("GPSPoints", StringType, nullable = true),
      StructField("VertexID", StringType, nullable = true),
      StructField("Candidates", StringType, nullable = true),
      StructField("pointRoadPair", StringType, nullable = true),
      StructField("RoadTime", StringType, nullable = true))
    )
    import spark.implicits._
    val df = spark.read.option("header", "true").schema(customSchema).csv(filename)
    val trajRDD = df.rdd.filter(row => row(3) != "(-1:-1)" && row(3) != "-1") // remove invalid entries
    val resRDD = trajRDD.map(row => {
      val tripID = row(1).toString
      val taxiID = row(0).toString
      val roadTime = row(6).toString.replaceAll("[() ]", "").split(",").grouped(2).toArray // Array(Array(roadID, time))
      val subTrajectories = roadTime.map(x => subTrajectory(x(1).toLong, 0, x(0), 0))
      mmTrajectoryS(tripID, taxiID, subTrajectories(0).startTime, subTrajectories)
    })
    resRDD.toDS
  }
}

//object readMMTrajTest extends App {
//  override def main(args: Array[String]): Unit = {
//    /** set up Spark */
//    readMMTrajFile("datasets/mm100000.csv").show(5)
//  }
//}
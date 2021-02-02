package preprocessing

import geometry.mmTrajectory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import geometry.road.RoadGrid
import org.apache.spark.sql.SparkSession

object ReadMMTrajFile extends Serializable {
  /**
   *
   * @param filename : path to the dataRDD file
   * @return : Dataset[mmTrajectoryS]
   *         +-------------------+--------+----------+--------------------+
   *         |             tripID|  taxiID| startTime|     subTrajectories|
   *         +-------------------+--------+----------+--------------------+
   *         |1372945809620000443|20000443|1372945816|[[1372945816, 0, ...|
   *         |1373005493620000136|20000136|1373005493|[[1 3 7 3 0 0 5 4 9 3, 0, ...|
   *         |1373009069620000595|20000595|1373009121|[[1373009121, 0, ...|
   *         |1373008468620000039|20000039|1373008468|[[1 3 7 3 0 0 8 4 6 8, 0, ...|
   *         |1373009580620000009|20000009|1373009580|[[1373009580, 0, ...|
   *         +-------------------+--------+----------+--------------------+
   *
   */
  def apply(filename: String, mapFile: String): RDD[mmTrajectory] = {
    val spark = SparkSession.builder().getOrCreate()
    val rGrid = RoadGrid(mapFile)
    val roadMap = rGrid.id2edge.map {
      case (id, edge) =>
        val startCoord = rGrid.id2vertex(edge.from).point
        val endCoord = rGrid.id2vertex(edge.to).point
        (id, List(startCoord, endCoord))
    }
    val customSchema = StructType(Array(
      StructField("taxiID", LongType, nullable = true),
      StructField("tripID", LongType, nullable = true),
      StructField("GPSPoints", StringType, nullable = true),
      StructField("VertexID", StringType, nullable = true),
      StructField("Candidates", StringType, nullable = true),
      StructField("pointRoadPair", StringType, nullable = true),
      StructField("RoadTime", StringType, nullable = true))
    )
    val df = spark.read.option("header", "true").schema(customSchema).csv(filename)
    val trajRDD = df.rdd.filter(row => row(2) != "(-1:-1)" && row(2) != "-1") // remove invalid entries
    val resRDD = trajRDD.map(row => {
      val tripID = row(0).toString
      val roadTime = row(5).toString.replaceAll("[() ]", "").split(",").grouped(2).toArray // Array(Array(roadID, time))
      val subTrajectories = roadTime.map(x => (x(0), x(1).toLong))
      mmTrajectory(tripID, subTrajectories).addMBR(roadMap)
    })
    resRDD
  }
}

package st4ml.operators.converter

import st4ml.instances.Trajectory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import st4ml.utils.mapmatching.MapMatcher
import st4ml.utils.mapmatching.road.RoadGrid

import scala.reflect.ClassTag

class Traj2TrajConverter(roadGrid: RoadGrid,
                         candidateThresh: Double = 50,
                         sigmaZ: Double = 4.07,
                         beta: Double = 20) extends Converter {
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  override val optimization: String = ""

  val mapMatcher = new MapMatcher(roadGrid)
  val mm: Broadcast[MapMatcher] = spark.sparkContext.broadcast(mapMatcher)

  def convert[V: ClassTag, D: ClassTag](input: RDD[Trajectory[V, D]]): RDD[Trajectory[String, String]] = {
    input.map(traj => mm.value.mapMatch(traj, candidateThresh, sigmaZ, beta))
  }

  def convertWithInterpolation[V: ClassTag, D: ClassTag](input: RDD[Trajectory[V, D]],
                                                         inferTime: Boolean = false): RDD[Trajectory[String, String]] = {
    input.map(traj => mm.value.mapMatchWithInterpolation(traj, candidateThresh, sigmaZ, beta, inferTime))
  }
}

object Traj2TrajConverter {
  // deprecated (used for the old map format)
  def apply(mapFile: String,
            candidateThresh: Double = 100,
            sigmaZ: Double = 4.07, beta: Double = 20): Traj2TrajConverter =
    new Traj2TrajConverter(RoadGrid(mapFile), candidateThresh, sigmaZ, beta)
}

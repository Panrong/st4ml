package operatorsNew.converter

import instances.{Event, Point, Trajectory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class Traj2TrajConverter[V: ClassTag](mapFile: String,
                                      candidateThresh: Double = 50,
                                      sigmaZ: Double = 4.07,
                                      beta: Double = 20) extends Converter {
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  override val optimization: String = ""
  type I = Trajectory[V, String]
  type O = Trajectory[String, String]
  val mapMatcher = new MapMatcher(mapFile)
  val mm: Broadcast[MapMatcher] = spark.sparkContext.broadcast(mapMatcher)

  def convert(input: RDD[I]): RDD[O] = {
    input.map(traj => mm.value.mapMatch(traj, candidateThresh, sigmaZ, beta))
  }

  def convertWithInterpolation(input: RDD[I], inferTime: Boolean = false): RDD[O] = {
    input.map(traj => mm.value.mapMatchWithInterpolation(traj, candidateThresh, sigmaZ, beta, inferTime))
  }
}

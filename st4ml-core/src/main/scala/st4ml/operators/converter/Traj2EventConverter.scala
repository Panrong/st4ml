package st4ml.operators.converter

import st4ml.instances.{Event, Point, Trajectory}
import org.apache.spark.rdd.RDD

class Traj2EventConverter {

  def convert[TV, TD](input: RDD[Trajectory[TV, TD]]): RDD[Event[Point, TV, TD]] = {
    val entries = input.flatMap(traj => {
      val points = traj.entries
      val attribute = traj.data
      points.map(point => (Array(point), attribute))
    })
    entries.map(e => Event(e._1, e._2))
  }
}

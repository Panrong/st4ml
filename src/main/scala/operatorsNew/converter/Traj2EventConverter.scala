package operatorsNew.converter
import instances.{Event, Point, Trajectory}
import org.apache.spark.rdd.RDD

class Traj2EventConverter[TV, TD] extends Converter {
  type I = Trajectory[TV, TD]
  type O = Event[Point, TV, TD]

  override def convert(input: RDD[I]): RDD[O] = {
    val entries = input.flatMap(traj => {
      val points = traj.entries
      val attribute = traj.data
      points.map(point => (Array(point), attribute))
    })
    entries.map(e => Event(e._1, e._2))
  }
}

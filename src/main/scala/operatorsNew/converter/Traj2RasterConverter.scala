package operatorsNew.converter

import instances.{Duration, Entry, Geometry, Polygon, Raster, Trajectory}
import org.apache.spark.rdd.RDD

class Traj2RasterConverter[V, D, VR, DR](f: Array[Trajectory[V, D]] => VR,
                                         stArray: Array[(Polygon, Duration)],
                                         d: DR = None) extends Converter {
  type I = Trajectory[V, D]
  type O = Raster[Polygon, VR, DR]

  val stMap: Array[(Int, (Polygon, Duration))] = stArray.zipWithIndex.map(_.swap)

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val trajSlots = partition.map(traj => {
        val slots = stMap.filter {
          case (_, (s, t)) => t.intersects(traj.duration) && s.intersects(traj.extent)
        }
        (traj, slots.map(_._1))
      }).flatMap {
        case (event, slots) => slots.map(slot => (slot, event)).toIterator
      }.toArray.groupBy(_._1).mapValues(x => x.map(_._2)).toArray

      val entries = trajSlots.map(slot => {
        val spatial = stMap(slot._1)._2._1
        val temporal = stMap(slot._1)._2._2
        val v = f(slot._2)
        new Entry(spatial, temporal, v)
      })
      val raster = new Raster[Polygon, VR, DR](entries, data = d)
      Iterator(raster)
    })
  }

}
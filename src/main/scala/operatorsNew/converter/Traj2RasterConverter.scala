package operatorsNew.converter

import instances.{Duration, Entry, Geometry, Polygon, Raster, Trajectory}
import org.apache.spark.rdd.RDD

class Traj2RasterConverter[V, D, VR, DR](f: Array[Trajectory[V, D]] => VR,
                                         polygonArr: Array[Polygon],
                                         durArr: Array[Duration],
                                         d: DR = None) extends Converter {
  type I = Trajectory[V, D]
  type O = Raster[Polygon, VR, DR]

  override def convert(input: RDD[I]): RDD[O] = {
    input.mapPartitions(partition => {
      val trajs = partition.toArray
      val emptyRaster = Raster.empty[I](polygonArr, durArr)
      Iterator(emptyRaster.attachInstance(trajs)
        .mapValue(f)
        .mapData(_ => d))
    })
  }
}
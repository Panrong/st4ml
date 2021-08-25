package operatorsNew.converter

import instances.{Duration, Entry, Geometry, LineString, Polygon, Raster, Trajectory}
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
      val queryArr = trajs.map(traj => {
        val s = traj.spatialSliding(2).map(p => LineString(p(0), p(1)))
        val t = traj.temporalSliding(2).map(t => Duration(t(0).start, t(1).end))
        (s zip t).toArray
      }) // convert trajectory to Array[(LineString, Duration)]
      Iterator(emptyRaster.attachInstanceExact(trajs, queryArr)
        .mapValue(f)
        .mapData(_ => d))
    })
  }
}
package STInstance

import scala.reflect.ClassTag

object implicits {
  implicit def trajectory2Points[T](traj: Trajectory[(Double, Double), T]): Array[Point[Long]] = {
    implicit def tuple2Array(a: (Double, Double)): Array[Double] = Array(a._1, a._2)

    traj.coord.zipWithIndex
      .map(x => Point(id = x._2, coord = x._1._1, timeStamp = x._1._2, property = Some(Map(("trajID", traj.id)))))
  }

  implicit def trajectory2SMVertices[T](traj: Trajectory[String, T]): Map[String, (Long, Long)] = {
    /**
     * return (roadSegID -> (trajID, time))
     */
    traj.coord
      .groupBy(_._1).mapValues(_.map(_._2))
      .mapValues(x => (traj.id, x.sum / x.length))
  }

  implicit def vertices2SM(vertices: Map[String, (Long, Long)]): SpatialMap[(Long, Long), Null] =
    SpatialMap(id = 0, vertices, property = None)

  implicit def trajectory2SM[T](traj: Trajectory[String, T]): SpatialMap[(Long, Long), Null] =
    vertices2SM(trajectory2SMVertices(traj))

  implicit def geometry2Instance(p: geometry.Point): Point[Null] =
    Point(p.id, p.coordinates, p.t, None)

  implicit def geometry2Instance(traj: geometry.Trajectory):
  Trajectory[(Double, Double), String] =
    Trajectory(traj.tripID,
      traj.points.map(x => ((x.coordinates(0), x.coordinates(1)), x.timeStamp._1)),
      Some(traj.attributes))

  implicit def instance2Geometry[T: ClassTag](instance: STInstance[T]): geometry.Shape = {
    instance match {
      case p: Point[T] => geometry.Point(p.coord, p.timeStamp).setID(p.id)
      case traj: Trajectory[(Double, Double), T] => {
        val points = traj.coord.map(x =>
          geometry.Point(Array(x._1._1, x._1._2), x._2))
        geometry.Trajectory(traj.id,
          traj.coord.head._2,
          points,
          traj.property.getOrElse(Map()).mapValues(_.toString)).mbr
      }
    }
  }

}

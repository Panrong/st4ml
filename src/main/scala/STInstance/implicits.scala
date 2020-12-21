package STInstance

import scala.reflect.ClassTag

object implicits {
  implicit def trajectory2Points[T: ClassTag](traj: Trajectory[(Double, Double), T]): Array[Point[Long]] = {
    implicit def tuple2Array(a:(Double, Double)): Array[Double] = Array(a._1, a._2)
    traj.coord.zipWithIndex
      .map(x => Point(id = x._2, coord = x._1._1, timeStamp = x._1._2, property = Some(Map(("trajID", traj.id)))))
  }
}

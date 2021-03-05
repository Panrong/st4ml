package geometry

import scala.reflect.ClassTag

case class SubSpatialMap[T: ClassTag](roadID: String, attributes: T) {

}

package STInstance

/**
 *
 * @param id       : point ID
 * @param coord    : Array of coordinates
 * @param property : any extra properties
 * @tparam T : parameter type for property
 */
case class Point[T](override val id: Long,
                    coord: Array[Double],
                    timeStamp: Long,
                    override var property: Option[Map[String, T]]) extends STInstance[T] {
  override def toString: String = s"Point $id:\nCoord: ${coord.deep}  timeStamp: $timeStamp\nProperties: ${property.get}"}

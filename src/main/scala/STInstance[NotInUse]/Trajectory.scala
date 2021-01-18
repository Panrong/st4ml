package STInstance

/**
 *
 * @param id       : trajectory ID
 * @param coord    : Array of (location, time), where U can be "String" for trajectory on road map or "Tuple3" for GPS points
 * @param property : any extra properties
 * @tparam U : parameter type for location
 * @tparam T : parameter type for property

 */
case class Trajectory[U, T](override val id: Long,
                            coord: Array[(U, Long)],
                            override var property: Option[Map[String, T]])
  extends STInstance[T] {
  assert(coord.length > 1, "The number of points in a trajectory has to be at least 2.")
  override def toString: String =
    s"Trajectory $id:\nCoord: ${coord.deep}\nProperties: ${property}"

}
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
                            override var property: Option[Map[String, T]]) extends STInstance[T] {

}

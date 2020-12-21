package STInstance

/**
 *
 * @param id       : ID of the sub-spatial map
 * @param vertices : vertices of (name, time-series-like list)
 * @param property : any extra properties (may include the adjacency matrix)
 * @tparam U : parameter type for vertex values
 * @tparam T : parameter type for property
 */
case class Raster[U, T](
                         override val id: Long,
                         vertices: Map[String, List[(Long, U)]],
                         override var property: Option[Map[String, T]]
                       ) extends STInstance[T] {
}


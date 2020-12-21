package STInstance

/**
 *
 * @tparam T : parameter type for property
 */
abstract class STInstance[T] {
  val id: Long
  var property: Option[Map[String, T]]
}

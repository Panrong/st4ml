package STInstance

/**
 *
 * @param id       : time series ID
 * @param series   : List of (time, value)
 * @param property : any extra properties
 * @tparam U : parameter type for time series values
 * @tparam T : parameter type for property
 *
 */
case class TimeSeries[U, T](
                             override val id: Long,
                             series: List[(Long, U)],
                             override var property: Option[Map[String, T]])
  extends STInstance[T] {

}

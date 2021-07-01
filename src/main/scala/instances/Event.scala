package instances

case class Event[S <: Geometry, D](
    override val spatial: S,
    override val temporal: Duration,
    override val data: D)
  extends Instance[S, Duration, D] {

}

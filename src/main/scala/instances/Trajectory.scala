package instances

case class Trajectory[D](
  override val spatial: Array[Point],
  override val temporal: Array[Duration],
  override val data: D)
  extends Instance[Array[Point], Array[Duration], D] {

}

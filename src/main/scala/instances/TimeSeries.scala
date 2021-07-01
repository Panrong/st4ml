package instances

case class TimeSeries[D](
  override val spatial: Array[Extent],
  override val temporal: Array[Duration],
  override val data: D)
  extends Instance[Array[Extent], Array[Duration], D] {

}
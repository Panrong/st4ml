package instances

case class SpatialMap[D](
  override val spatial: Array[Polygon],
  override val temporal: Array[Duration],
  override val data: D)
  extends Instance[Array[Polygon], Array[Duration], D] {

}
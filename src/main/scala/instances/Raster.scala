package instances

case class Raster[S <: Geometry, D](
  override val spatial: Array[S],
  override val temporal: Array[Duration],
  override val data: D)
  extends Instance[Array[S], Array[Duration], D] {

}
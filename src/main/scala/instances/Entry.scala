package instances


case class Entry[S <: Geometry, V](
  spatial: S,
  temporal: Duration,
  value: V) {

  def extent: Extent = Extent(spatial.getEnvelopeInternal)
  def duration: Duration = temporal
  def spatialCenter: Point = extent.center
  def temporalCenter: Long = temporal.center
  def spatialCentroid: Point = spatial.getCentroid
}






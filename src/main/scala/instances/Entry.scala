package instances


case class Entry[S <: Geometry, V] (
  spatial: S,
  temporal: Duration,
  value: V) {

  def extent: Extent = spatial.getEnvelopeInternal
  def duration: Duration = temporal
  def spatialCenter: Point = extent.center
  def temporalCenter: Long = temporal.center
  def spatialCentroid: Point = spatial.getCentroid

}

object Entry {
  def apply[S <: Geometry, V](entry: (S, Duration, V)): Entry[S, V] =
    new Entry(entry._1, entry._2, entry._3)

  def apply[S <: Geometry](entry: (S, Duration)): Entry[S, None.type] =
    new Entry(entry._1, entry._2, None)
}






package instances

object Utils {

  def getExtentFromGeometryArray[T <: Geometry](geomArr: Array[T]): Extent = {
    val envelopes = geomArr.map(_.getEnvelopeInternal)
    val xMin = envelopes.map(_.getMinX).min
    val xMax = envelopes.map(_.getMaxX).max
    val yMin = envelopes.map(_.getMinY).min
    val yMax = envelopes.map(_.getMaxY).max
    Extent(xMin, yMin, xMax, yMax)
  }

  def getExtentFromInstanceArray[T <: Instance[_,_,_]](instanceArr: Array[T]): Extent =
    Extent(instanceArr.map(_.extent))
}

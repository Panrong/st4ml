package instances

object Utils {

  // methods for calculating an overall Extent
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


  // methods for calculating the corresponding index/indices of bins for given query
  def getBinIndex(bins: Array[Duration], queryArr: Array[Long]): Array[Array[Int]] =
    queryArr.map(q =>
      Array(bins.indexWhere(_.intersects(q)))
    )

  def getBinIndices(bins: Array[Duration], queryArr: Array[Duration]): Array[Array[Int]] = {
    queryArr.map(q =>
      bins.zipWithIndex.filter(_._1.intersects(q)).map(_._2)
    )
  }

  def getBinIndices(bins: Array[Duration], queryArr: Array[Long]): Array[Array[Int]] = {
    queryArr.map(q =>
      bins.zipWithIndex.filter(_._1.intersects(q)).map(_._2)
    )
  }

  def getBinIndex[T <: Geometry](bins: Array[Polygon], queryArr: Array[T]): Array[Array[Int]] =
    queryArr.map(q =>
      Array(bins.indexWhere(poly => poly.intersects(q)))
    )

  def getBinIndices[T <: Geometry](bins: Array[Polygon], queryArr: Array[T]): Array[Array[Int]] = {
    queryArr.map(q =>
      bins.zipWithIndex.filter(_._1.intersects(q)).map(_._2)
    )
  }




}

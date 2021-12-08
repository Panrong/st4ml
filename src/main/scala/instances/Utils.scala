package instances

import operators.selection.indexer.RTreeDeprecated

import scala.reflect.ClassTag

object Utils {

  // methods for calculating an overall Polygon
  def getPolygonFromGeometryArray[T <: Geometry](geomArr: Array[T]): Polygon = {
    val nonEmptyGeomArr = geomArr.filter(!_.isEmpty)
    val envelopes = nonEmptyGeomArr.map(_.getEnvelopeInternal)

    if (envelopes.nonEmpty) {
      val xMin = envelopes.map(_.getMinX).min
      val xMax = envelopes.map(_.getMaxX).max
      val yMin = envelopes.map(_.getMinY).min
      val yMax = envelopes.map(_.getMaxY).max
      Extent(xMin, yMin, xMax, yMax).toPolygon
    }
    else Polygon.empty
  }

  def getPolygonFromInstanceArray[T <: Instance[_, _, _]](instanceArr: Array[T]): Polygon = {
    if (instanceArr.nonEmpty) {
      Extent(instanceArr.map(_.extent)).toPolygon
    }
    else Polygon.empty
  }

  // methods for calculating an overall Duration
  def getDuration[T](geomArr: Array[T]): Duration = {
    val durArr = geomArr.map {
      case i: Instance[_, _, _] => i.duration
      case _ => Duration.empty
    }
    Duration(durArr)
  }

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

  def getBinIndicesRTree[S <: Geometry](rTree: RTree[S], queryArr: Array[Duration]): Array[Array[Int]] =
    queryArr.map(query =>
      rTree.range1d((query.start, query.end)).map(_._2.toInt))


  def getBinIndices(bins: Array[Duration], queryArr: Array[Long]): Array[Array[Int]] = {
    queryArr.map(q =>
      bins.zipWithIndex.filter(_._1.intersects(q)).map(_._2)
    )
  }

  def getBinIndex[S <: Geometry, T <: Geometry](bins: Array[S], queryArr: Array[T]): Array[Array[Int]] =
    queryArr.map(q =>
      Array(bins.indexWhere(poly => poly.intersects(q)))
    )

  def getBinIndices[S <: Geometry, T <: Geometry](bins: Array[S], queryArr: Array[T]): Array[Array[Int]] = {
    queryArr.map(q =>
      bins.zipWithIndex.filter(_._1.intersects(q)).map(_._2)
    )
  }

  def getBinIndicesRTreeDeprecated[T <: Geometry](rTree: RTreeDeprecated[geometry.Rectangle], queryArr: Array[T]): Array[Array[Int]] = {
    val qArray = queryArr.map(x => {
      val extent = Extent(x.getEnvelopeInternal)
      geometry.Rectangle(Array(extent.xMin, extent.yMin, extent.xMax, extent.yMax))
    })
    qArray.map(query => rTree.range(query).map(_._2.toInt))
  }

  def getBinIndicesRTree[S <: Geometry, T <: Geometry : ClassTag](RTree: RTree[S], queryArr: Array[T]): Array[Array[Int]] = {
    queryArr.map(query => RTree.range(query).map(_._2.toInt))
  }
}

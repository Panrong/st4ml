package st4ml.instances

import scala.reflect.ClassTag

class SpatialMap[S <: Geometry : ClassTag, V, D](override val entries: Array[Entry[S, V]],
                                                  override val data: D)
  extends Instance[S, V, D] {

  lazy val spatials: Array[S] = entries.map(_.spatial)
  lazy val temporals: Array[Duration] = entries.map(_.temporal)
  lazy val temporal: Duration = Duration(temporals.head.start, temporals.last.end)

  // the rTree is built when 1) explicitly call attachInstanceRTree function or
  // 2) convert event/traj to spatial map with optimization
  var rTree: Option[RTree[S]] = None
  require(validation,
    s"The length of entries for SpatialMap should be at least 1, but got ${entries.length}")

  // as long as no overlapping area, regard as disjoint
  def isSpatialDisjoint: Boolean = {
    if (spatials.length > 1) {
      val pairs = spatials.combinations(2)
      pairs.foreach(polyPair => if (polyPair(0).intersection(polyPair(1)).getArea > 0) return false)
    }
    true
  }

  def isRegular: Boolean = {
    val areaEqual = spatials.map(_.getArea).sum == this.toGeometry.getArea
    val allRectangle = spatials.map(_.isRectangle).forall(_ == true)
    val lengths = spatials.map(r => {
      val e = r.getEnvelopeInternal
      (e.getMaxX - e.getMinX, e.getMaxY - e.getMinY)
    })
    val lengthEqual = lengths.forall(_ == lengths.head)
    areaEqual && isSpatialDisjoint && lengthEqual && allRectangle
  }

  def mapSpatial[T <: Geometry : ClassTag](f: S => T): SpatialMap[T, V, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          f(entry.spatial),
          entry.temporal,
          entry.value)),
      data)

  override def mapTemporal(f: Duration => Duration): SpatialMap[S, V, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          f(entry.temporal),
          entry.value)),
      data)

  override def mapValue[V1](f: V => V1): SpatialMap[S, V1, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          f(entry.value))),
      data)

  def mapValuePlus[V1](f: (V, S, Duration) => V1): SpatialMap[S, V1, D] = {
    val newEntries = entries.map(entry => {
      val spatial = entry.spatial
      val temporal = entry.temporal
      Entry(spatial, temporal, f(entry.value, spatial, temporal))
    })
    SpatialMap(newEntries, data)
  }


  override def mapEntries[V1](
                               f1: S => S,
                               f2: Duration => Duration,
                               f3: V => V1): SpatialMap[S, V1, D] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          f1(entry.spatial),
          f2(entry.temporal),
          f3(entry.value))),
      data)

  override def mapEntries[V1](f: Entry[S, V] => Entry[S, V1]): SpatialMap[S, V1, D] =
    SpatialMap(entries.map(f(_)), data)

  override def mapData[D1](f: D => D1): SpatialMap[S, V, D1] =
    SpatialMap(
      entries.map(entry =>
        Entry(
          entry.spatial,
          entry.temporal,
          entry.value)),
      f(data))

  def mapDataPlus[D1](f: (D, Polygon, Duration) => D1): SpatialMap[S, V, D1] =
    SpatialMap(entries, f(data, extent.toPolygon, temporal))

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

  def getBinIndicesRTree[S <: Geometry, T <: Geometry : ClassTag](RTree: RTree[S], queryArr: Array[T]): Array[Array[Int]] = {
    queryArr.map(query => RTree.range(query).map(_._2.toInt))
  }

  def getBinIndicesRTree[S <: Geometry, T <: Geometry : ClassTag](RTree: RTree[S], queryArr: Array[T], distance: Double): Array[Array[Int]] = {
    queryArr.map(query => RTree.distanceRange(query, distance).map(_._2.toInt))
  }

  def getSpatialIndex[G <: Geometry](geomArr: Array[G]): Array[Array[Int]] = {
    if (geomArr.head.isInstanceOf[Point] && isSpatialDisjoint)
      getBinIndex(spatials, geomArr)
    else
      getBinIndices(spatials, geomArr)
  }

  def getSpatialIndexRTree[G <: Geometry : ClassTag](geomArr: Array[G]): Array[Array[Int]] = {
    getBinIndicesRTree(rTree.get, geomArr)
  }

  def getSpatialIndexRTree[G <: Geometry : ClassTag](geomArr: Array[G], distance: Double): Array[Array[Int]] = {
    getBinIndicesRTree(rTree.get, geomArr, distance)
  }

  def getSpatialIndexToObjRTree[T: ClassTag, G <: Geometry : ClassTag](objArr: Array[T], geomArr: Array[G]): Map[Int, Array[T]] = {
    if (geomArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getSpatialIndexRTree(geomArr)
      objArr.zip(indices)
        .filter(_._2.length > 0)
        .flatMap { case (geom, indexArr) =>
          for {
            idx <- indexArr
          } yield (geom, idx)
        }
        .groupBy(_._2)
        .mapValues(x => x.map(_._1))
    }
  }

  def getSpatialIndexToObj[T: ClassTag, G <: Geometry](objArr: Array[T], geomArr: Array[G]): Map[Int, Array[T]] = {
    if (geomArr.isEmpty) {
      Map.empty[Int, Array[T]]
    } else {
      val indices = getSpatialIndex(geomArr)
      objArr.zip(indices)
        .filter(_._2.length > 0)
        .flatMap { case (geom, indexArr) =>
          for {
            idx <- indexArr
          } yield (geom, idx)
        }
        .groupBy(_._2)
        .mapValues(x => x.map(_._1))
    }
  }

  def createSpatialMap[T: ClassTag](
                                     spatialIndexToObj: Map[Int, Array[T]],
                                   ): SpatialMap[S, Array[T], D] = {
    if (spatialIndexToObj.nonEmpty) {
      val newValues = entries.zipWithIndex.map(entryWithIdx =>
        if (spatialIndexToObj.contains(entryWithIdx._2)) {
          entryWithIdx._1.value.asInstanceOf[Array[T]] ++ spatialIndexToObj(entryWithIdx._2)
        }
        else {
          entryWithIdx._1.value.asInstanceOf[Array[T]]
        }
      )
      val newTemporals = newValues.map(value => Utils.getDuration(value))
      val newEntries = (spatials, newTemporals, newValues).zipped.toArray.map(Entry(_))
      SpatialMap(newEntries, data)
    }
    else {
      this.asInstanceOf[SpatialMap[S, Array[T], D]]
    }
  }

  def attachGeometry[T <: Geometry : ClassTag](geomArr: Array[T])
                                              (implicit ev: Array[T] =:= V): SpatialMap[S, Array[T], D] = {
    val entryIndexToGeom = getSpatialIndexToObj(geomArr, geomArr)
    createSpatialMap(entryIndexToGeom)
  }

  def attachInstance[T <: Instance[_, _, _] : ClassTag, G <: Geometry : ClassTag]
  (instanceArr: Array[T], geomArr: Array[G])(implicit ev: Array[T] =:= V): SpatialMap[S, Array[T], D] = {
    require(instanceArr.length == geomArr.length,
      "the length of two arguments must match")
    val entryIndexToInstance = getSpatialIndexToObj(instanceArr, geomArr)
    createSpatialMap(entryIndexToInstance)
  }

  def attachInstanceRTree[T <: Instance[_, _, _] : ClassTag, G <: Geometry : ClassTag]
  (instanceArr: Array[T], geomArr: Array[G])(implicit ev: Array[T] =:= V): SpatialMap[S, Array[T], D] = {
    // in called in a converter, a broadcast Rtree will pass in and this line will not execute
    if (rTree.isEmpty) rTree = Some(Utils.buildRTree(spatials))
    require(instanceArr.length == geomArr.length,
      "the length of two arguments must match")
    val entryIndexToInstance = getSpatialIndexToObjRTree(instanceArr, geomArr)
    createSpatialMap(entryIndexToInstance)
  }

  def attachInstance[T <: Instance[_, _, _] : ClassTag](instanceArr: Array[T])
                                                       (implicit ev: Array[T] =:= V): SpatialMap[S, Array[T], D] = {
    val geomArr = instanceArr.map(_.toGeometry)
    attachInstance(instanceArr, geomArr)
  }


  // todo: handle different order of the same spatials
  def merge[T: ClassTag](
                          other: SpatialMap[S, Array[T], _]
                        )(implicit ev: Array[T] =:= V): SpatialMap[S, Array[T], None.type] = {
    require(spatials sameElements other.spatials,
      "cannot merge SpatialMap with different spatial structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      x._1.asInstanceOf[Array[T]] ++ x._2.asInstanceOf[Array[T]]
    )
    val newTemporals = entries.map(_.temporal).zip(other.entries.map(_.temporal)).map(x =>
      Duration(Array(x._1, x._2))
    )
    val newEntries = (spatials, newTemporals, newValues).zipped.toArray.map(Entry(_))
    SpatialMap(newEntries, None)
  }

  def merge[T](
                other: SpatialMap[S, T, D],
                valueCombiner: (V, T) => V,
                dataCombiner: (D, D) => D
              ): SpatialMap[S, V, D] = {
    require(spatials sameElements other.spatials,
      "cannot merge SpatialMap with different spatial structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      valueCombiner(x._1, x._2)
    )
    val newTemporals = entries.map(_.temporal).zip(other.entries.map(_.temporal)).map(x =>
      Duration(Array(x._1, x._2))
    )
    val newEntries = (spatials, newTemporals, newValues).zipped.toArray.map(Entry(_))
    val newData = dataCombiner(data, other.data)
    SpatialMap(newEntries, newData)
  }

  def merge[T: ClassTag](
                          other: SpatialMap[S, Array[T], D],
                          dataCombiner: (D, D) => D
                        )(implicit ev: Array[T] =:= V): SpatialMap[S, Array[T], D] = {
    require(spatials sameElements other.spatials,
      "cannot merge SpatialMap with different spatial structure")

    val newValues = entries.map(_.value).zip(other.entries.map(_.value)).map(x =>
      x._1.asInstanceOf[Array[T]] ++ x._2.asInstanceOf[Array[T]]
    )
    val newTemporals = entries.map(_.temporal).zip(other.entries.map(_.temporal)).map(x =>
      Duration(Array(x._1, x._2))
    )
    val newEntries = (spatials, newTemporals, newValues).zipped.toArray.map(Entry(_))
    val newData = dataCombiner(data, other.data)
    SpatialMap(newEntries, newData)
  }

  // sort by xMin then yMin of the spatial of each entry
  def sorted: SpatialMap[S, V, D] = {
    val newEntries = entries.sortBy(x => (x.spatial.getCoordinates.map(_.x).min, x.spatial.getCoordinates.map(_.y).min))
    new SpatialMap[S, V, D](newEntries, data)
  }

  override def toGeometry: Polygon = extent.toPolygon

  override def setData[D1](newData: D1): SpatialMap[S, V, D1] = new SpatialMap(entries, newData)

}

object SpatialMap {
  def empty[S <: Geometry : ClassTag, T: ClassTag](polygonArr: Array[S]): SpatialMap[S, Array[T], None.type] =
    SpatialMap(polygonArr.map(x => Entry(x, Duration.empty, Array.empty[T])))

  def empty[T: ClassTag](extentArr: Array[Extent]): SpatialMap[Polygon, Array[T], None.type] = {
    val polygonArr = extentArr.map(_.toPolygon)
    SpatialMap.empty(polygonArr)
  }

  def apply[S <: Geometry : ClassTag, V, D](entries: Array[Entry[S, V]], data: D): SpatialMap[S, V, D] =
    new SpatialMap(entries, data)

  def apply[S <: Geometry : ClassTag, V](entries: Array[Entry[S, V]]): SpatialMap[S, V, None.type] =
    new SpatialMap(entries, None)
}
package st4ml.instances

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private object Utils {

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

  def getBinIndicesRTree[S <: Geometry, T <: Geometry : ClassTag](RTree: RTree[S], queryArr: Array[T]): Array[Array[Int]] = {
    queryArr.map(query => RTree.range(query).map(_._2.toInt))
  }

  def getBinIndicesRTree[S <: Geometry, T <: Geometry : ClassTag](RTree: RTree[S], queryArr: Array[T], distance: Double): Array[Array[Int]] = {
    queryArr.map(query => RTree.distanceRange(query, distance).map(_._2.toInt))
  }

  implicit class smRDDFuncs[V: ClassTag, S <: Geometry : ClassTag, D: ClassTag](rdd: RDD[SpatialMap[S, V, D]]) {
    def mapValuePlus[V1](f: (V, S, Duration) => V1): RDD[SpatialMap[S, V1, D]] =
      rdd.map(x => x.mapValuePlus(f))

    def mapValue[V1](f: V => V1): RDD[SpatialMap[S, V1, D]] =
      rdd.map(x => x.mapValue(f))

    def collectAndMerge[V1](init: V1, f: (V1, V) => V1): SpatialMap[S, V1, D] = {
      val sms = rdd.collect

      def merge(a: SpatialMap[S, V1, D], b: SpatialMap[S, V, D]): SpatialMap[S, V1, D] = {
        val newEntries = (a.entries zip b.entries).map(x => new Entry(x._1.spatial, x._1.temporal, f(x._1.value, x._2.value)))
        SpatialMap(newEntries, a.data)
      }

      val initSm = sms.head.mapValue(_ => init)
      sms.foldLeft(initSm)(merge)
    }
  }

  implicit class rasterRDDFuncs[V: ClassTag, S <: Geometry : ClassTag, D: ClassTag](rdd: RDD[Raster[S, V, D]]) {
    def mapValuePlus[V1](f: (V, S, Duration) => V1): RDD[Raster[S, V1, D]] =
      rdd.map(x => x.mapValuePlus(f))

    def mapValue[V1](f: V => V1): RDD[Raster[S, V1, D]] =
      rdd.map(x => x.mapValue(f))

    def collectAndMerge[V1](init: V1, f: (V1, V) => V1): Raster[S, V1, D] = {
      val sms = rdd.collect

      def merge(a: Raster[S, V1, D], b: Raster[S, V, D]): Raster[S, V1, D] = {
        val newEntries = (a.entries zip b.entries).map(x => new Entry(x._1.spatial, x._1.temporal, f(x._1.value, x._2.value)))
        Raster(newEntries, a.data)
      }

      val initSm = sms.head.mapValue(_ => init)
      sms.foldLeft(initSm)(merge)
    }
  }

  implicit class tsRDDFuncs[V: ClassTag, S <: Geometry : ClassTag, D: ClassTag](rdd: RDD[TimeSeries[V, D]]) {
    def mapValuePlus[V1](f: (V, Polygon, Duration) => V1): RDD[TimeSeries[V1, D]] =
      rdd.map(x => x.mapValuePlus(f))

    def mapValue[V1](f: V => V1): RDD[TimeSeries[V1, D]] =
      rdd.map(x => x.mapValue(f))

    def collectAndMerge[V1](init: V1, f: (V1, V) => V1): TimeSeries[V1, D] = {
      val sms = rdd.collect

      def merge(a: TimeSeries[V1, D], b: TimeSeries[V, D]): TimeSeries[V1, D] = {
        val newEntries = (a.entries zip b.entries).map(x => new Entry(x._1.spatial, x._1.temporal, f(x._1.value, x._2.value)))
        TimeSeries(newEntries, a.data)
      }

      val initSm = sms.head.mapValue(_ => init)
      sms.foldLeft(initSm)(merge)
    }
  }

  implicit class smRDDFuncs2[V: ClassTag, S <: Geometry : ClassTag, D: ClassTag](rdd: RDD[SpatialMap[S, Array[V], D]]) extends Serializable {
    def mapValuePlus2[V1: ClassTag](f: (V, S, Duration) => V1): RDD[SpatialMap[S, Array[V1], D]] =
      rdd.map(x => x.mapValuePlus2(f))
  }

  implicit class smFuncs[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag](sm: SpatialMap[S, Array[V], D]) extends Serializable {
    def mapValuePlus2[V1: ClassTag](f: (V, S, Duration) => V1): SpatialMap[S, Array[V1], D] = {
      val newEntries = sm.entries.map(entry => {
        val spatial = entry.spatial
        val temporal = entry.temporal
        Entry(spatial, temporal, entry.value.map(x => f(x, spatial, temporal)).toArray)
      })
      SpatialMap(newEntries, sm.data)
    }
  }

  //TODO add func for ts and raster and mapdata
}



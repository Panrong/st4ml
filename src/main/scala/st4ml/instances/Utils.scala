package st4ml.instances

import org.apache.spark.rdd.RDD

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

  // 1d rtree for time series
  def buildRTree(temporals: Array[Duration]): RTree[Polygon, String] = {
    val r = math.sqrt(temporals.length).toInt
    var entries = new Array[(Polygon, String, Int)](0)
    for (i <- temporals.zipWithIndex) {
      val p = Extent(0, 0, 1, 1).toPolygon
      p.setUserData(Array(i._1.start.toDouble, i._1.end.toDouble))
      entries = entries :+ (p.copy.asInstanceOf[Polygon], i._2.toString, i._2)
    }
    RTree[Polygon, String](entries, r, dimension = 3)
  }

  //  2d rtree for spatial map
  def buildRTree[T <: Geometry : ClassTag](geomArr: Array[T]): RTree[T, String] = {
    val r = math.sqrt(geomArr.length).toInt
    val entries = geomArr.zipWithIndex.map(x => (x._1, x._2.toString, x._2))
    RTree[T, String](entries, r)
  }


  def buildRTree[T <: Geometry : ClassTag](geomArr: Array[Event[T, None.type, String]]): RTree[T, String] = {
    val r = math.sqrt(geomArr.length).toInt
    val entries = geomArr.zipWithIndex.map(x => (x._1.entries.head.spatial, x._1.data, x._2))
    RTree[T, String](entries, r)
  }

  // 3d rtree for raster
  def buildRTree[T <: Geometry : ClassTag](geomArr: Array[T],
                                           durArr: Array[Duration]): RTree[T, String] = {
    val r = math.sqrt(geomArr.length).toInt
    var entries = new Array[(T, String, Int)](0)
    for (i <- geomArr.indices) {
      geomArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (geomArr(i).copy.asInstanceOf[T], i.toString, i)
    }
    RTree[T, String](entries, r, dimension = 3)
  }

  def buildRTreeLite[T <: Geometry : ClassTag](geomArr: Array[T],
                                               durArr: Array[Duration]): RTreeLite[T] = {
    val r = math.sqrt(geomArr.length).toInt
    var entries = new Array[(T, String, Int)](0)
    for (i <- geomArr.indices) {
      geomArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (geomArr(i).copy.asInstanceOf[T], i.toString, i)
    }
    RTreeLite[T](entries, r, dimension = 3)
  }

  def buildRTree3d[T <: Instance[_, _, _] : ClassTag](instanceArr: Array[T]): RTree[Polygon, String] = {
    val r = math.sqrt(instanceArr.length).toInt
    var entries = new Array[(Polygon, String, Int)](0)
    val geomArr = instanceArr.map(_.extent.toPolygon)
    val durArr = instanceArr.map(_.duration)
    for (i <- instanceArr.indices) {
      geomArr(i).setUserData(Array(durArr(i).start.toDouble, durArr(i).end.toDouble))
      entries = entries :+ (geomArr(i).copy.asInstanceOf[Polygon], instanceArr(i).data.toString, i)
    }
    RTree[Polygon, String](entries, r, dimension = 3)
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

  implicit class rasterRDDFuncs[V: ClassTag, S <: Geometry : ClassTag, D: ClassTag](rdd: RDD[Raster[S, V, D]]) extends Serializable {
    def mapValuePlus[V1](f: (V, S, Duration) => V1): RDD[Raster[S, V1, D]] =
      rdd.map(x => x.mapValuePlus(f))

    def mapValue[V1](f: V => V1): RDD[Raster[S, V1, D]] =
      rdd.map(x => x.mapValue(f))

    def collectAndMerge[V1](init: V1, f1: (V1, V) => V1, f2: (V1, V1) => V1): Raster[S, V1, D] = {

      def merge1(a: Raster[S, V1, D], b: Raster[S, V, D]): Raster[S, V1, D] = {
        val newEntries = (a.entries zip b.entries).map(x => new Entry(x._1.spatial, x._1.temporal, f1(x._1.value, x._2.value)))
        Raster(newEntries, a.data)
      }

      def merge2(a: Raster[S, V1, D], b: Raster[S, V1, D]): Raster[S, V1, D] = {
        val newEntries = (a.entries zip b.entries).map(x => new Entry(x._1.spatial, x._1.temporal, f2(x._1.value, x._2.value)))
        Raster(newEntries, a.data)
      }

      val initSm = rdd.take(1).head.mapValue(_ => init)
      rdd.aggregate(initSm)(merge1, merge2)

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

  // funcs2 means considering spatial and temporal information of each entry
  implicit class smRDDFuncs2[V: ClassTag, S <: Geometry : ClassTag, D: ClassTag](rdd: RDD[SpatialMap[S, Array[V], D]]) extends Serializable {
    def mapValuePlus2[V1: ClassTag](f: (V, S) => V1): RDD[SpatialMap[S, Array[V1], D]] =
      rdd.map(x => x.mapValuePlus2(f))

    def mapDataPlus2[D1: ClassTag](f: (D, Array[S]) => D1): RDD[SpatialMap[S, Array[V], D1]] =
      rdd.map(x => x.mapDataPlus2(f))
  }

  implicit class smFuncs[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag](sm: SpatialMap[S, Array[V], D]) extends Serializable {
    def mapValuePlus2[V1: ClassTag](f: (V, S) => V1): SpatialMap[S, Array[V1], D] = {
      val newEntries = sm.entries.map(entry => {
        val spatial = entry.spatial
        val temporal = entry.temporal
        Entry(spatial, temporal, entry.value.map(x => f(x, spatial)))
      })
      SpatialMap(newEntries, sm.data)
    }

    def mapDataPlus2[D1: ClassTag](f: (D, Array[S]) => D1): SpatialMap[S, Array[V], D1] = {
      val newData = f(sm.data, sm.spatials)
      SpatialMap(sm.entries, newData)
    }
  }

  implicit class rasterRDDFuncs2[V: ClassTag, S <: Geometry : ClassTag, D: ClassTag](rdd: RDD[Raster[S, Array[V], D]]) extends Serializable {
    def mapValuePlus2[V1: ClassTag](f: (V, S, Duration) => V1): RDD[Raster[S, Array[V1], D]] =
      rdd.map(x => x.mapValuePlus2(f))

    def mapDataPlus2[D1: ClassTag](f: (D, Array[S], Array[Duration]) => D1): RDD[Raster[S, Array[V], D1]] =
      rdd.map(x => x.mapDataPlus2(f))
  }

  implicit class rasterFuncs[S <: Geometry : ClassTag, V: ClassTag, D: ClassTag](raster: Raster[S, Array[V], D]) extends Serializable {
    def mapValuePlus2[V1: ClassTag](f: (V, S, Duration) => V1): Raster[S, Array[V1], D] = {
      val newEntries = raster.entries.map(entry => {
        val spatial = entry.spatial
        val temporal = entry.temporal
        Entry(spatial, temporal, entry.value.map(x => f(x, spatial, temporal)))
      })
      Raster(newEntries, raster.data)
    }

    def mapDataPlus2[D1: ClassTag](f: (D, Array[S], Array[Duration]) => D1): Raster[S, Array[V], D1] = {
      val newData = f(raster.data, raster.spatials, raster.temporals)
      Raster(raster.entries, newData)
    }
  }

  implicit class tsRDDFuncs2[V: ClassTag, D: ClassTag](rdd: RDD[TimeSeries[Array[V], D]]) extends Serializable {
    def mapValuePlus2[V1: ClassTag](f: (V, Duration) => V1): RDD[TimeSeries[Array[V1], D]] =
      rdd.map(x => x.mapValuePlus2(f))

    def mapDataPlus2[D1: ClassTag](f: (D, Array[Duration]) => D1): RDD[TimeSeries[Array[V], D1]] =
      rdd.map(x => x.mapDataPlus2(f))
  }

  implicit class tsFuncs[V: ClassTag, D: ClassTag](ts: TimeSeries[Array[V], D]) extends Serializable {
    def mapValuePlus2[V1: ClassTag](f: (V, Duration) => V1): TimeSeries[Array[V1], D] = {
      val newEntries = ts.entries.map(entry => {
        val spatial = entry.spatial
        val temporal = entry.temporal
        Entry(spatial, temporal, entry.value.map(x => f(x, temporal)))
      })
      TimeSeries(newEntries, ts.data)
    }

    def mapDataPlus2[D1: ClassTag](f: (D, Array[Duration]) => D1): TimeSeries[Array[V], D1] = {
      val newData = f(ts.data, ts.temporals)
      TimeSeries(ts.entries, newData)
    }
  }
}



# Programming Guide


In this page, we introduce the design and use of two core parts in ST4ML: ST instances and operations.
With the well-designed ST instances, programmers can easily manipulate heterogeneous ST data; and with the three types operators
programmers can make ST data ingestible for machine learning applications.

> - The source code of ST4ML functionalities are located at `st4ml/st4ml-core/`, and the package 
name is `st4ml`.
> - Currently, ST4ML relies on Spark environment (i.e., it can only be used under `spark-submit` or `spark-shell` but
> not normal `scala`). We may extend ST support to normal `scala` environment in the future versions.

[ST Instances](#ST-Instances)
- [Geometry](#Geometry)
- [Duration](#Duration)
- [The Five ST Instances](#The-Five-ST-Instances)
  - [Event](#Event)
  - [Trajectory](#Trajectory)
  - [Time Series](#Time-Series)
  - [Spatial Map](#Spatial-Map)
  - [Raster](#Raster)
  
[Operators](#Operators)
  - [Selector](#Selector)
  - [Converter](#Converter)
  - [Extractor](#Extractor)

## ST Instances

We first introduce the ST support based on our ``Geometry`` class and ``Duration`` class.

### Geometry

ST4ML supports three most commonly-used types of 2-d geometries: ``Point``, ``Polygon``, and ``LineString``. These geometries inherit the widely-used [JTS] package, which means that
all JTS functions are supported, while we extend them with other commonly-used functions based on our industrial experience.

[JTS]:  https://github.com/locationtech/jts

We add easier construction methods in scala. E.g., besides the original JTS constructions, a ``Point`` can be created with two ``Double`` or a tuple:

```scala
    import st4ml.instances.Point

    val point1 = Point(1.2, 1.3)
    val point2 = Point((103.682106, 1.346750))
```

> **_NOTE:_**
    In ST4ML's convention, when describing a geographical point, it is denoted as ``Point(longitude, latitude)``, i.e. the longitude is defined first. The order is important when using geometrical functions (e.g. calculating the Great Circle Distance). 
    The same applies to other geometries. 
>  Extra accessing methods for the coordinates are available: `point.x == point.lon`, `point.y == point.lat`.

A ``LineString`` can be created with points or array of tuples:

```scala
    import st4ml.instances.LineString

    val ls1 = LineString(point1, point2)
    val ls2 = LineString(Array((1.2, 1.3), (2.2, 2.3)))
```
Similarly, a ``Polygon`` can be created with points or array of tuples, but the last point has to be the same as the first one (to close the ring). The points are connected sequentially to form a polygon (the same concept as JTS use):

``` scala
    import st4ml.instances.Polygon

    val ls2 = Polygon(Array((1.2, 1.3), (2.2, 2.3), (2.2, 2.5), (1.2, 1.3)))
```

We define a special geometry ``Extent``, which is a rectangle defined by `xMin`, `yMin`, `xMax`, and `yMax`:

```scala
    import st4ml.instances.Extent

    val extent = new Extent(0, 0, 1, 1.5)
```
It is internally used to describe the minimum bounding rectangle (MBR) for a geometry.

Note that the ``Extent`` class does not extend the ``Geometry`` class and it cannot be directly used in ST4ML functions (which require ``[T <: Geometry]``)

However, ``Extent`` has the function ``toPolygon``:

```scala
    val polygon = extent.toPolygon
```
which provides an easy way to construct a rectangular polygon.

#### Built-in Functions

> Import the dependency below before using the following functionalities
>  ```scala
>  import st4ml.instances.GeometryImplicits._
>  ```

```scala
  /** Tests whether this geometry is topologically equal to the input geometry. */
  def ==(geom: Geometry): Boolean

  /** Return the MBR as extent of a geometry */
  def extent: Extent

  /** Returns the euclidean distance between centroids of the two geometries */
  def euclidean(geom: Geometry): Double

  /** Returns the great circle distance between centroids of the two geometries */
  def greatCircle(geom: Geometry): Double

  /** Returns the distance between the nearest points on the input geometries */
  def minDistance(geom: Geometry): Double

  /** Returns the nearest points in the input geometries */
  def nearestPoints(geom: Geometry): Array[Point]
```

### Duration


The ``Duration`` class records temporal information. Examples of constructing a Duration:

```scala
    import st4ml.instances.Duration
    import java.util.TimeZone

    val dur1 = Duration(0L, 12L) // a duration starts at 0 and ends at 12
    val dur2 = Duration(1646832991L) // an instant at timestamp 1646832991
    val dur3 = Duration(Array(0L, 12L)) // the length of the array has to be 2
    val dur4 = Duration("2022-12-19 14:30:00", format = "yyyy-MM-dd HH:mm:ss", timeZone = TimeZone.getDefault) // the formant and timeZone argument has default values as used in the example
    val dur5 = Duration(("2022-12-19 14:30:00", "2022-12-19 15:30:00"), format = "yyyy-MM-dd HH:mm:ss", timeZone = TimeZone.getDefault) // similar as above
    val dur6 = Duration(Array(dur2, dur3)) // combine multiple durations
```

> **_NOTE:_**
    The ``Duration`` class takes ``Long`` type input since in most cases the time is in the unix timestamp format.

#### Built-in Functions
```scala
  /* fields */
  val start: Long
  val end: Long

  /** Test if the length of the duration is zero. If so, return true */
  def isInstant: Boolean 

  /** Test if two Durations are identical */
  def ==(other: Duration): Boolean 
  
  /** Convert the duration into seconds i.e, (end - start) */
  def seconds: Long

  /** Convert the duration into hours */
  def hours: Double

  /** Convert the duration into days */
  def days: Double 

  /** Find the center point of a duration */
  def center: Long 

  /** Check if two Durations intersect, both ends are inclusive */
  
  def intersects(timestampInSecond: Long): Boolean 

  def intersects(dur: Duration): Boolean

  /** Check if one Duration contains the other, both ends are exclusive */
  def contains(timestampInSecond: Long): Boolean 
  
  def contains(dur: Duration): Boolean 

  /** Find the intersection of two Durations.
   *  If the length of intersection > 0, return Some(Duration), else None */
  def intersection(other: Duration): Option[Duration] 

  /** Add shift to the duration */
  def plusSeconds(deltaStart: Long, deltaEnd: Long): Duration 

  def plusSeconds(delta: Long): Duration

```

### The Five ST Instances


#### The Instance abstract class


An ST instance contain two fields: ``entries`` and ``data``.  The ``entries`` field is an array of ``Entry``, where each entry contains
spatial, temporal and an auxiliary value. The spatial information should inherit the ``Geometry`` class, the temporal information is fixed to ``Duration``, while 
the ``value`` and ``data`` information can be of any type specified by the programmer.

It is formally defined as below:

``` scala
abstract class Instance[S <: Geometry, V, D] extends Serializable {
    val entries: Array[Entry[S, V]]
    val data: D
    ...
}

case class Entry[S <: Geometry, V](spatial: S,
                                   temporal: Duration,
                                   value: V) {
   ...
}
```

##### Built-in Functions (for all `Instance` inheritances) :
```scala
  // fields
  val entries: Array[Entry[S, V]]
  val data: D

  // validate if the length of entries is at least one
  def validation: Boolean

  // check the if the length of entries is zero
  def isEmpty: Boolean 

  // find the length of entries
  def entryLength: Int
  
  // change the data field by constructing a new instance
  def setData[D1](data: D1): Instance[S, V, D1]

  // the aggregated extent and duration of the entries 
  lazy val extent: Extent

  lazy val duration: Duration 

  // find the centers of the instance
  def center: (Point, Long) 

  def spatialCenter: Point 

  def temporalCenter: Long 

  // convert the instance to a pure geometry
  def toGeometry: Geometry

  // Predicates: for intersects, both ends are inclusive; for contains, both ends are exclusive
  def intersects(g: Geometry): Boolean 

  def intersects(e: Extent): Boolean 

  def intersects(dur: Duration): Boolean 

  def intersects(g: Geometry, dur: Duration): Boolean 

  def intersects(e: Extent, dur: Duration): Boolean 

  def contains(g: Geometry): Boolean 

  def contains(e: Extent): Boolean

  def contains(dur: Duration): Boolean 

  def contains(g: Geometry, dur: Duration): Boolean 

  def contains(e: Extent, dur: Duration): Boolean 

  // Methods
  def mapTemporal(f: Duration => Duration): Instance[S, V, D]

  def mapValue[V1](f: V => V1): Instance[S, V1, D]

  def mapEntries[V1](f1: S => S,
                     f2: Duration => Duration,
                     f3: V => V1): Instance[S, V1, D]

  def mapEntries[V1](f: Entry[S, V] => Entry[S, V1]): Instance[S, V1, D]

  def mapData[D1](f: D => D1): Instance[S, V, D1]


  // not inheriting, but separately implemented

  def mapSpatial[T <: Geometry : ClassTag](f: S => T): Instance[T, V, D]

  def mapValuePlus[V1](f: (V, S, Duration) => V1): Instance[S, V1, D]

  def mapDataPlus[D1](f: (D, Polygon, Duration) => D1): Instance[S, V, D1]
  }
```

ST4ML provides 5 ST instances, which can be divided into two categories:

- Singular: ``Event`` and ``Trajectory``
- Collective: ``SpatialMap``, ``TimeSeries``, and ``Raster``


We first introduce the definition and construction of each instance and then present the functions supported by them.

#### Event


An event stands for an occurrence of an object and the length of ``entries`` is retrained to be **1**.

```scala
    case class Event[S <: Geometry, V, D](entries: Array[Entry[S, V]],
                                          data: D) extends Instance[S, V, D] {

        override def validation: Boolean = entries.length == 1
        require(validation, s"The length of entries for Event should be 1, but got ${entries.length}")

        ...
        }
```

An alternative construction method is provided to allow programmer create an event with one ``spatial`` and one ``temporal`` value (the ``value`` and ``data`` are set to ``None`` by default):

``` scala
    def apply[S <: Geometry, V, D](entry: Entry[S, V], data: D): Event[S, V, D] = ...
    def apply[S <: Geometry, V, D](s: S, t: Duration, v: V = None, d: D = None): Event[S, V, D] = ...
```

  
#### Trajectory


A trajectory consists of a series of ordered ST points (length > 1) and is defined as follows:

``` scala
    class Trajectory[V, D](
        override val entries: Array[Entry[Point, V]],
        override val data: D) extends Instance[Point, V, D] {

        require(validation,
            s"The length of entries for Trajectory should be at least 2, but got ${entries.length}")

        override def validation: Boolean = entries.length > 1

        ...
        }
```
We provide alternative construction methods:

``` scala
    def apply[V, D](pointArr: Array[Point],
                    durationArr: Array[Duration],
                    valueArr: Array[V], 
                    d2: D): Trajectory[V, D] = ...

    def apply[V, D](arr: Array[(Point, Duration, V)], d: D): Trajectory[V, D] = ...

    /** if value and data fields are empty, they can be omitted */
    /** the data field can be later set with .setData() function */
    def apply(arr: Array[(Point, Duration)]): Trajectory[None.type, None.type] = ...
    
    def apply(pointArr: Array[Point],
              durationArr: Array[Duration]): Trajectory[None.type, None.type] = ...
```

Additional methods:
```scala
  // sliding functions
  def entrySliding(n: Int): Iterator[Array[Entry[Point, V]]] = ...

  def spatialSliding(n: Int): Iterator[Array[Point]] = ...

  def temporalSliding(n: Int): Iterator[Array[Duration]] = ...

 // finding consecutive values along spatial / temporal intervals
  def consecutiveSpatialDistance(metric: String = "euclidean"): Array[Double] = ...

  def consecutiveSpatialDistance(metric: (Point, Point) => Double): Array[Double] = ...

  def consecutiveTemporalDistance(metric: String): Array[Long] = ...

  def consecutiveTemporalDistance(metric: (Duration, Duration) => Long): Array[Long] = ...

  def mapConsecutive(f: (Array[Double], Array[Long]) => Array[Double],
                      spatialMetric: String = "euclidean",
                      temporalMetric: String = "start"
                    ): Array[Double] = ...

  def mapConsecutive(f: (Array[Double], Array[Long]) => Array[Double],
                      spatialMetric: (Point, Point) => Double,
                      temporalMetric: (Duration, Duration) => Long
                    ): Array[Double] = ...
 // reverse 
 def reverse: Trajectory[V, D] = ...
```

#### SpatialMap


A spatial map is a collection of geometries (we call it a cell), each of which contains information (the ``value`` field) of the same type.
The value field can be of any type, even ``Event`` or ``Array[Trajectory]``.
The duration of each cell is not significant. 
Some typical spatial maps include road network (with LineString-typed cells), and grids (with equal-sized, non-overlapping polygon-typed cells).
It is formally defined as:

``` scala
    class SpatialMap[S <: Geometry : ClassTag, V, D](override val entries: Array[Entry[S, V]],
                                                  override val data: D)
           extends Instance[S, V, D] {
           ...}
```
Each polygon corresponds to one ``entry`` in the ``SpatialMap`` class. The polygons can be of different shapes and sizes, and can also overlap.

ST4ML provides two methods to construct an empty spatial map (with defined cells and empty values of each cell)

``` scala
    def empty[S <: Geometry : ClassTag, T: ClassTag](polygonArr: Array[S]): SpatialMap[S, Array[T], None.type] = ...
        
    def empty[T: ClassTag](extentArr: Array[Extent]): SpatialMap[Polygon, Array[T], None.type] = ...
```
Additional methods:
```scala
  // merge two spatial maps
  def merge[T: ClassTag](other: SpatialMap[S, Array[T], _]
                        )(implicit ev: Array[T] =:= V): SpatialMap[S, Array[T], None.type] = ...

  def merge[T](other: SpatialMap[S, T, D],
                valueCombiner: (V, T) => V,
                dataCombiner: (D, D) => D): SpatialMap[S, V, D] = ...

  def merge[T: ClassTag](other: SpatialMap[S, Array[T], D],
                          dataCombiner: (D, D) => D)(implicit ev: Array[T] =:= V): SpatialMap[S, Array[T], D] = ...

  // sort by xMin then yMin of the spatial of each entry
  def sorted: SpatialMap[S, V, D] = ...
```

#### TimeSeries


A time series is a collection of durations, each of which contains information (the ``value`` field) of the same type.
The value field can be of any type, even ``Event`` or ``Array[Trajectory]``.
The shape of each cell is not significant, and is restricted to be a ``Polygon``. It is lazily calculated as the MBR of all elements of the value field if applicable. 
The most common time series are with non-overlapping continuous fixed-length durations, e.g., [12:00-13:00, 13:00-14:00, ...]. Practically, programmers are free to create time series with flexible durations (overlapping, of different length).
It is formally defined as:

``` scala
    class TimeSeries[V, D](
                        override val entries: Array[Entry[Polygon, V]],
                        override val data: D)
        extends Instance[Polygon, V, D] {
        ...}
```

ST4ML provides a method to construct an empty time series (with defined cells and empty values of each cell)

``` scala
    def empty[T: ClassTag](durArr: Array[Duration]): TimeSeries[Array[T], None.type] = {...}
```

Additional methods:
```scala
  // merge two time series
  def merge[T: ClassTag](other: TimeSeries[Array[T], _])(implicit ev: Array[T] =:= V): TimeSeries[Array[T], None.type] = ...

  def merge[T](other: TimeSeries[T, D],
                valueCombiner: (V, T) => V,
                dataCombiner: (D, D) => D): TimeSeries[V, D] = ...

  def merge[T: ClassTag](other: TimeSeries[Array[T], D],
                          dataCombiner: (D, D) => D)(implicit ev: Array[T] =:= V): TimeSeries[Array[T], D] = ...

  def split(at: Long): (TimeSeries[V, D], TimeSeries[V, D]) = ...

  // manipulation
  def select(targetDur: Array[Duration]): TimeSeries[V, D] = ...

  def append(other: TimeSeries[V, _]): TimeSeries[V, None.type] = ...

  def append(other: TimeSeries[V, D], dataCombiner: (D, D) => D): TimeSeries[V, D] = ...

  // sort by tMin of the temporal of each entry
  def sorted: TimeSeries[V, D] = ...
```
#### Raster

A raster is a collection of 3-d cubes in the ST space. Each cell is defined with a 2-d shape with a duration. Raster instance is useful when both the spatial and temporal information of the cell is required for calculation.
Similarly to spatial map, the cell can be of any shape. It is formally defined as:

``` scala
    class Raster[S <: Geometry : ClassTag, V, D](override val entries: Array[Entry[S, V]],
                                             override val data: D)
        extends Instance[S, V, D] {
        ...}
```

Three ways to create an empty raster are described below:

``` scala
    def empty[T: ClassTag](entryArr: Array[Entry[Polygon, _]]): Raster[Polygon, Array[T], None.type] = {...}

    def empty[T: ClassTag](extentArr: Array[Extent], durArr: Array[Duration]): Raster[Polygon, Array[T], None.type] = {...}

    def empty[T: ClassTag](polygonArr: Array[Polygon], durArr: Array[Duration]): Raster[Polygon, Array[T], None.type] = {...}
```
Additional methods:

```scala
  def merge[T: ClassTag](other: Raster[S, Array[T], _])(implicit ev: Array[T] =:= V): Raster[S, Array[T], None.type] = ...

  def merge[T: ClassTag](other: Raster[S, V, D],
                          valueCombiner: (V, V) => V,
                          dataCombiner: (D, D) => D): Raster[S, V, D] = ...

  def merge[T: ClassTag](other: Raster[S, Array[T], D],
                          dataCombiner: (D, D) => D
                        )(implicit ev: Array[T] =:= V): Raster[S, Array[T], D] = ...

  // sort by tMin then xMin then yMin of the spatial of each entry
  def sorted: Raster[S, V, D] = ...
```

> **_NOTE:_** 
    The above methods are not exhaustive. Some internally used functions are also exposed to programmers and might be useful in their applications. 

## Operators


### Selector


The selector is used to load *targeted* data from persistent storage to the memory pool and represent them as RDD.
The selection step is realized in two lines of scala code: one to initialize the selector and the other to build partial DAG.

#### Initialization

To define a `Selector`, the spatial and temporal ranges of interest (`sQuery` and `tQuery`), the preferred `parallelism`, and the ST `Instance` type (currently `Event` and `Trjactory` are supported) has to be identified. 
```scala
    class Selector[I <: Instance[_, _, _] : ClassTag](var sQuery: Polygon = Extent(-180, -90, 180, 90).toPolygon,
                                                      var tQuery: Duration = Duration(Long.MinValue, Long.MaxValue),
                                                      var parallelism: Int = 1)
```

Two alternative initialization approaches are provided:

``` scala
    def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                                 tQuery: Duration,
                                                 numPartitions: Int): Selector[I] = {...}

    def apply[I <: Instance[_, _, _] : ClassTag](sQuery: Polygon,
                                                 tQuery: Duration,
                                                 partitioner: STPartitioner): Selector[I] = {...}
```

#### Selection execution

- Selecting Data with ST4ML standard

The ``Selector`` provides concise commands over ``Event`` or ``Trajectory`` data that meet [ST4ML standard](https://github.com/Panrong/st4ml/blob/instance/docs/data_standard.md).

Programmers may convert the source data to the standard with his preferred methods.

``` scala
    def selectEvent(dataDir: String,
                    metaDataDir: String = "None",
                    index: Boolean = false,
                    partition: Boolean = true): RDD[Event[Geometry, Option[String], String]] = {...}

    def selectTraj(dataDir: String, 
                   metaDataDir: String = "None", 
                   index: Boolean = false,
                   partition: Boolean = true): RDD[Trajectory[Option[String], String]] = {...}
```

Here the ``metaDataDir`` refers to the optimized data loading, which requires a [pre-generated metadata file](#On\-disk-metadata-indexing).

If ``index`` is set true, a per-partition R-tree index is built to facilitate data selection (should be useful when the data size is large and the selectivity is small, otherwise the R-tree generation time may dominate and the disposable index is not worthwhile).

Similarly, the ``partition`` flag can be toggled by the programmer. If the application is simple, she may disable the data repartitioning.


- Selection over RDD

If the RDD is already formulated, the `Selector` could also select data from it as follows:

```scala
    def selectRDD(rdd: RDD[I]): RDD[I] = {...}
```

- Selection over CSV

ST4ML supports selection over `CSV` files with some predefined fields. For details please refer to [toy datasets](https://github.com/Panrong/st4ml/blob/instance/docs/README.md#Toy-Datasets). 

```scala
    def selectEventCSV(dataDir: String): RDD[EventDefault] = {...}
```
```scala
    def selectTrajCSV(dataDir: String): RDD[TrajDefault] = {...}
```

For `EventDefault` and `TrajDefault`, please refer to  [ST4ML standard](https://github.com/Panrong/st4ml/blob/instance/docs/data_standard.md).
#### Example
The following example illustrates how to select trajectories that fall in range ``(0, 0, 10, 10), Duration(0, 100)`` from ``fileName`` and construct a ``trajRDD``:

``` scala
    val spatial = Extent(0, 0, 10, 10).toPolygon
    val temporal = Duration(0, 100)
    val numParititions = 2
    val fileName = "Path/To/Data"

    val selector = Selector[TRAJ](spatial, temporal, numPartitions)
    val trajRDD = selector.selectTraj(fileName)
```

The ``Selector`` returns an RDD of ``Event`` or ``Trajectory``, where each entry has String-typed ``value`` and ``data`` fields according to the ST4ML data standard. Programmers may apply data transformation on the two fields making use of the instance functions.


#### On-disk metadata indexing

- Metadata generation

The metadata file can be generated and stored as follows:
```scala
    // The (event or traj) RDD has to be loaded into memory before hand 
    /** partition ST Instance RDD and persist on disk */
    val partitioner = new TSTRPartitioner(tNumPartitions, sNumPartitions, Some(0.0001)) // identify the granularity of temporal and spatial dimension, as well as the sampling rate 
    val (partitionedRDDWithPId, pInfo) = eventRDD.stPartitionWithInfo(partitioner) // operate on the targeted RDD
    val EventDsWithPid = partitionedRDDWithPId.toDs() 
    EventDsWithPid.show(2, truncate = false) // for checking only
    pInfo.toDisk(metadataDir) // write the metadata
    partitionedRDDWithPId.toDisk(resDir) // write the partitioned data corresponding to the metadata file
```

If the generated metadata `JSON` file is in HDFS, it has to be copied to the master machine. 



### Converter

ST4ML pre-builds the following conversions among the ST instances. The converters are packaged in ``st4ml.operators.converter``.

#### Event to Trajectory


The ``Event2TrajConverter`` groups events according to their ``data`` fields (which may represent an Id or name) to form trajectories. The converter groups the events and order them according to the timestamp.

The shape of the events are required to be ``Point``.

Usage:

```scala
    type V = None.type
    type D = String
    val converter = new Event2TrajConverter
    // to convert an RDD of Event[Point, None.type, String]
    val trajRDD = converter.convert[V, D](eventRDD) 
    // return an RDD of Trajectory[None.type, String], the data field is the same as the events.
```

#### Trajectory to Event


The ``Traj2EventConverter`` takes the sojourn points inside each trajectory as point-shaped individual events.

Usage:

```scala
    type V = None.type
    type D = String
    val converter = new Traj2EventConverter
    // to convert an RDD of Trajectory[None.type, String]
    val trajRDD = converter.convert[V, D](trajRDD) 
    // return an RDD of Event[Point, None.type, String], the data field of each event is the same as the trajectory.
```

#### Event and Trajectory to Spatial Map


The ``Event2SmConverter`` and ``Traj2SmConverter`` groups events and trajectories according to their spatial locations.
Since they behave similarly, we explain them together.

When doing allocation, we check intersection, If a shape (e.g., linestring for trajectory) intersects a cell, it is allocated to the cell.
This incurs duplication of events/trajectories but ensures correctness for various applications.

Instantiation:

``` scala
    class Event2SpatialMapConverter(sArray: Array[Polygon],
                                override val optimization: String = "rtree"
                               ) extends Converter {
                               ...}

    class Traj2SpatialMapConverter(sArray: Array[Polygon],
                                override val optimization: String = "rtree"
                               ) extends Converter {
                               ...}                          
```
To instantiate a ``xx2SmConverter``, the programmer needs to identify the structure of the spatial map,
which is represented by an array of ``Polygon``. An optimization method can also be identified, which can be selected from ``"none"``, ``"rtree"`` and ``"regular"``.
In general, the R-tree based conversion is faster than brute-force iteration ("none"). If ``"regular"`` is chosen, a regularity check is conducted before conversion.

A spatial map is regular only if all its cells have the same shape and size, while they densely tile the whole space (i.e., the MBR of the cells equals the sum of areas of the cells). In this case, no calculation is needed
and the conversion can be finished in the shortest time.

During these types of conversions, the data locality is not changed: only map-sided transformations are implemented.

The resulting spatial map has ``spatial`` type of ``Polygon``, ``value`` type of ``Array[Event[S, V, D]]``, which is the same as the input events, and ``data`` field of ``None.type``.


Usage example:

``` scala
    // sArray is an array of Polygons
    val converter = new Event2SmConverter(sArray, "regular")
    val tsRDD = converter.convert(eventRDD)
    // return an RDD of RDD[SpatialMap[Polygon, Array[Event[S, V, D]], None.type]]
```

ST4ML provides two extension points, which allow programmers to perform customized conversion.

``` scala
    // the two extensions can also be used individually
    val tsRDD = converter.convert(eventRDD, preMap, agg)
```
The ``preMap``  function performs transformation on the events (trajectories) before conversion.
E.g., ``f: Event[S1, V1, D1] => Event[S2, V2, D2]``

The ``agg`` functions performs in-cell aggregation (still in a distributed way, no data shuffling across machines happen).
E.g., ``f: Array[Event[S2, V2, D2]] => T``


#### Event and Trajectory to Time Series


These two conversions are similar to event and trajectory to spatial map conversions.

It is recommended to read :ref:`Event and Trajectory to Spatial Map` first before applying a converter from this category.

The only difference is that for instantiation, an array of durations should be identified.
The optimizations, extensions etc. also apply here.

Usage example:

```scala
    // tArray is an array of Durations
    val converter = new Event2TsConverter(tArray, "regular")
    val tsRDD = converter.convert(eventRDD)
    // return an RDD of RDD[TimeSeries[Polygon, Array[Event[S, V, D]], None.type]]
```
#### Event and Trajectory to Raster


These two conversions are similar to event and trajectory to spatial map conversions.

It is recommended to read :ref:`Event and Trajectory to Spatial Map` first before applying a converter from this category.

The only difference is that for instantiation, two arrays should be identified. The first array ``Array[Polygon]`` are the spatial information of the cells
and the second ``Array[Duration]`` are the durations. Note that the elements of the two arrays should 1-to-1 matched.

The optimizations, extensions etc. also apply here.

Usage example:

```scala
    // sArray is an array of polygons, and tArray is an array of Durations
    val converter = new Event2RasterConverter(sArray, tArray, "regular")
    val tsRDD = converter.convert(eventRDD)
    // return an RDD of RDD[Raster[Polygon, Array[Event[S, V, D]], None.type]]
```

#### Trajectory to Trajectory and Event to Event

These conversion implemented [_Map Matching with Hidden Markov Model_](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/12/map-matching-ACM-GIS-camera-ready.pdf) to map a trajectory or event to a road network (for events, the neareast segment will simply return).

Usage example:
```scala
// To initialize the converter, a roadGrid representing the road network has to be defined. 
// The rest parameters are for the HMM algorithm, where the default values are from the original paper.

class Traj2TrajConverter(roadGrid: RoadGrid,
                         candidateThresh: Double = 50,
                         sigmaZ: Double = 4.07,
                         beta: Double = 20){...}

// There are two conversion options:
// Conversion without interpolation will map existing points to the road network:
def convert[V: ClassTag, D: ClassTag](input: RDD[Trajectory[V, D]]): RDD[Trajectory[String, String]] = {...}

// Conversion with interpolation: the map matching results in a linestring made of connected road segments.
// For a road segment, if no existing sojourn point maps to it (meaning that it is generated by shortest path connection),
// the endpoint of it will be added to the trajectory to make it complete.
// The inferTime flag can be set true to infer the timestamp of the added point. 
// The inference is based on the assumption that the object moves with constant speed.
def convertWithInterpolation[V: ClassTag, D: ClassTag](input: RDD[Trajectory[V, D]],
                                                       inferTime: Boolean = false): RDD[Trajectory[String, String]] = {...}
```
```scala
// The Event2EventConverter takes an array of linestrings as the road network, and a distance threshold (in meter)
class Event2EventConverter[V: ClassTag, D: ClassTag](map: Array[LineString],
                                                     dist: Double = 500) extends Converter {...}

// During execution, each event is mapped to its nearest road segment. 
// The conversion takes a 'discard' option, which controls whether to discard unmatched events of not. 
def convert(input: RDD[I], discard: Boolean = false): RDD[O] = {
```

#### Raster to Spatial Map and Time Series

Sometimes we have a fine-grained ST-partitioned raster and want to regroup it along the spatial or temporal dimension.

Basically, what these converters do is to regroup the raster cells and merge the contents inside. When converting to spatial maps,
an array of polygons has to be provided and when converting to time series, an array of durations have to be provided.

Usage example:

``` scala
  // the types have to be identified: S, V1, D of the raster as well as the V2 for the resulting spatial map
  val converter = new Raster2SmConverter[Polygon, Int, None.type, Int](map)

  // the function for converting Array[V1] to V2
  def f(x: Array[Int]): Int = x.sum
  val cRDD = converter.convert(rasterRDD, f)
```

### Extractor

#### Built-in extractors
The usage illustration of built-in extractors is as follows:

```scala
  /** Find abnormal events defined on occurring at specific ST locations, return filtered events RDD */
  object EventAnomalyExtractor{
  // explicitly define ST ranges, E is the input event type
  def apply[E, G <: Geometry: ClassTag](sRanges: Array[G], tRanges:Array[Duration]): Extractor[E] = {...}
  // define periodical durations, e.g., "23,4,daily"
  def apply[G <: Geometry, ClassTag](sRanges:Array[G], tRanges: String): Extractor[E] = {...}}
    
  /** Find companion relationships among events (occur within ST proximity), return an RDD consists of companion pairs */
  object EventCompanionExtractor{
    // define the max distance as threshold, unit in meter and second if "useGreatCircle"
    def apply[E](sThreshold: Double, tThreshold: Int, useGreatCircle: Boolean = true): Extractor[E] = {...}}

  /** Cluster events using DBSCAN , return a cluster RDD */
  object EventClusterExtractor{
    // define the number of clusters, epsilon, minimal points, and distance metric
    def apply[E](n: Int, eps: Double, minPoints: Int, metric: String = "euclidean"): Extractor[E] = {...}}
    
  /** Extract speed for trajectories */
  object TrajSpeedExtractor{
    /* define the distance metric, and the calculation mode ("avg" for using consecutive points to calculate distance and then divide by total time, "odAvg" for using OD distance dividing by total time, "interval" by resulting interval speed of every consecutive points"), return an RDD where the value fields of the trajectories are replaced by the speed */
    def apply[T](metric: String = "greatCircle", mode = "avg"): Extractor[T] = {...}}
   
  /** Find the origin-destination of trajectories */
  object TrajOdExtractor{
     /* if external POI table is provided, map the OD points to the nearest POI, otherwise return the raw Points; return an RDD[(T,(Point, Point))] */
     def apply[T, G <: Geometry](table: Option[Array[(G, String)]] = None): Extractor[T] = {...}}
     
  /** Find the stay points inside trajectories */
  object TrajStayPointExtractor{
    /* define the max distance as threshold, unit in meter and second if "useGreatCircle", return an RDD[(T, Array[Point])] */
    def apply[T](sThreshold: Double, tThreshold: Int, useGreatCircle: Boolean = true): Extractor[T] = {...}}
    
  /** Find turnings of trajectories */
  object TrajTurningExtractor{
    /* use road network, input a defined structure, return an RDD where the value field of each trajectory sojourn point records the turning: 0 for left and 1 for right */
    def apply[T](rn: RoadGraph): Extractor[T] = {...}
      /* use raw gps points, the turning angle should be bigger than threshold in degree, return an RDD where the value field of each trajectory sojourn point records the turning: 0 for left and 1 for right */
    def apply[T](threshold: Double): Extractor[T] = {...}}    
    
  /** Extract the flow count of a time series */
  object TsFlowExtractor{
   /** require the value fields to be type of Array[X], return an RDD[TimeSeries[Int, _]] */
   def apply[T]: Extractor[T] = {...}}

  /** Extract the average speed of different time slots inside a time series */
  object TsSpeedExtractor{
   /** require the value fields to be type of Array[Trajetory], the mode can be "intesect" to consider all trajectories that intersects the slot, or "contain" to only consider trajectories fully inside a slot, or "trim" to only retain the subtrajectory inside a slot */
   return an RDD[TimeSeries[Double, _]] */
   def apply[T](mode: String = "intersect"): Extractor[T] = {...}}
   
  /** For each time slot, find the window frequency (i.e., count) */
  object TsSpeedExtractor{
   /** the window is defined as (length, overlap) */
   return an RDD[TimeSeries[Double, _]] */
   def apply[T](window: (Long, Long)): Extractor[T] = {...}}

  /** Extract the flow count of a spatial map */
  object SmFlowExtractor{
   /** require the value fields to be type of Array[X], return an RDD[SpatialMap[Int, _]] */
   def apply[S]: Extractor[S] = {...}}
   
  /** Extract the average speed of different cells inside a spatial map */
  object SmSpeedExtractor{
   /** require the value fields to be type of Array[Trajetory], the mode can be "intesect" to consider all trajectories that intersects the cell, or "contain" to only consider trajectories fully inside a cell, or "trim" to only retain the subtrajectory inside a cell */
   return an RDD[SpatialMap[Double, _]] */
   def apply[S](mode: String = "intersect"): Extractor[S] = {...}}
   
  /** Extract the transition among different cells inside a spatial map */
  object SmTransitionExtractor{
   /** require the value fields to be type of Array[Trajetory], return an RDD of spatial map which has value fields of (Array[(Int, Int)], Int, Array[(Int, Int)]), which represents the in, stay and out count */
   def apply[S]: Extractor[S] = {...}}

  /** Extract the flow count of a raster */
  object RasterFlowExtractor{
   /** require the value fields to be type of Array[X], return an RDD[Raster[Int, _]] */
   def apply[R]: Extractor[R] = {...}}

  /** Extract the average speed of different cells inside a raster */
  object RasterSpeedExtractor{
   /** require the value fields to be type of Array[Trajetory], the mode can be "intesect" to consider all trajectories that intersects the cell, or "contain" to only consider trajectories fully inside a cell, or "trim" to only retain the subtrajectory inside a cell */
   return an RDD[Raster[Double, _]] */
   def apply[R](mode: String = "intersect"): Extractor[R] = {...}}

  /** Extract the transition among different cells inside a raster */
  object RasterTransitionExtractor{
   /** require the value fields to be type of Array[Trajetory], return an RDD of raster which has value fields of (Array[(Int, Int)], Int, Array[(Int, Int)]), which represents the in, stay and out count */
   def apply[R]: Extractor[R] = {...}}
```

#### RDD APIs

ST4ML provides the following APIs over RDDs for programmers to write customized extractors.
> **_NOTE:_** To use the following functions, please do `import st4ml.instances.Utils._`.

For RDDs with types `RDD[SpatialMap[S, V, D]]` or `RDD[TimeSeries[S, V, D]]` or `RDD[TimeSeries[S, V, D]]`, The following functions can be called (we take spatial map as an example):

```scala
    // map the value field without considering the ST information of each cell
    def mapValue[V1](f: V => V1): RDD[SpatialMap[S, V1, D]]

    // the spatial and temporal information will be replaced by the values of each cell
    def mapValuePlus[V1](f: (V, S, Duration) => V1): RDD[SpatialMap[S, V1, D]]

    def collectAndMerge[V1](init: V1, f: (V1, V) => V1): SpatialMap[S, V1, D]

```
# Data Standard in ST4ML

Programmers may transform their data to ST4ML's **on-disk standard** with their preferred methods
 and utilize ST4ML's data ingestion functions for in-memory processing. Otherwise, they may directly 
write Spark code to transform the data to ST4ML's **in-memory standard**.

## On-Disk Parquet Data Standard


### Event

The on-disk events should be stored as ``Parquet`` files and contain the following fields:

``` scala
    case class E(shape: String, 
                 timeStamp: Array[Long], 
                 v: Option[String], 
                 d: String)
```
where the shape ``String`` follows the [WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry) format, e.g., ``POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))``.
The `timeStamp` can store a duration as `Array(start, end)` or an instance as `Array(instance)`.
The `v` field is preserved for some extra values to be stored, and can be `None`. The `d` field is used for storing information such as ID.

### Trajectory

The on-disk trajectories are defined as follows:

```scala
    case class TrajPoint(lon: Double, 
                         lat: Double, 
                         t: Array[Long], 
                         v: Option[String])

    case class T(points: Array[TrajPoint], d: String)
```
The `t`, `v`, and `d` fields follow the convention as in Event.

### OSM Map

The on-disk road network is defined with _nodes_ and _edges_:
```scala
    case class node(osmid: String, shape: String)
    case class edge(shape: String, 
                    start_node: String,  
                    end_node:String,               
                    osmid:String, 
                    oneway:Boolean,
                    length: Double)
```
To facilitate the built-in functions, there should be a `nodes.csv` and `edges.csv` pair together in one directory.


## In-Memory Data Standard

The default types of `Event` and `Trajectory` are:
```scala
    type TrajDefault = Trajectory[None.type, Map[String, String]]

    type EventDefault = Event[Geometry, None.type, Map[String, String]]
```

`RoadGrid`

The `RoadGrid` class defines the road network for map-matching related applications.
```scala
class RoadGrid(vertexes: Array[RoadVertex], 
               edges: Array[RoadEdge],
               minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, gridSize: Double)
  extends Grid(minLon, minLat, maxLon, maxLat, gridSize) {...}

final case class RoadVertex(id: String, point: Point) {...}
final case class RoadEdge(id: String, from: String, to: String, length: Double, ls: LineString) {...}
```
A `RoadGrid` can be initialized as: 
```scala
def apply(sourceFilePath: String, gridSize: Double = 0.1): RoadGrid = {...}
```
Where the `sourceFilePath` has to contain `edges.csv` and `nodes.csv`. The `gridSize` is the granularity of longitude/latitude.

> For more flexible instance declaration and usage, please refer to [Full API](https://github.com/Panrong/st4ml/blob/instance/docs/internal.md).
# End-To-End Examples

In this page we present three real applications implementing with ST4ML. The source code can be found [here](https://github.com/Panrong/st-tool/tree/instance/examples/src/main/scala).

- [Average Speed Extraction](Average-Speed-Extraction)
- [Writing Customized Extraction](Writing-Customized-Extraction)
- [Map Matching](Map-Matching)

## Average Speed Extraction

This application takes some trajectories, a raster structure and find the average speed of trajectories falling in each raster cell. It is useful in DL applications such as traffic prediction and route recommendation.

### Input
- Spark master
- directory to the trajectory data
- directory to te raster structure
- parallelism

Example:
`local[*] datasets/porto_toy datasets/porto_raster.csv 64`

### Essential code:
Dependencies and environment set up:
```scala
import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Extent}
import st4ml.operators.converter.Traj2RasterConverter
import st4ml.operators.extractor.RasterSpeedExtractor
import st4ml.operators.selector.SelectionUtils.ReadRaster
import st4ml.operators.selector.Selector

object AverageSpeedExample {
  def main(args: Array[String]): Unit = {
    val master = args(0)
    val trajDir = args(1)
    val rasterDir = args(2)
    val parallelism = args(3).toInt

    val spark = SparkSession.builder()
      .appName("AverageSpeedExample")
      .master(master)
      .getOrCreate()
```
We first read the raster structure with the helper function `ReadRaster` and get the "MBR" or both spatial and temporal dimensions (for feeding the Selector).

```scala
    val (sArray, tArray) = ReadRaster(rasterDir)
    val sRange = Extent(sArray).toPolygon
    val tRange = Duration(tArray)
```

We then initialize the three operators. In this example, we invoke the `Traj2RasterConverter` and the built-in extractor.
```scala
    val selector = Selector(sRange, tRange, parallelism) // takes in the entire range of interest
    val converter = new Traj2RasterConverter(sArray, tArray) // the two arrays define the raster
    val extractor = new RasterSpeedExtractor // built-in extractor
```

We finally pipeline the three operators and execute them in sequence.
```scala
    val selectedRDD = selector.selectTrajCSV(trajDir) // based on the input data format 
    println(s"--- Selected ${selectedRDD.count} trajectories")
    val convertedRDD = converter.convert(selectedRDD) // by default, the R-tree-based conversion applies
    println(s"--- Converted to ${converter.numCells} raster cells")
    val extractedRDD = extractor.extract(convertedRDD, metric = "greatCircle", convertKmh = true) // the default extractor takes input arguments
    println("=== Top 2 raster cells with the highest speed:")
    extractedRDD.sortBy(_._2, ascending = false).take(2).foreach(println) // show examples
    sc.stop()
```


## Writing Customized Extraction
In this example, we write an application that *extracts the number of trajectory TRIP_IDs inside each spatial map cell*. We demonstrate the implementation
using cusotmized converters and extractors.
### Input (same as the above example)
- Spark master
- directory to the trajectory data
- directory to te raster structure
- parallelism

Example:
`local[*] datasets/porto_toy datasets/porto_raster.csv 64`

### Essential code:
Dependencies and environment set up:
```scala
object CustomizationExample {
  def main(args: Array[String]): Unit = {
    // example inputs: local[*] datasets/porto_toy datasets/porto_raster.csv 64
    val master = args(0)
    val trajDir = args(1)
    val rasterDir = args(2)
    val parallelism = args(3).toInt

    val spark = SparkSession.builder()
      .appName("CustomizationExample")
      .master(master)
      .getOrCreate()
```
Read the spatial map structure (we reuse the raster file for this example):
```scala
    val sm = ReadRaster(rasterDir)._1.distinct // read the spatial grids from the raster
```

Initialize the selector and converter:
```scala
    val selector = new Selector(tQuery = Duration(1408039037, 1408125437), parallelism = parallelism) // select trajectories according to the timestamps only
    val converter = new Traj2SpatialMapConverter(sm)
```

Perform selection:
```scala
    /* selection */
    val selectedRDD = selector.selectTrajCSV(trajDir)
    println(s"--- Selected ${selectedRDD.count} trajectories")
```

#### Implementation with customized converter:

```scala
    /* customized conversion */

    // The preMap function preserves the 'TRIP_ID' and discards other attributes in the data field
    val preMap: TrajDefault => Trajectory[None.type, String] = traj => traj.mapData(x => x("TRIP_ID"))

    // The agg function group the mapped data inside each grid as a Map(TRIP_ID -> count)
    val agg: Array[Trajectory[None.type, String]] => Map[String, Int] = _.map(_.data).groupBy(identity).map(t => (t._1, t._2.length))

    // Customized conversion 
    val convertedRDD = converter.convert(selectedRDD, preMap, agg) 

    // combine the distributed results 
    import st4ml.instances.Utils._
    
    def combineMap(a: Map[String, Int], b: Map[String, Int]): Map[String, Int] = {
      a ++ b.map { case (k, v) => k -> (v + a.getOrElse(k, 0)) }
    }

    val resultSm = convertedRDD.collectAndMerge(Map[String, Int](),combineMap)
    println(s"Number of TRIP_IDs inside each cell: ${resultSm.entries.map(_.value).deep}") // utilizing the collectiveRDD functions
```
#### Implementation with customized extractor:

```scala
    val selectedRDD2 = selector.selectTrajCSV(trajDir)
    println(s"--- Selected ${selectedRDD2.count} trajectories")
    val convertedRDD2 = converter.convert(selectedRDD2)

    // realize the customized extractor
    class CountExtractor extends Extractor {
      def agg(trajArr: Array[Trajectory[None.type, Map[String, String]]]): Map[String, Int] = {
        trajArr.map(_.mapData(x => x("TRIP_ID"))).map(_.data).groupBy(identity).map(t => (t._1, t._2.length))
      }

      def extract(rdd: RDD[SpatialMap[Polygon, Array[Trajectory[None.type, Map[String, String]]], None.type]]): SpatialMap[Polygon, Map[String, Int], None.type] = {
        rdd.map(sm => sm.mapValue(agg))
          .collectAndMerge(Map[String, Int](), combineMap)
      }
    }

    // invoke the customized extractor
    val extractor = new CountExtractor
    val extractedResult = extractor.extract(convertedRDD2)

    println(s"Number of trajectories inside each cell: ${extractedResult}") // utilizing the customized extractor
```

## Map Matching

[_Map Matching with Hidden Markov Model_](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/12/map-matching-ACM-GIS-camera-ready.pdf) is developed as a built-in application. In this application, only selector and converter are involved as follows. 
The main algorithm is implemented in `convertWithInterpolation` in a MapReduce manner. For each trajectory, after conversion a new trajectory with sojourn points on the road network is generated. For road segments that no nearby GPS points are captured,
interpolation is induced. 

For the input format of the road network, please refer to [here](https://github.com/Panrong/st4ml/blob/instance/docs/data_standard.md). 
```scala
    val selector = new Selector(sRange, parallelism = parallelism)
    val converter = new Traj2TrajConverter(map)

    val selectedRDD = selector.selectTrajCSV(trajDir)
    println(s"--- Selected ${selectedRDD.count} trajectories")
    val convertedRDD = converter.convertWithInterpolation(selectedRDD) // this will interpolate the missing roads based on shortest path
    println(s"--- Two example map-matched trajectories:")
    convertedRDD.filter(_.data != "invalid").take(2).foreach(println)
```
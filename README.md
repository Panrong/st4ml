# ST4ML: Machine Learning Oriented Spatio-Temporal Data Processing at Scale


[Overview](#overview)

[Quick Start](#quick-start)

[Core Techniques](#core-techniques)

[Next Step](#next-step)

[References](#references)

## Overview

![Overview of ST4ML](docs/overview.png)

ST4ML is a distributed spatio-temporal(ST) data processing
system built on top of [Apache Spark][spark]. It facilitates data engineers and researchers to handle 
big ST data in a distributed manner and conveniently utilize the large amount of available ST data in supporting various ML applications.

Specifically, ST4ML provides:
- _Selection-Conversion-Extraction_, a three-stage pipeline that eases the programming effort of making big ST data ML-ready
- Efficient data ingestion and parsing of common formats (e.g., CSV + WKT) and public datasets (e.g. OSM and Porto taxi)
- Fundamental operation over spatial and ST objects (e.g., event, trajectory and raster)
- Abundant feature extraction functions (e.g., map matching, trajectory speed extraction, and anomaly events extraction)

## Quick Start
### Local environment setup
To test the first ST4ML application without a Spark cluster established, please follow the instruction to set 
up the single-machine environment:

Download the latest Spark from https://spark.apache.org/downloads.html and unzip it
```
tar zxvf spark-3.3.0-bin-hadoop3.tgz 
```

Clone this repo to local:
```
git clone https://github.com/Panrong/st-tool.git
```

### Application example
Run the following command as your first ST4ML application:
```bash
cd PATH_TO_ST4ML
bash PATH_TO_SPARK/bin/spark-submit\
 --class examples.AnomalyExtractionTest\
 target/scala-2.12/st-tool-2.12-3.0.jar
```

The following result shall appear (after some Spark logs) if the application is executed correctly:
```
TODO: change to top dataset
============================================
== ST4ML example: finding abnormal events ==
============================================
Processing 23 events
Extracted 0 abnormal events occurring during 23 to 4.
2 examples: 
```

### Programming with ST4ML
#### Example background
Dividing an urban area into grids, researchers take historical traffic speeds of each grid cell to train an ML model and predict the future speeds. 
The model input is usually formulated as a sequence of 2-d matrices, where a matrix records the traffic speeds of the grids at a given time, 
and each element of the matrix is the average speed of a grid cell. 
Since the actual traffic speed (across the grids and time slots) is often not directly 
available, researchers need to derive them from other attainable data, such as the trajectories of individual vehicles.


#### Essential code
// TODO refine this code

```scala
// define inputs
val raster =  ReadRaster(rasterFile)
val dataDir = "./datasets/"
val sQuery = Extent().toPolygon
val tQuery = Duration()
// initialize operators
val selector = Selector[STTraj](sQuery, tQuery, n = 100)
val converter = Traj2RasterConverter(raster)
val extractor = RasterSpeedExtractor(unit = "kmh")
// execute the application
val trajRDD = selector.select(dataDir)
val rasterRDD = converter.convert(trajRDD)
val speedRDD = extractor.extract(rasterRDD)
// save results
saveCSV(speedRDD, resDir)
```
The helper function `ReadRaster` reads the raster structure from a CSV file (each line has fields shape, t_min and t_max) into `Array[Polygon]` and `Array[Duration]` for conversion.
The three operators are initiated in lines 7-9. In line 7, `STTraj` indicates that the original data type is trajectory, while
`sQuery` and `tQuery` specify the spatial and temporal range of interest. `n` specifies the number of partitions, i.e., the parallelism of the application.
For this feature extraction task, the most suitable data representation is raster, so a `Traj2Raster` converter is initiated as in line 8.
Last, ST4ML's built-in `RasterSpeedExtractor` is invoked.
After defining the operators, their execution functions are called in sequence. 
In line 10, the path to the trajectory data directory is passed to the selector, 
and subsequently the resulting RDDs are passed to the converter and extractor 
as a pipeline (lines 11-12). The final results are saved as CSV files with the `saveCSV` helper function.

The complete example can be found at 'src/main/scala/examples/RasterSpeedExample.scala'. TODO

## Core Techniques
The figure below plots the main components of ST4ML's _three-stage pipeline_ abstraction.

![Core techniques](docs/st4ml-internal.png?raw=true "Three-stage pipeline")


In the **Selection** stage, ST4ML retrieves an in-memory subset from gigantic 
on-disk ST data according to specified ST constraints. 
ST datasets are of large scale while ML applications are often applied to a portion 
of them. Loading all data into memory leads to a waste of memory and computation. 
A <ins>persistent metadata</ins> scheme is proposed, which groups and indexes on-disk 
ST data so only partial data are loaded into memory while the ST locality is 
preserved. <ins>In-memory indexing</ins> is implemented for faster selection 
and multiple <ins>ST-partitioners</ins> are also proposed to achieve ST-aware load balance 
during distributed computations. 

In the **Conversion** stage, ST4ML describes ST data with <ins>five ST instances</ins>:
_event_, _trajectory_, _time series_, _spatial map_, and _raster_. 
These instances provide representative abstractions of ST data and are suitable 
for different feature extraction applications. 
Efficient conversions among the five ST instances are supported in ST4ML. 
The original ST data as one instance can be converted to the most appropriate 
instance according to the nature of the ML applications. 
Specific <ins>R-tree-based</ins> optimizations are designed to speed up expensive conversions and benefit 
the computation pipeline. 

In the **Extraction** stage, ST4ML takes user-specified feature extraction 
functions and executes them in parallel. To provide different levels of
<ins>flexibility</ins>, ST4ML _pre-builds common extraction functions, supports users to 
embed logics with instance-level APIs, as well as allows direct manipulation of RDDs._ 

Such a paradigm transforms the ML feature extraction problem into scalable 
distributed executions, and thus makes the best use of the underlying distributed computing platform.

We list the supported technique and operations:

|                        <span>       |     <span>                                                                                      |     
|--------------------------------|---------------------------------------------------------------------------------------------------------|
|           ST instances           |Event, Trajectory, Time Series, Spatial Map, Raster                            |
|          ST Partitioners         |                                   Hash, STR, T-STR, Quad-tree, T-balance                                  |
|            ST Indexers           |                                           (1-d, 2-d, 3-d) R-tree                                          |
|       Input ST data format       |                                              CSV+WKT, OSM map                                             |
| Build-in extraction applications | EventAnomalyExtractor, EventCompanionExtractor, EventClusterExtractor, TrajSpeedExtractor, TrajOdExtractor, TrajStayPointExtractor, TrajTurningExtractor, TrajCompanionExtractor, TsFlowExtractor, TsSpeedExtractor, TsWindowFreqExtractor, SmFlowExtractor, SmSpeedExtractor, SmTransitExtractor, RasterFlowExtractor, RasterSpeedExtractor RasterTransitExtractor                                         |

## Next Step
Please refer to the following documentation for a thorough guide on using ST4ML.

- API for Data I/O, main computation abstraction
- Guide on installation and deployment (local mode, cluster mode, docker mode)
- Efficiency report

## References

Please cite our paper if you find ST4ML useful in your research.






[spark]: https://spark.apache.org/
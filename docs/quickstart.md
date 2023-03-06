## Evironment and Installation Check
In this page, we first present a simple example of quickly running an application with ST4ML (to check installation)
and then demonstrate how to write programs with ST4ML.
### Local environment setup
To test the first ST4ML application without a Spark cluster established, please follow the instruction to set
up a single-machine environment:

Download the latest Spark from https://spark.apache.org/downloads.html and unzip it
```
tar zxvf spark-3.x.x-bin-hadoop3.tgz 
```

Clone this repo to local:
```
git clone https://github.com/Panrong/st4ml.git
```

### Application example

Compile ST4ML core and the example project:
```bash
cd PATH_TO_ST4ML/st4ml-core
sbt assembly

cd PATH_TO_ST4ML/examples
sbt assembly
```

Submit the example application to Spark:
```bash
cd PATH_TO_ST4ML
bash PATH_TO_SPARK/bin/spark-submit\
 --master local[*]\
 --class AnomalyExtractionExample\
 --jars st4ml/target/scala-2.12/st4ml-assembly-3.0.jar\
 target/scala-2.12/st4ml_examples-assembly-0.1.jar
```

The following result shall appear (after some Spark logs) if the application is executed correctly:
```
============================================
== ST4ML example: finding abnormal events ==
============================================
Processing 9933 events
Extracted 2674 abnormal events occurring during 23 to 4.
2 examples: 
Map(vendor_id -> CMT, trip_distance -> 2.80, rate_code -> 1, dropoff_longitude -> -73.979507, pickup_latitude -> 40.78125, hack_license -> 768FD7AF6008C453A3A5CAD66813E4A0, pickup_longitude -> -73.946266, g -> POINT (-73.946266 40.78125), passenger_count -> 1, trip_time_in_secs -> 672, dropoff_datetime -> 2013-07-07 13:32:37, pickup_datetime -> 2013-07-07 13:21:25, dropoff_latitude -> 40.763603, medallion -> 768FD7AF6008C453A3A5CAD66813E4A0)
Map(vendor_id -> VTS, trip_distance -> 2.74, rate_code -> 1, dropoff_longitude -> -73.97673, pickup_latitude -> 40.781464, hack_license -> 768261A6327C320FD1F61E61B7F1358B, pickup_longitude -> -73.946503, g -> POINT (-73.946503 40.781464), passenger_count -> 5, trip_time_in_secs -> 600, dropoff_datetime -> 2013-07-07 12:43:00, pickup_datetime -> 2013-07-07 12:33:00, dropoff_latitude -> 40.760757, medallion -> 768261A6327C320FD1F61E61B7F1358B)

```

Also, programmers can use ST4ML's ST functionalities through `spark-shell`:

> - Currently, ST4ML relies on Spark environment (i.e., it can only be used under `spark-submit` or `spark-shell` but
    > not normal `scala`). We may extend ST support to normal `scala` environment in the future versions.

```bash
>>
spark-shell --jars PATH_TO_ST4ML/st4ml-core/target/scala-2.xx/st4ml-assembly-x.x.jar
```

Inside Spark Shell, for example, calculate the Great Circle Distance of two points as:
```scala
import st4ml.instances.Point
import st4ml.instances.GeometryImplicits.withExtraPointOps

val p1 = Point(103.6832868, 1.3470576)
val p2 = Point(103.685823, 1.3421684)
p1.greatCircle(p2)
```
The great circle distance (in meter) between two points output as 
```bash
res1: Double = 612.4118575312602
```


ST4ML is better to run on within a Spark cluster with HDFS storage support. For programmers that are
familiar with Spark and have the cluster set up, they may simply include the `jar` package of ST4ML as execution resources
to enjoy the ST functionalities. For newcomers to Spark, please follow [Spark Installation guide](https://github.com/Panrong/st4ml/blob/instance/docs/installation.md)
to  build up a distributed computation environment.

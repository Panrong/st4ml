# ST4ML: Machine Learning Oriented Spatio-Temporal Data Processing at Scale

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

## Quick start
### Local environment setup
To test the first ST4ML application without a Spark cluster deployed, please follow the instruction to set 
up the single-machine environment:

Download the latest Spark from https://spark.apache.org/downloads.html and unzip it
```aidl
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
## Core techniques
The figure below plots the main components of ST4ML's _three-stage pipeline_ abstraction.

![Core techniques](docs/st4ml-internal.png?raw=true "Three-stage pipeline")


In the **Selection** stage, ST4ML retrieves an in-memory subset from gigantic 
on-disk ST data according to specified ST constraints. 
ST datasets are of large scale while ML applications are often applied to a portion 
of them. Loading all data into memory leads to a waste of memory and computation. 
A _persistent metadata_ scheme is proposed, which groups and indexes on-disk 
ST data so only partial data are loaded into memory while the ST locality is 
preserved. _In-memory indexing_ is implemented for faster selection 
and multiple _ST-partitioners_ are also proposed to achieve ST-aware load balance 
during distributed computations. 

In the **Conversion** stage, ST4ML describes ST data with five ST instances: 
_event_, _trajectory_, _time series_, _spatial map_, and _raster_. 
These instances provide representative abstractions of ST data and are suitable 
for different feature extraction applications. 
Efficient _index-based conversions_ among the five ST instances are supported in ST4ML. 
The original ST data as one instance can be converted to the most appropriate 
instance according to the nature of the ML applications. 
Specific optimizations are designed to speed up expensive conversions and benefit 
the computation pipeline. 

In the **Extraction** stage, ST4ML takes user-specified feature extraction 
functions and executes them in parallel. To provide different levels of 
flexibility, ST4ML _pre-builds common extraction functions, supports users to 
embed logics with instance-level APIs, as well as allows direct manipulation of RDDs._ 

Such a paradigm transforms the ML feature extraction problem into scalable 
distributed executions, and thus makes the best use of the underlying distributed computing platform.


## Next step
Please refer to the following documentation for a thorough guide on using ST4ML.

- API for Data I/O, main computation abstraction
- Guide on installation and deployment (local mode, cluster mode, docker mode)
- Efficiency report

## References







[spark]: https://spark.apache.org/
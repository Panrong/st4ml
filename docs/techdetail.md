## Technical Highlights
The figure below plots the main components of ST4ML's _three-stage pipeline_ abstraction.

![Core techniques](st4ml-internal.png?raw=true "Three-stage pipeline")


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

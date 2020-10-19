Getting Started with ST-Tool
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Map Matching
---------------
The map matching module in the ST-Tool implements the widely used `Hidden Markov Model (HMM) <https://www.microsoft.com/en-us/research/publication/hidden-markov-map-matching-noise-sparseness/>`_ algorithm and supports data parallel execution. 

To run map matching, go to ``ST-TOOLHOME/run`` and run 
``stt mapmatching mmconfig.json``

The ``mmconfig.json`` file should have the following fields (example: ``mmconfig-example.json``)::

    {
    "useHDFS": true, # use HDFS or not
    "hadoopHome": "/usr/lib/hadoop-3.2.1" # path to Hadoop home
    "master": "spark", # "local" for single machine
    "total-executor-cores": "8", # the total cores used
    "executor-cores": "2", # number of CPU cores per executor
    "executor-memory": "3500M", # the memory assigned to each executor
    "sparkmaster": "spark://Master:7077", # address of Spark master
    "jarpackage": "../target/scala-2.12/map-matching_2.12-1.0.jar", # path to the .jar
    "trajfile": "/datasets/porto_traj.csv", # path to the file consisting trajectory data
    "mapfile": "../preprocessing/porto.csv",  # path to the file consisting road map data
    "numtraj": "10000", # number of trajectories to apply map matching
    "resultsdir": "/datasets/tmpmmres", # directory to save the map matching results
    }

One way to generate this configuration file is using ``ST-TOOLHOME/run/gen_mm_config.py``. 

* Notes on file storage location:

When using ST-Tool in a distributed cluster, the ``trajfile`` is better to be stored in *HDFS* so that all the workers can access it easily.
Otherwise, the workers should have the same file with the same directory name. 

The ``mapfile`` is currently stored *locally* on each worker node with the same directory name since the graph reading function cannot access HDFS currently.

After map matching is done, a folder is created at the specific location, which consists multiple ``.csv`` files named ``part-000...``. Each of the files is 
the production of an executor. To combine the files into a single file, a helper function ``ST-TOOLHOME/run/helper/combine.py`` can be used.



Range Query
---------------
ST-Tool supports range query on the road network. The user inputs a map-matched trajectory dataset and a list of query ranges, ST-Tool will output the trajectories 
inside the query ranges. 

To run map matching, go to ``ST-TOOLHOME/run`` and run 
``stt rangequery rqconfig.json``

The ``rqconfig.json`` file should have the following fields (example: ``rqconfig-example.json``)::

    {
    "hadoopHome": "/usr/lib/hadoop-3.2.1" # path to Hadoop home
    "master": "spark", # "local" for single machine
    "total-executor-cores": "8", # the total cores used
    "executor-cores": "2", # number of CPU cores per executor
    "executor-memory": "3500M", # the memory assigned to each executor
    "sparkmaster": "spark://Master:7077", # address of Spark master
    "jarpackage": "../target/scala-2.12/map-matching_2.12-1.0.jar", # path to the .jar
    "mmtrajfile": "/datasets/mm100000.csv", # path to the file consisting map-matched trajectory data (from the ST-Tool)
    "mapfile": "../preprocessing/porto.csv",  # path to the file consisting road map data
    "query": "../datasets/queries.txt", # path to the file consisting query ranges
    "gridsize": "2", # to partition the map into grids with length gridsize (in km)
    "numpartition": "8" # number of partitions for RTree indexing. It can be set to be the same as number of CPU cores in the cluster. 
    "rtreecapacity": 1000 # RTree capacity, should not be less than square root of total number of trajectories
    "resultsdir": "/datasets/tmprqres", # temporory directory to save the range query results in HDFS

    }

One way to generate this configuration file is using ``ST-TOOLHOME/run/gen_rq_config.py``. 

* Notes on file storage location:

When using ST-Tool in a distributed cluster, the ``mmtrajfile`` is better to be stored in *HDFS* so that all the workers can access it easily.
Otherwise, the workers should have the same file with the same directory name. 

The ``mapfile`` is currently stored *locally* on **each** worker node with the same directory name since the graph and text reading function cannot access HDFS currently.

The ``queryfile`` should *locally* stored on the master node.

After map matching is done, a folder is created at the specific location, which consists multiple folders that one for each query. Each folder consists of multiple ``.csv`` files named ``part-000...``. Each of the files is 
the production of an executor. To combine the files into a single file, a helper function ``ST-TOOLHOME/run/helper/combine.py`` can be used.


OD Query
---------------

The OD query returns all the trajectories that traverse from the given origin (*O*) to the given destination (*D*).

Two options are available: 

1) OD query on given OD pairs
2) query thoroughly on all possible OD pairs

To run OD query, go to ``ST-TOOLHOME/run`` and run 
``stt odquery odconfig.json``

The ``odconfig.json`` file should have the following fields (example: ``rqconfig-example.json``)::

    {
    "hadoopHome": "/usr/lib/hadoop-3.2.1" # path to Hadoop home
    "master": "spark", # "local" for single machine
    "total-executor-cores": "8", # the total cores used
    "executor-cores": "2", # number of CPU cores per executor
    "executor-memory": "3500M", # the memory assigned to each executor
    "sparkmaster": "spark://Master:7077", # address of Spark master
    "jarpackage": "../target/scala-2.12/map-matching_2.12-1.0.jar", # path to the .jar
    "mmtrajfile": "/datasets/mm100000.csv", # path to the file consisting map-matched trajectory data (from the ST-Tool)
    "mapfile": "../preprocessing/porto.csv",  # path to the file consisting road map data
    "query": "../datasets/odqueries.txt", # path to the file consisting query ODs OR "all" for generating the thorough OD matrix 
    "numpartition": "8" # number of partitions for RTree indexing. It can be set to be the same as number of CPU cores in the cluster. 
    "resultsdir": "/datasets/tmpodres", # temporory directory to save the range query results in HDFS

    }

One way to generate this configuration file is using ``ST-TOOLHOME/run/gen_od_config.py``. 

* Notes on file storage location:

When using ST-Tool in a distributed cluster, the ``mmtrajfile`` is better to be stored in *HDFS* so that all the workers can access it easily.
Otherwise, the workers should have the same file with the same directory name. 

The ``mapfile`` is currently stored *locally* on **each** worker node with the same directory name since the graph and text reading function cannot access HDFS currently.

The ``queryfile`` should *locally* stored on the master node.

After map matching is done, a folder is created at the specific location, which consists multiple ``.csv`` files named ``part-000...``. Each of the files is 
the production of an executor. To combine the files into a single file, a helper function ``ST-TOOLHOME/run/helper/combine.py`` can be used.

Speed Query
---------------
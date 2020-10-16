Getting Started with ST-Tool
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Map Matching
---------------
The map matching module in the ST-Tool implements the widely used `Hidden Markov Model (HMM) <https://www.microsoft.com/en-us/research/publication/hidden-markov-map-matching-noise-sparseness/>`_ algorithm and supports data parallel execution. 

To run map matching, go to ``ST-TOOLHOME/run`` and run 
``./ST-Tool mapmatching mmconfig.json``

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
OD Query
---------------
Speed Query
---------------
Benchmark
^^^^^^^^^^^^^^^
Map Matching
---------------


Testing environment: 
    4 executors on 2 workers with ``4 CPU cores and 7 GB memory`` each.

Data infomation:
    10000 trajectories from Porto dataset to match.

Execution output::

    ... Setting Spark up took: 2.755052387s
    ... Generating road graph took: 2.069630188s
    ==== Read CSV Done
    --- Total number of lines: 1710670
    --- Total number of valid entries: 1674160
    ... Time used: 178.694599319s
    ==== Split trajectories with speed limit 50.0 m/s and time interval limit 180.0 s
    ==== Split Trajectories Done
    --- Now total number of entries: 1676953
    ... Time used: 203.45639891s
    ==== Remove Redundancy Done
    --- Now total number of entries: 1676953
    ... Time used: 278.193874952s
    ==== Check Map Coverage Range Done
    --- Now total number of entries: 1642652 in the map range of List(41.0998131, -8.6999794, 41.2511297, -8.4999935)
    ... Time used: 285.104529379s
    ... Generating trajRDD took: 945.457998428s
    ==== Start Map Matching
    Total time: 969.98793277

`[spark log] <http://18.141.153.85:18080/history/app-20201015171021-0014/jobs/>`_

Range Query
---------------

Testing environment: 
    4 executors on 2 workers with ``4 CPU cores and 7 GB memory`` each.

Data infomation:
    100 (random) queries on 100000 map matched trajectories.

    RTree capacity: 1000

    grid size: 2

    number of partitions: 8

Execution output::

    ... Repartition time: 20.42047792s
    ... RTree generation time: 48.624654005s
    ... RTree query time: 49.973619858s

`[spark log] <http://18.141.153.85:18080/history/app-20201018134949-0004/jobs/>`_

OD Query
---------------

Testing environment: 
    4 executors on 2 workers with ``4 CPU cores and 7 GB memory`` each.

Data infomation:
    100 (random) queries on 100000 map matched trajectories.

    number of partitions: 8

Execution output::

    ... odRDD generation time: 5.655062451s
    ... OD query time for 538008025 pairs: 861.586406464s

`[spark log] <http://18.141.153.85:18080/history/app-20201019151041-0009/jobs/>`_

Speed Query
---------------
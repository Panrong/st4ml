Installation
^^^^^^^^^^^^^^^
Dependencies
---------------
ST-Tool is tested with the following dependencies and versions:

* ``Java 1.8.0``
* ``Scala 2.12.10``
* ``Spark 3.1.0``
* ``Hadoop 3.2.1``

To install Spark, please follow the instructions from `Spark Documentation <https://spark.apache.org/downloads.html>`_.

[Optional] To enable Spark REST API, include the following line in ``spark-defaults.conf`` :

.. code-block:: json    
 
    spark.master.rest.enabled true

[Optinal] To enable history server for view job details when finished, please follow `History Server Documentation <https://spark.apache.org/docs/latest/monitoring.html#web-interfaces>`_.

To install Hadoop with HDFS, please follow `Hadoop Documentation <https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html>`_.


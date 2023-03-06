# Simple Guides on Spark and HDFS Installation

This page provides a simple guide for programmers that are new to Spark to manually install and deploy Spark and HDFS 
in a computer cluster. To better utilize ST4ML's distributed computing efficiency, a (linux) cluster with one master and at least two workers is 
desired.

> An automatic / one-line Spark deployment will be added in the future version.
## General Steps of Installing Apache Spark

Below lists the necessary steps of installing Spark. For details please search for online detailed instructions.
0. Pre-requisite: Java, Scala, Sbt  
1. [Optional] Modify the host name of all machines for convenience (e.g,. master, worker1, worker2, etc.)
2. Enable passwordless SSH connection among all machines 
3. Download and unzip the compiled Spark from https://spark.apache.org/downloads.html to the master server
4. Configure the following files in `PATH_TO_SPARK/conf/`:
   - `workers`
   - `spark-defaults.conf`
   - `spark-env.sh`
5. Copy the configured spark folder to all workers
6. Add `PATH_TO_SPARK` to system paths (e.g., by modifying `~/.bashrc`)
6. On master server, start the cluster by `bash PATH_TO_SPARK/sbin/start-all.sh`
7. Check if the cluster is up at `localhost:8080` 
8. [Optional] Enable history server for debugging and logging


## General Steps of Installing Hadoop

ST4ML employs Hadoop as its default data storage. Below lists the necessary steps of installing Hadoop. For details please search online.

1. Download and unzip the compiled Hadoop from https://hadoop.apache.org/releases.html to the master server
2. Configure the following files in `PATH_TO_HADOOP/etc/hadoop/`:
    - `workers`
    - `core-site.xml`
    - `hdfs-site.xml`
    - `hadoop-env.sh`
3. Copy the configured hadoop folder to all workers
4. Copy the configuration files (`core-site.xml` and `hdfs-site.xml`) to Spark (`PATH_TO_SPARK/conf/`) so Spark takes HDFS as its default data storage.
4. Add `PATH_TO_HADOOP` to system paths (e.g., by modifying `~/.bashrc`)
4. For the first time, run on the master server `hdfs namenode -format`
5. Start HDFS by `bash PATH_TO_HADOOP/sbin/start-dfs.sh`
6. Check if it works well by `hdfs dfsadmin -report`



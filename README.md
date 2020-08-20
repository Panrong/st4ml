# spark-map-matching
Main function on AWS (Currently only read data): 
  $su spark
  $bash startMapMatching.sh (Yarn client mode, not sure if correct, not shown in Hadoop monitor)
  $bash startMapMatching-Standalone.sh (Spark standalone mode) takes about 4.5 min
  output:
  ==== Read CSV Done
  --- Total number of lines: 1710670
  --- Total number of valid entries: 1674160
  ==== Split trajectories with speed limit 50.0 m/s and time interval limit 180.0 s
  ==== Split Trajectories Done
  --- Now total number of entries: 1687318
  ==== Remove Redundancy Done
  --- Now total number of entries: 1687318

source code location: /home/spark/MapMatching/src/main/scala

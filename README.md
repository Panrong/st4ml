# spark-map-matching

20200922 - Panrong

updates:
1. refactor old RoadGraph into RoadGrid and RoadGraph
2. create a small RoadGraph given a pair origin and destination for calculating shortest paths
3. add new abstraction package main.scala.geometry
4. [bug fix]preprocessing/graphml_to_csv: remove duplicate src and dst coordinates in linestring

notes:
1. Examples for RoadGrid and RoadGraph are in main.scala.graph.RoadGridTest
2. Some codes in main.scala.mapmatching are commented out for testing

Test getShortestPathAndLength: performance
--Running getShortestPathAndLength on selected edges took: 0.003123629 s
--Running getShortestPathAndLength on all edges took: 0.199301143 s
--selected edges result: (List(2214758555, 129557542, 6551282170),52.855000000000004)
--all edges result: (List(2214758555, 129557542, 6551282170),52.855000000000004)
Speedup 63.80435800794524 times for one pair of OD

-------------------------------------------------

init commit - Kaiqi


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

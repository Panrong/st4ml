# ST-Tool

## Update: the documentation has been linked to http://18.141.153.85:8000/

## Mapmatched result file format for visualization use:
20200926 - Kaiqi

Example file location: 
`master:/home/ubuntu/mm2000.csv` 

Header: `taxiID	tripID GPSPoints VertexID Candidates PointRoadPair`

 - taxiID and tripID: for identidfication
 - GPSPoints: recorded GPS info in free space of format: `(lon lat:flag)` where flag indicates whether a point is map matched (`1`) or removed (`0`)
 - VertexID: the vertex ID of the map matched trajectory, each two consecutive IDs form an edge on the road graph. The format is `(ID:flag)` where flag indicates whether a vertex is directly matched from GPS points (`1`) or interpolated from shortest path connection (`0`)
 - Candidates: possible road edges to be mapped to each feasible GPSPoints (with flag 1). The format is `idx:(edge1 edge2 ...)` seperated by `;`
 -  PointRoadPair: information aggregated from GPSPoints and VetexID. The format is `(lon,lat,edge)`
 
 ## Run map matching on single machine
 in IDEA:
 
 Input arguments
 `"path_to_raw_traj_file.csv" "path_to_road_graph_.csv" "directory_to_save_results" "local" num_of_total_traj`
  
 Example:
 
 `"path\to\train_short.csv" "path\to\porto.csv" "path\to\spark-map-matching\out\res" "local" 2000`
 
 The object to run: `RunMapMatching`
 
 Otherwise: build to `.jar` and run with `spark-submit`
  ## Run map matching on aws (distributed)
  Example script:
  
  `master:/home/spark/start.sh`
 
 ##
 
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

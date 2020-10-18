Data Format
^^^^^^^^^^^^^^^
Input files
---------------
Map file
>>>>>>>>
to be edited

Trajectory file
>>>>>>>>>>>>>>>
The trajectory file should be a ``.csv`` file whose format follows the open-source `Porto trajectory dataset <http://www.geolink.pt/ecmlpkdd2015-challenge/dataset.html>`_.
However, not all attributed are read/used in the ST-Tool.

Attribute Information:

Each data sample corresponds to one completed trip. It contains a total of 9 features, described as follows:

    **TRIP_ID**: (String) It contains a unique identifier for each trip;

    CALL_TYPE: (char) -- *Not used by ST-Tool, can be filled by any value*

    ORIGIN_CALL: (integer) -- *Not used by ST-Tool, can be filled by any value*

    ORIGIN_STAND: (integer): -- *Not used by ST-Tool, can be filled by any value*

    **TAXI_ID**: (integer): It contains a unique identifier for the taxi driver that performed each trip;

    **TIMESTAMP**: (integer) Unix Timestamp (in seconds). It identifies the trip's start;

    DAYTYPE: (char) -- *Not used by ST-Tool, can be filled by any value*

    MISSING_DATA: (Boolean)-- *Not used by ST-Tool, can be filled by any value*

    **POLYLINE**: (String): It contains a list of GPS coordinates (i.e. WGS84 format) mapped as a string. The beginning and the end of the string are identified with brackets (i.e. [ and ], respectively). Each pair of coordinates is also identified by the same brackets as **[LONGITUDE, LATITUDE]**. This list contains one pair of coordinates for each 15 seconds of trip. The last list item corresponds to the trip's destination while the first one represents its start.

Batch range query file
>>>>>>>>>>>>>>>
Range query can be done per batch by reading a ``.txt`` file. The format is as below::

    bl.lon bl.lat tr.lon tr.lat

where **bl** is the botton left corner of the query rectangle and **tr** is the top left corner of the query rectangle. **lon** stands for longitude and **lat** stands for latitude.

The numbers are seperated with a single *space*.

Example (of ranges in Porto city)::

    -8.682329739182336 41.16930767535641 -8.553892156181982 41.17336956864337
    -8.610269994657497 41.141684995811005 -8.560452908967754 41.21404596573107
    -8.656135520314116 41.1333164462172 -8.51591767515082 41.232968562456634
    ... ...

Output files
---------------
Map matching result file
>>>>>>>>>>>>>>>>>>>>>>>>>>

The ``.csv`` file generated from map matching has the following header:

+-------+-------+------------+----------+-----------+--------------+-----------+
|taxiID |tripID | GPSPoints  |VertexID  | Candidates|PointRoadPair |   RoadTime|
+=======+=======+============+==========+===========+==============+===========+
+-------+-------+------------+----------+-----------+--------------+-----------+

Explanations: 

     **taxiID** and **tripID**: for identidfication

     **GPSPoints**: recorded GPS info in free space of format: *(lon lat:flag)* where flag indicates whether a point is map matched (1) or removed (0)

     **VertexID**: the vertex ID of the map matched trajectory, each two consecutive IDs form an edge on the road graph. The format is *(ID:flag)* where flag indicates whether a vertex is directly matched from GPS points (1) or interpolated from shortest path connection (0)

     **Candidates**: possible road edges to be mapped to each feasible GPSPoints (with flag 1). The format is *idx:(edge1 edge2 ...)* seperated by ;

     **PointRoadPair**: information aggregated from GPSPoints and VetexID. The format is *(lon,lat,edge)*

     **RoadTime**: the estimated time of the taxi passing the center of each road segment. The format is *(roadID,timeStamp)*


Range query result file
>>>>>>>>>>>>>>>>>>>>>>>>>>

The ``.csv`` file generated from range query has the following header:

+-----------+-------+----------+
|queryRange |tripID | taxiID   |
+===========+=======+==========+
+-----------+-------+----------+

Explanations: 

     **queryRange**: with the format(bl.lon,bl.lat,tr.lon,tr.lat)
     
     **taxiID** and **tripID**: for identidfication
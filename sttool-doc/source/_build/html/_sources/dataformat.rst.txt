Data Format
^^^^^^^^^^^^^^^

The following explains the format of input and output files of the ST-Tool.

Input files
---------------
Map file
>>>>>>>>
The map file is a ``.csv`` file that specifies the underlying road networks. A correct map file should contain two types of lines, where node lines denote key locations (e.g., intersections) and edge lines represent the physical connections among those locations. The format is as below::

    node, [node_id], [node_longitude], [node_latitude]
    edge, [from_node_id], [to_node_id], [is_one_way], [length], [linestring]

where is_one_way="0" means that the edge is bi-directional and linestring is the gps coordinates of the edge. Bi-directional edges should be recorded as two rows, and linestring is a String consisits of a set of coordinates, where coordinates are separated by "%" while longitude and latitude are separated by " ". Note that the first coordinate is the coordinate of from_node_id, and the last coordinate is the coordinate of to_node_id.

A possible example is as follows::

    node,415754682,-8.577353,41.1569772
    node,415754656,-8.5786276,41.1579203
    edge,415754682,415754656,0,152.19400000000002,-8.577353 41.1569772%-8.577353 41.1569772%-8.5778803 41.1572505%-8.5781347 41.157393%-8.578215699999999 41.157453%-8.5782624 41.1574953%-8.578314499999999 41.1575442%-8.578361900000001 41.1575972%-8.578402199999999 41.1576487%-8.5784229 41.1576806%-8.578504000000001 41.1577925%-8.578627600000001 41.1579203%-8.5786276 41.1579203

To obtain the road networks of a target area, we recommand to use `OSMnx <https://osmnx.readthedocs.io/en/stable/>`_.  A possible example is as follows :

.. code-block:: python

   import osmnx as ox
   ox.config(log_console=True, use_cache=True)
   ox.config(all_oneway=False)
   ox.graph_from_bbox(41.25, 41.10, -8.50, -8.70, network_type='drive', simplify=True, retain_all=True)


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
>>>>>>>>>>>>>>>>>>>>>>
Used for ``rangequery`` or ``speedquery`` with ``id`` mode.
Range query can be done per batch by reading a ``.txt`` file. The format is as below::

    bl.lon bl.lat tr.lon tr.lat

where **bl** is the botton left corner of the query rectangle and **tr** is the top left corner of the query rectangle. **lon** stands for longitude and **lat** stands for latitude.

The numbers are seperated with a single *space*.

Example (of ranges in Porto city)::

    -8.682329739182336 41.16930767535641 -8.553892156181982 41.17336956864337
    -8.610269994657497 41.141684995811005 -8.560452908967754 41.21404596573107
    -8.656135520314116 41.1333164462172 -8.51591767515082 41.232968562456634
    ... ...

Batch OD query file
>>>>>>>>>>>>>>>>>>>>>>
Used for ``odquery``.
OD query can be done per batch by reading a ``.txt`` file. The format is as below::

    Origin->Destination

The IDs are seperated with a single ``->``.

Example (of road IDs in Porto city)::

    297369744->475341668
    3446699979->25632278
    128674452->5264784641
    ... ...


Batch roadID query file
>>>>>>>>>>>>>>>>>>>>>>>>
Used for ``speedquery`` with ``id`` mode.
RoadID query can be done per batch by reading a ``.txt`` file. The format is as below::

    ID1-ID2

The IDs are seperated with a single ``-``. Make sure that ID1 and ID2 are connected for valid queries.

Example (of road IDs in Porto city)::

    1687535732-688007835
    5284452790-5284452787
    420776894-431925048
    1323015484-1323015492
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
|queryOD    |trajs  | taxiID   |
+===========+=======+==========+
+-----------+-------+----------+

Explanations: 

     **queryRange**: with the format(bl.lon,bl.lat,tr.lon,tr.lat)

     **taxiID** and **tripID**: for identidfication
 

OD query result file
>>>>>>>>>>>>>>>>>>>>>>>>>>

The ``.csv`` file generated from range query has the following header:

+-----------+-------+
|queryRange |tripID | 
+===========+=======+
+-----------+-------+

Explanations: 

     **queryRange**: with the format(bl.lon,bl.lat,tr.lon,tr.lat)
    
     **trajID**: for identidfication


Speed query result file
>>>>>>>>>>>>>>>>>>>>>>>>>>

The ``.csv`` file generated from range query has the following header:

+-----------+-------+-------------+
|queryRange |Num    | TrajID:Speed|
+===========+=======+=============+
+-----------+-------+-------------+

or

+-----------+-------+-------------+
|queryID    | Num   | TrajID:Speed|
+===========+=======+=============+
+-----------+-------+-------------+

depending on the query mode ("range" and "id" respectively).

Explanations: 

     **queryRange**: with the format(bl.lon,bl.lat,tr.lon,tr.lat)
     
     **queryRange**: with the format(Origin-Desitation)

     **Num**: total number of sum trajectories queried

     **TrajID**: for identidfication

     **Speed**: km/h


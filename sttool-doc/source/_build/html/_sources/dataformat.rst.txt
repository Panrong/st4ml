Data Format
^^^^^^^^^^^^^^^
Input files
---------------
Map file
>>>>>>>>
Trajectory file
>>>>>>>>>>>>>>>
The trajectory file should be a ``.csv`` file whose format follows the open-source `Porto trajectory dataset <http://www.geolink.pt/ecmlpkdd2015-challenge/dataset.html>`_.
However, not all attributed are read/used in the ST-Tool.

* Attribute Information:

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


Output files
---------------
Map Matching Result file
>>>>>>>>>>>>>>>>>>>>>>>>>>
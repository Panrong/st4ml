import osmnx as ox
import argparse
import pandas as pd
import os

# python process_osm.py -r='-8.5,41.1,-8.55,41.15' -o osm
if __name__  == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--range', '-r', required=True, help = "the spatial range lon_min,lat_min,lon_max,lat_max")
    parser.add_argument('--network_type', '-n', default='drive', help = "network type, default: drive")
    parser.add_argument('--simplify', '-s', default=True, help="boolean, simplify or not")
    parser.add_argument('--res_dir', '-o', required=True, help="the directory to save the resulting csvs")
    args = parser.parse_args()

    res_dir_root = ''
    if(args.res_dir[-1] == '/'): 
        res_dir_root = args.res_dir[:-1]
    else:
        res_dir_root = args.res_dir
    os.system('mkdir {}'.format(res_dir_root))

    sr = [float(i) for i in args.range.split(",")]
    assert len(sr)== 4, "the input spatial range is invalid"
    G = ox.graph_from_bbox(sr[1], sr[3], sr[0], sr[2], network_type=args.network_type, simplify=args.simplify, retain_all=True)
    print('Found ', len(G.nodes()), 'nodes', len(G.edges()), 'edges')

    res = [['shape','start_node','end_node','osmid','oneway','length']]
    for edge in G.edges(data=True):
        src_gps = (G.nodes[edge[0]]["x"], G.nodes[edge[0]]["y"])
        dst_gps = (G.nodes[edge[1]]["x"], G.nodes[edge[1]]["y"])
        if "geometry" in edge[2].keys():
            linestring =  str(edge[2]['geometry'])
        else:
            linestring = 'LINESTRING ({} {}, {} {})'.format(src_gps[0], src_gps[1],dst_gps[0], dst_gps[1])
        edge_info = [linestring, str(edge[0]), str(edge[1]), str(edge[2]['osmid']), str(edge[2]['oneway']), str(edge[2]['length'])]
        res += [edge_info]

    df = pd.DataFrame(res)
    res_dir = res_dir_root+'/edges.csv' 
    df.to_csv(res_dir, index=False, header = False)

    res = [['osmid', 'shape']]
    for node in G.nodes(data=True):
        res.append([str(node[0]), 'POINT({} {})'.format(str(node[1]['x']), str(node[1]['y']))])
    df = pd.DataFrame(res)
    res_dir = res_dir_root+'/nodes.csv'
    df.to_csv(res_dir, index=False, header = False)

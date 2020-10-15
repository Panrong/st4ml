import json

config = {}
config["hadoopHome"] = "/usr/lib/hadoop-3.2.1"
config["master"] = "spark"
config["jarpackage"] = "../target/scala-2.12/map-matching_2.12-1.0.jar"
config["trajfile"] = "/datasets/porto_traj.csv"
config["mapfile"] = "../preprocessing/porto.csv"
config["resultsdir"] = "/datasets/tmpmmres"
config["sparkmaster"] = "spark://Master:7077"
config["executor-memory"] = "3500M"
config["total-executor-cores"] = "8"
config["executor-cores"] = "2"
config["numtraj"] = "10000"
config["useHDFS"] = True
json.dump(config, open("mmconfig-example.json", "w"))


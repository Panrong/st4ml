import json

config = {}
config["hadoopHome"] = "/usr/lib/hadoop-3.2.1"
config["master"] = "spark"
config["jarpackage"] = "../target/scala-2.12/map-matching_2.12-1.0.jar"
config["mmtrajfile"] = "/datasets/speed7000.csv"
config["mapfile"] = "../preprocessing/porto.csv"
config["sparkmaster"] = "spark://Master:7077"
config["executor-memory"] = "3500M"
config["total-executor-cores"] = "8"
config["executor-cores"] = "2"
config["numpartition"] = config["total-executor-cores"]
config["query"] = "../datasets/road_id_queries.txt"
config["speedrange"] = "0,200"
config["resultsdir"] = "/datasets/tmpspeedres"
config["mode"] = "id"
json.dump(config, open("speedconfig-roadid-example.json", "w"))
